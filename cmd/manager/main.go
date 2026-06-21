package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"message-broker/pkg/protocol"
)

// Глобальные переменные - любимый паттерн джуна
var (
	GlobalLock       sync.Mutex
	BrokersMap       = make(map[string]string)    // id -> address
	BrokerLastSeen   = make(map[string]time.Time) // id -> last heartbeat
	TopicToBroker    = make(map[string]string)    // topic -> brokerID
	OffsetsRegistry  = make(map[string]map[string]uint64) // topic -> group -> committedOffset
	ActiveLeases     = make(map[string]string)    // "topic:group:offset" -> "subID:expiryUnix"
	SubscribersSeen  = make(map[string]map[string]time.Time) // topic -> subID -> lastTime
	StateFilePath    string
)

// Простейшая структура для сохранения на диск
type SavedState struct {
	Offsets map[string]map[string]uint64 `json:"offsets"`
}

func loadState() {
	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	data, err := ioutil.ReadFile(StateFilePath)
	if err != nil {
		fmt.Println("Файла с офсетами нет, ну и ладно, создадим новый при сохранении")
		return
	}

	var state SavedState
	err = json.Unmarshal(data, &state)
	if err != nil {
		fmt.Println("Ошибка парсинга файла офсетов:", err)
		return
	}

	if state.Offsets != nil {
		OffsetsRegistry = state.Offsets
	}
	fmt.Println("Успешно загрузили офсеты из файла")
}

func saveState() {
	// Джун не знает про атомарную перезапись, пишет прямо в файл
	dir := filepath.Dir(StateFilePath)
	os.MkdirAll(dir, 0777)

	state := SavedState{
		Offsets: OffsetsRegistry,
	}

	data, err := json.Marshal(state)
	if err != nil {
		fmt.Println("Ошибка маршалинга:", err)
		return
	}

	err = ioutil.WriteFile(StateFilePath, data, 0666)
	if err != nil {
		fmt.Println("Не могу записать файл офсетов:", err)
	}
}

func registerBrokerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}

	var req protocol.BrokerRegisterRequest
	data, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(data, &req)

	GlobalLock.Lock()
	BrokersMap[req.ID] = req.Address
	BrokerLastSeen[req.ID] = time.Now()
	GlobalLock.Unlock()

	fmt.Println("Зарегистрировали брокер:", req.ID, "по адресу:", req.Address)
	w.WriteHeader(200)
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req protocol.BrokerRegisterRequest
	data, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(data, &req)

	GlobalLock.Lock()
	BrokerLastSeen[req.ID] = time.Now()
	if _, ok := BrokersMap[req.ID]; !ok {
		BrokersMap[req.ID] = req.Address
	}
	GlobalLock.Unlock()

	w.WriteHeader(200)
}

func registerTopicHandler(w http.ResponseWriter, r *http.Request) {
	var req protocol.TopicRegisterRequest
	data, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(data, &req)

	GlobalLock.Lock()
	TopicToBroker[req.Topic] = req.BrokerID
	GlobalLock.Unlock()

	fmt.Println("Привязали топик:", req.Topic, "к брокеру:", req.BrokerID)
	w.WriteHeader(200)
}

func routeTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		w.WriteHeader(400)
		return
	}

	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	brokerID, ok := TopicToBroker[topic]
	var addr string
	if ok {
		lastSeen := BrokerLastSeen[brokerID]
		if time.Since(lastSeen) < 10*time.Second {
			addr = BrokersMap[brokerID]
		}
	}

	// Автоматический выбор брокера если не привязан или упал
	if addr == "" {
		var active []string
		for id, seen := range BrokerLastSeen {
			if time.Since(seen) < 10*time.Second {
				active = append(active, id)
			}
		}

		if len(active) == 0 {
			w.WriteHeader(503)
			w.Write([]byte("Нет активных брокеров в системе вообще!"))
			return
		}

		// Выбираем первый попавшийся брокер
		chosen := active[0]
		TopicToBroker[topic] = chosen
		addr = BrokersMap[chosen]
		fmt.Println("Авто-привязали топик", topic, "к брокеру", chosen)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"address":"` + addr + `"}`))
}

func leaseHandler(w http.ResponseWriter, r *http.Request) {
	var req protocol.QMLeaseRequest
	data, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(data, &req)

	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	// Инициализация карт
	if OffsetsRegistry[req.Topic] == nil {
		OffsetsRegistry[req.Topic] = make(map[string]uint64)
	}

	currentOffset, ok := OffsetsRegistry[req.Topic][req.Group]
	if !ok {
		OffsetsRegistry[req.Topic][req.Group] = 1
		currentOffset = 1
	}

	if SubscribersSeen[req.Topic] == nil {
		SubscribersSeen[req.Topic] = make(map[string]time.Time)
	}
	SubscribersSeen[req.Topic][req.SubscriberID] = time.Now()

	var leasedOffsets []uint64
	now := time.Now().Unix()

	// Ищем офсеты для аренды
	for offset := currentOffset; offset <= req.BrokerMaxOffset; offset++ {
		if len(leasedOffsets) >= req.Limit {
			break
		}

		leaseKey := fmt.Sprintf("%s:%s:%d", req.Topic, req.Group, offset)
		val, leased := ActiveLeases[leaseKey]

		if leased {
			// Проверяем протухание аренды
			parts := strings.Split(val, ":")
			if len(parts) == 2 {
				exp, _ := strconv.ParseInt(parts[1], 10, 64)
				if now > exp {
					// Аренда протухла, отдаем заново
					ActiveLeases[leaseKey] = fmt.Sprintf("%s:%d", req.SubscriberID, now+10)
					leasedOffsets = append(leasedOffsets, offset)
				}
			}
		} else {
			// Чистая аренда
			ActiveLeases[leaseKey] = fmt.Sprintf("%s:%d", req.SubscriberID, now+10)
			leasedOffsets = append(leasedOffsets, offset)
		}
	}

	resp := protocol.QMLeaseResponse{Offsets: leasedOffsets}
	respData, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(respData)
}

func ackHandler(w http.ResponseWriter, r *http.Request) {
	var req protocol.QMAckRequest
	data, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(data, &req)

	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	if OffsetsRegistry[req.Topic] == nil {
		w.WriteHeader(404)
		return
	}

	currentOffset := OffsetsRegistry[req.Topic][req.Group]

	// Джун делает самый простой ACK:
	// Снимает аренду с пришедших офсетов и двигает committed offset вперед,
	// если пришедший офсет равен текущему.
	// Если пришел офсет больше, мы просто снимаем аренду, но committed offset
	// сдвинется только когда придет нужный.
	// Чтобы пройти тесты надежности (load balancing и restart с правильного места):
	// Мы можем просто пометить этот офсет как обработанный в памяти и сдвинуть committed offset вперед.
	// Для простоты храним "подтвержденные" офсеты прямо в ActiveLeases со статусом "done"
	for _, offset := range req.Offsets {
		leaseKey := fmt.Sprintf("%s:%s:%d", req.Topic, req.Group, offset)
		ActiveLeases[leaseKey] = "done:0"
	}

	// Сдвигаем committed offset вперед мимо всех подтвержденных
	for {
		checkKey := fmt.Sprintf("%s:%s:%d", req.Topic, req.Group, currentOffset)
		if ActiveLeases[checkKey] == "done:0" {
			delete(ActiveLeases, checkKey)
			currentOffset++
		} else {
			break
		}
	}

	OffsetsRegistry[req.Topic][req.Group] = currentOffset
	saveState() // Запись на диск при каждом коммите

	w.WriteHeader(200)
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	now := time.Now()

	// Ручная сборка JSON-статуса
	activeBrokers := make(map[string]string)
	for id, addr := range BrokersMap {
		if now.Sub(BrokerLastSeen[id]) < 15*time.Second {
			activeBrokers[id] = addr
		}
	}

	type GroupStatus struct {
		CommittedOffset   uint64   `json:"committed_offset"`
		PendingAcksCount  int      `json:"pending_acks_count"`
		ActiveSubscribers []string `json:"active_subscribers"`
	}

	type TopicStatus struct {
		BrokerID     string                 `json:"broker_id"`
		ActiveGroups map[string]GroupStatus `json:"active_groups"`
	}

	status := struct {
		ActiveBrokers map[string]string      `json:"active_brokers"`
		Topics        map[string]TopicStatus `json:"topics"`
	}{
		ActiveBrokers: activeBrokers,
		Topics:        make(map[string]TopicStatus),
	}

	for topic, brokerID := range TopicToBroker {
		groups := make(map[string]GroupStatus)
		
		if OffsetsRegistry[topic] != nil {
			for group, offset := range OffsetsRegistry[topic] {
				// Считаем активные аренды
				pending := 0
				for k, v := range ActiveLeases {
					if strings.HasPrefix(k, topic+":"+group+":") && !strings.HasPrefix(v, "done:") {
						parts := strings.Split(v, ":")
						if len(parts) == 2 {
							exp, _ := strconv.ParseInt(parts[1], 10, 64)
							if now.Unix() <= exp {
								pending++
							}
						}
					}
				}

				// Активные подписчики
				var activeSubs []string
				if SubscribersSeen[topic] != nil {
					for subID, lastSeen := range SubscribersSeen[topic] {
						if now.Sub(lastSeen) < 30*time.Second {
							activeSubs = append(activeSubs, subID)
						}
					}
				}

				groups[group] = GroupStatus{
					CommittedOffset:   offset,
					PendingAcksCount:  pending,
					ActiveSubscribers: activeSubs,
				}
			}
		}

		status.Topics[topic] = TopicStatus{
			BrokerID:     brokerID,
			ActiveGroups: groups,
		}
	}

	res, _ := json.Marshal(status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(res)
}

func main() {
	port := flag.Int("port", 8080, "Port")
	state := flag.String("state", "data/manager/state.json", "state file")
	flag.Parse()

	StateFilePath = *state

	loadState()

	// Фоновый говно-цикл для очистки протухших брокеров
	go func() {
		for {
			time.Sleep(5 * time.Second)
			GlobalLock.Lock()
			now := time.Now()
			for id, last := range BrokerLastSeen {
				if now.Sub(last) > 20*time.Second {
					delete(BrokersMap, id)
					delete(BrokerLastSeen, id)
					fmt.Println("Брокер", id, "давно не присылал пинг, удаляем его")
				}
			}
			GlobalLock.Unlock()
		}
	}()

	http.HandleFunc("/brokers/register", registerBrokerHandler)
	http.HandleFunc("/brokers/heartbeat", heartbeatHandler)
	http.HandleFunc("/topics/register", registerTopicHandler)
	http.HandleFunc("/topics/route", routeTopicHandler)
	http.HandleFunc("/qm/lease", leaseHandler)
	http.HandleFunc("/qm/ack", ackHandler)
	http.HandleFunc("/status", statusHandler)

	fmt.Println("Стартуем Менеджер Очередей на порту", *port)
	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
}
