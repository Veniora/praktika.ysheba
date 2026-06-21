package main

import (
	"bytes"
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

// Глобальные настройки джуна
var (
	BrokerID    string
	Port        int
	QMAddr      string
	DataDir     string
	ExtAddr     string
	Mutex       sync.Mutex // Глобальный лок на все файловые операции брокера
)

func registerWithQM() {
	reqBody, _ := json.Marshal(protocol.BrokerRegisterRequest{
		ID:      BrokerID,
		Address: ExtAddr,
	})

	resp, err := http.Post(QMAddr+"/brokers/register", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Println("Ошибка регистрации на МО:", err)
		return
	}
	resp.Body.Close()
	fmt.Println("Успешно зарегистрировались на МО")
}

func sendHeartbeat() {
	reqBody, _ := json.Marshal(protocol.BrokerRegisterRequest{
		ID:      BrokerID,
		Address: ExtAddr,
	})
	resp, err := http.Post(QMAddr+"/brokers/heartbeat", "application/json", bytes.NewBuffer(reqBody))
	if err == nil {
		resp.Body.Close()
	}
}

func registerTopicWithQM(topic string) {
	reqBody, _ := json.Marshal(protocol.TopicRegisterRequest{
		Topic:    topic,
		BrokerID: BrokerID,
	})
	resp, err := http.Post(QMAddr+"/topics/register", "application/json", bytes.NewBuffer(reqBody))
	if err == nil {
		resp.Body.Close()
	}
}

func handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}

	var req protocol.PublishRequest
	body, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	Mutex.Lock()
	defer Mutex.Unlock()

	// Путь к файлу топика
	filePath := filepath.Join(DataDir, req.Topic+".txt")
	os.MkdirAll(DataDir, 0777)

	// Читаем весь файл, чтобы определить следующий офсет (очень медленно, зато просто!)
	content, _ := ioutil.ReadFile(filePath)
	lines := strings.Split(string(content), "\n")
	var offset uint64 = 1
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			offset++
		}
	}

	// Записываем в формате: офсет|сообщение
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("Ошибка открытия файла: " + err.Error()))
		return
	}
	defer file.Close()

	// Запись в файл без fsync (синхронизации)
	_, _ = file.WriteString(fmt.Sprintf("%d|%s\n", offset, req.Payload))

	// Зарегистрируем топик на всякий случай
	go registerTopicWithQM(req.Topic)

	respObj := protocol.PublishResponse{Offset: offset}
	respData, _ := json.Marshal(respObj)
	w.Header().Set("Content-Type", "application/json")
	w.Write(respData)
}

func handleFetch(w http.ResponseWriter, r *http.Request) {
	var req protocol.FetchRequest
	body, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	Mutex.Lock()
	defer Mutex.Unlock()

	filePath := filepath.Join(DataDir, req.Topic+".txt")

	// Определяем максимальный офсет в топике, опять читая весь файл
	content, _ := ioutil.ReadFile(filePath)
	lines := strings.Split(string(content), "\n")
	var maxOffset uint64 = 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			parts := strings.SplitN(line, "|", 2)
			if len(parts) == 2 {
				o, _ := strconv.ParseUint(parts[0], 10, 64)
				if o > maxOffset {
					maxOffset = o
				}
			}
		}
	}

	// Запрашиваем у МО аренду офсетов
	leaseReq := protocol.QMLeaseRequest{
		Topic:           req.Topic,
		Group:           req.Group,
		SubscriberID:    req.SubscriberID,
		Limit:           req.Limit,
		BrokerMaxOffset: maxOffset,
	}
	leaseData, _ := json.Marshal(leaseReq)
	resp, err := http.Post(QMAddr+"/qm/lease", "application/json", bytes.NewBuffer(leaseData))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("МО недоступен: " + err.Error()))
		return
	}
	defer resp.Body.Close()

	var leaseResp protocol.QMLeaseResponse
	d, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(d, &leaseResp)

	// Если нам ничего не выделили, отдаем пустой массив
	if len(leaseResp.Offsets) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"messages":[]}`))
		return
	}

	// Создаем мапу для быстрого поиска нужных нам офсетов
	needed := make(map[uint64]bool)
	for _, o := range leaseResp.Offsets {
		needed[o] = true
	}

	var foundMsgs []protocol.Message
	// Пробегаем по файлу еще раз и ищем сообщения с нужными офсетами
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			parts := strings.SplitN(line, "|", 2)
			if len(parts) == 2 {
				o, _ := strconv.ParseUint(parts[0], 10, 64)
				if needed[o] {
					foundMsgs = append(foundMsgs, protocol.Message{
						Offset:  o,
						Payload: parts[1],
					})
				}
			}
		}
	}

	respObj := protocol.FetchResponse{Messages: foundMsgs}
	respData, _ := json.Marshal(respObj)
	w.Header().Set("Content-Type", "application/json")
	w.Write(respData)
}

func handleAck(w http.ResponseWriter, r *http.Request) {
	var req protocol.AckRequest
	body, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	// Просто пересылаем запрос в МО
	qmAck := protocol.QMAckRequest{
		Topic:        req.Topic,
		Group:        req.Group,
		SubscriberID: req.SubscriberID,
		Offsets:      req.Offsets,
	}
	qmAckData, _ := json.Marshal(qmAck)
	resp, err := http.Post(QMAddr+"/qm/ack", "application/json", bytes.NewBuffer(qmAckData))
	if err != nil {
		w.WriteHeader(500)
		return
	}
	resp.Body.Close()

	w.WriteHeader(200)
}

func main() {
	id := flag.String("id", "broker-1", "Broker unique ID")
	port := flag.Int("port", 8081, "Port")
	qm := flag.String("qm", "http://localhost:8080", "QM address")
	data := flag.String("data", "data/broker", "data directory")
	addr := flag.String("addr", "", "External address")
	flag.Parse()

	BrokerID = *id
	Port = *port
	QMAddr = *qm
	DataDir = filepath.Join(*data, *id)
	ExtAddr = *addr
	if ExtAddr == "" {
		ExtAddr = fmt.Sprintf("http://localhost:%d", Port)
	}

	// Попытка найти существующие топики при старте
	files, err := ioutil.ReadDir(DataDir)
	if err == nil {
		for _, f := range files {
			if !f.IsDir() && filepath.Ext(f.Name()) == ".txt" {
				topic := f.Name()[:len(f.Name())-4]
				fmt.Println("Нашли старый файл топика:", topic)
				go registerTopicWithQM(topic)
			}
		}
	}

	registerWithQM()

	// Говно-цикл пинга
	go func() {
		for {
			time.Sleep(2 * time.Second)
			sendHeartbeat()
		}
	}()

	http.HandleFunc("/publish", handlePublish)
	http.HandleFunc("/fetch", handleFetch)
	http.HandleFunc("/ack", handleAck)

	fmt.Println("Стартуем Брокер", BrokerID, "на порту", Port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", Port), nil)
	if err != nil {
		fmt.Println("Ошибка запуска сервера брокера:", err)
	}
}
