package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"message-broker/pkg/protocol"
	"message-broker/pkg/sdk"
)

func main() {
	mode := flag.String("mode", "publish", "Режим")
	topic := flag.String("topic", "test-topic", "Топик")
	group := flag.String("group", "group-1", "Группа")
	id := flag.String("id", "sub-1", "Айди подписчика")
	count := flag.Int("count", 1, "Количество")
	payload := flag.String("payload", "hello", "Содержимое")
	delay := flag.Int("delay", 0, "Задержка")
	rate := flag.Int("rate", 0, "Скорость")
	qmAddr := flag.String("qm", "http://localhost:8080", "Адрес МО")
	limit := flag.Int("limit", 10, "Лимит")
	flag.Parse()

	if *mode == "publish" {
		pub := sdk.NewPublisher(*qmAddr)
		fmt.Println("Отправляем", *count, "сообщений...")
		for i := 1; i <= *count; i++ {
			msg := fmt.Sprintf("%s-%d", *payload, i)
			offset, err := pub.Publish(*topic, msg)
			if err != nil {
				fmt.Println("Какая-то ошибка при отправке:", err)
				return
			}
			if *count <= 10 || i%100 == 0 || i == *count {
				fmt.Println("Отправили сообщение:", msg, "офсет:", offset)
			}
			if *rate > 0 {
				time.Sleep(time.Duration(*rate) * time.Millisecond)
			}
		}
	} else if *mode == "subscribe" {
		sub := sdk.NewSubscriber(*qmAddr, *group, *id)
		fmt.Println("Подписчик", *id, "запустился для чтения", *count, "сообщений...")

		consumed := 0
		var lock sync.Mutex
		done := make(chan bool)

		go sub.Subscribe(*topic, *limit, 200*time.Millisecond, func(msg protocol.Message) error {
			lock.Lock()
			defer lock.Unlock()

			consumed++
			fmt.Printf("[%s] Received: offset=%d, payload='%s' (Total consumed: %d/%d)\n", *id, msg.Offset, msg.Payload, consumed, *count)

			if *delay > 0 {
				time.Sleep(time.Duration(*delay) * time.Millisecond)
			}

			if consumed >= *count {
				select {
				case done <- true:
				default:
				}
			}
			return nil
		})

		select {
		case <-done:
			fmt.Println("Прочитали все запланированные сообщения")
			time.Sleep(200 * time.Millisecond) // Спим чтобы фоновый горутин успел послать ACK
		case <-time.After(60 * time.Second):
			fmt.Println("Время вышло, выходим из подписчика")
		}
	} else if *mode == "status" {
		resp, err := http.Get(*qmAddr + "/status")
		if err != nil {
			fmt.Println("Ошибка запроса статуса:", err)
			return
		}
		defer resp.Body.Close()

		var status map[string]interface{}
		data, _ := ioutil.ReadAll(resp.Body)
		json.Unmarshal(data, &status)

		pretty, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(pretty))
	} else {
		fmt.Println("Неизвестный режим:", *mode)
	}
}
