package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"message-broker/pkg/protocol"
)

type Client struct {
	qmAddr     string
	httpClient *http.Client
}

func NewClient(qmAddr string) *Client {
	return &Client{
		qmAddr:     qmAddr,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// Получить адрес брокера для топика с Менеджера Очередей
func (c *Client) getBroker(topic string) (string, error) {
	url := c.qmAddr + "/topics/route?topic=" + topic
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("ошибка роутинга, статус: %d", resp.StatusCode)
	}

	var res map[string]string
	data, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(data, &res)

	return res["address"], nil
}

type Publisher struct {
	client *Client
}

func NewPublisher(qmAddr string) *Publisher {
	return &Publisher{client: NewClient(qmAddr)}
}

func (p *Publisher) Publish(topic string, payload string) (uint64, error) {
	// Простая попытка отправить до 3 раз с переполучением адреса брокера
	var lastErr error
	for i := 0; i < 3; i++ {
		addr, err := p.client.getBroker(topic)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		reqObj := protocol.PublishRequest{
			Topic:   topic,
			Payload: payload,
		}
		data, _ := json.Marshal(reqObj)

		resp, err := p.client.httpClient.Post(addr+"/publish", "application/json", bytes.NewBuffer(data))
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("брокер вернул статус %d", resp.StatusCode)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var pubResp protocol.PublishResponse
		d, _ := ioutil.ReadAll(resp.Body)
		json.Unmarshal(d, &pubResp)
		return pubResp.Offset, nil
	}

	return 0, fmt.Errorf("не удалось отправить после 3 попыток. Последняя ошибка: %v", lastErr)
}

type Subscriber struct {
	client       *Client
	group        string
	subscriberID string
}

func NewSubscriber(qmAddr, group, subscriberID string) *Subscriber {
	return &Subscriber{
		client:       NewClient(qmAddr),
		group:        group,
		subscriberID: subscriberID,
	}
}

func (s *Subscriber) Fetch(topic string, limit int) ([]protocol.Message, error) {
	var lastErr error
	for i := 0; i < 3; i++ {
		addr, err := s.client.getBroker(topic)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		reqObj := protocol.FetchRequest{
			Topic:        topic,
			Group:        s.group,
			SubscriberID: s.subscriberID,
			Limit:        limit,
		}
		data, _ := json.Marshal(reqObj)

		resp, err := s.client.httpClient.Post(addr+"/fetch", "application/json", bytes.NewBuffer(data))
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("брокер вернул статус %d", resp.StatusCode)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var fetchResp protocol.FetchResponse
		d, _ := ioutil.ReadAll(resp.Body)
		json.Unmarshal(d, &fetchResp)
		return fetchResp.Messages, nil
	}

	return nil, fmt.Errorf("не удалось получить после 3 попыток. Последняя ошибка: %v", lastErr)
}

func (s *Subscriber) Ack(topic string, offsets []uint64) error {
	if len(offsets) == 0 {
		return nil
	}

	var lastErr error
	for i := 0; i < 3; i++ {
		addr, err := s.client.getBroker(topic)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		reqObj := protocol.AckRequest{
			Topic:        topic,
			Group:        s.group,
			SubscriberID: s.subscriberID,
			Offsets:      offsets,
		}
		data, _ := json.Marshal(reqObj)

		resp, err := s.client.httpClient.Post(addr+"/ack", "application/json", bytes.NewBuffer(data))
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("брокер вернул статус %d при ack", resp.StatusCode)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		return nil
	}

	return fmt.Errorf("не удалось отправить ack после 3 попыток. Последняя ошибка: %v", lastErr)
}

// Subscribe запускает бесконечный цикл опроса
func (s *Subscriber) Subscribe(topic string, limit int, pollInterval time.Duration, handler func(protocol.Message) error) {
	for {
		msgs, err := s.Fetch(topic, limit)
		if err != nil {
			fmt.Println("Ошибка получения:", err, "Спим...")
			time.Sleep(pollInterval)
			continue
		}

		if len(msgs) == 0 {
			time.Sleep(pollInterval)
			continue
		}

		var ackOffsets []uint64
		for _, msg := range msgs {
			err := handler(msg)
			if err == nil {
				ackOffsets = append(ackOffsets, msg.Offset)
			} else {
				fmt.Println("Обработчик вернул ошибку для офсета:", msg.Offset)
			}
		}

		if len(ackOffsets) > 0 {
			err = s.Ack(topic, ackOffsets)
			if err != nil {
				fmt.Println("Ошибка отправки ack:", err)
			}
		}
	}
}
