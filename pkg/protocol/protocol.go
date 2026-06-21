package protocol

// message representation
type Message struct {
	Offset  uint64 `json:"offset"`
	Payload string `json:"payload"`
}

type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

type PublishResponse struct {
	Offset uint64 `json:"offset"`
}

type FetchRequest struct {
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	SubscriberID string `json:"subscriber_id"`
	Limit        int    `json:"limit"`
}

type FetchResponse struct {
	Messages []Message `json:"messages"`
}

type AckRequest struct {
	Topic        string   `json:"topic"`
	Group        string   `json:"group"`
	SubscriberID string   `json:"subscriber_id"`
	Offsets      []uint64 `json:"offsets"`
}

type BrokerRegisterRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type TopicRegisterRequest struct {
	Topic    string `json:"topic"`
	BrokerID string `json:"broker_id"`
}

type QMLeaseRequest struct {
	Topic           string `json:"topic"`
	Group           string `json:"group"`
	SubscriberID    string `json:"subscriber_id"`
	Limit           int    `json:"limit"`
	BrokerMaxOffset uint64 `json:"broker_max_offset"`
}

type QMLeaseResponse struct {
	Offsets []uint64 `json:"offsets"`
}

type QMAckRequest struct {
	Topic        string   `json:"topic"`
	Group        string   `json:"group"`
	SubscriberID string   `json:"subscriber_id"`
	Offsets      []uint64 `json:"offsets"`
}
