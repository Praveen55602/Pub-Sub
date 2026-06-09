package broker

import (
	"crypto/rand"
	"fmt"
	"time"
)

// Message represents the data packet traveling through our broker
type Message struct {
	ID        string
	Body      []byte
	Timestamp time.Time
}

// NewMessage creates a utility helper for generating messages with a unique ID
func NewMessage(body []byte) Message {
	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return Message{
		ID:        fmt.Sprintf("%x", b),
		Body:      body,
		Timestamp: time.Time{},
	}
}
