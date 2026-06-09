package broker

import (
	"fmt"
	"sync"
)

type Exchange struct {
	mu       sync.RWMutex
	bindings map[string][]Queue
}

func NewExchange() *Exchange {
	return &Exchange{
		bindings: make(map[string][]Queue),
	}
}

// Bind links a queue to this exchange
func (e *Exchange) Bind(topic string) *Queue {
	e.mu.Lock()
	defer e.mu.Unlock()

	q := NewQueue()
	e.bindings[topic] = append(e.bindings[topic], *q)

	return q
}

// Publish takes a message, analyzes the bindings, and routes copies to matching queues
func (e *Exchange) Publish(msg Message, topic string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	queues, ok := e.bindings[topic]
	if !ok {
		return fmt.Errorf("no listeners for the topic %s", topic)
	}

	for _, q := range queues {
		q.Push(msg)
	}

	return nil
}
