package broker

import (
	"fmt"
)

type Queue struct {
	ch chan Message // Channel to stream messages out to connected consumers
}

func NewQueue() *Queue {
	return &Queue{
		ch: make(chan Message, 100), // Buffer capacity for consumers
	}
}

// Push adds a message to the queue and alerts listening consumers
func (q *Queue) Push(msg Message) {
	// Notify active consumer channel asynchronously without blocking the push lock
	select {
	case q.ch <- msg:
		// Message popped into channel instantly
	default:
		fmt.Println("msg dropped due to buffer full ")
	}
}

func (q *Queue) Consume() <-chan Message {
	return q.ch
}
