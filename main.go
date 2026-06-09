package main

import (
	"fmt"
	"sync"
	"time"
)

type broker struct {
	mu        sync.Mutex
	listeners map[string][]chan int
}

func NewBroker() *broker {
	return &broker{
		listeners: make(map[string][]chan int),
	}
}

func (b *broker) subscribe(topic string) chan int {
	b.mu.Lock()
	defer b.mu.Unlock()

	//we'll make channel buffered so it acts as a queue.
	newListener := make(chan int, 10)
	b.listeners[topic] = append(b.listeners[topic], newListener)

	return newListener
}

func (b *broker) publish(topic string, data int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subscribers, ok := b.listeners[topic]
	if !ok {
		return fmt.Errorf("no listeners for the topic %s", topic)
	}

	//without queue we face 2 issues
	//1. if a receiver is not ready then publis becomes blocking operation
	//2. and it can't handle multiple events for the same subscriber on the same topic at the same time.

	// we'll solve 2 issue here by making the channel as buffered-
	//1. we'll make channel as buffered so it behave as a queue and absorb some events if the receiver is not ready yet
	//in production also we'll have some sort of queue to absorb the events and once the receiver is ready it will consume those events one by one in LIFO style, in production since we have the broker as seperate service therefore all the events sharing is not done via queue but using long live tcp streams between receiver and the broker.
	//2. it will also make this publish a non-blocking operation.
	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
			fmt.Println("dropping data as not able to put data into the queue")
		}
	}

	return nil
}

func main() {
	wg := sync.WaitGroup{}
	b := NewBroker()

	topicX := "topic-X"
	topicY := "topic-Y"

	wg.Add(3)

	go func() {
		fmt.Println("this is service A")

		q := b.subscribe(topicX)
		wg.Done()

		for data := range q {
			fmt.Printf("service A received data %d on event for topic %s\n", data, topicX)
		}
	}()

	go func() {
		fmt.Println("this is service B")

		q := b.subscribe(topicX)
		wg.Done()

		for data := range q {
			fmt.Printf("service B received data %d on event for topic %s\n", data, topicX)
		}
	}()

	go func() {
		//service C is not subscribing for the event
		fmt.Println("this is service C")

		q := b.subscribe(topicY)
		wg.Done()

		for data := range q {
			fmt.Printf("service C received data %d on event for topic %s\n", data, topicY)
		}
	}()

	wg.Wait()

	//some data that needs publishing so that all services subscribed to it receives that data.
	data1 := 5
	data2 := 6

	b.publish(topicX, data1)
	b.publish(topicY, data2)

	time.Sleep(2 * time.Second)
}
