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

	newListener := make(chan int)
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

	for _, ch := range subscribers {
		ch <- data
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
