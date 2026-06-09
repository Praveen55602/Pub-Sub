package main

import (
	"fmt"
	"sync"
	"time"
)

type broker struct {
	mu        sync.Mutex
	listeners []chan int
}

func (b *broker) addListener() chan int {
	b.mu.Lock()
	defer b.mu.Unlock()

	newListener := make(chan int)
	b.listeners = append(b.listeners, newListener)

	return newListener
}

func (b *broker) publish(data int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.listeners {
		ch <- data
	}
}

func main() {
	wg := sync.WaitGroup{}
	b := broker{}

	wg.Add(2)

	go func() {
		fmt.Println("this is service A")

		q := b.addListener()
		wg.Done()

		for data := range q {
			fmt.Println("service A received a event from broker - ", data)
		}
	}()

	go func() {
		fmt.Println("this is service B")

		q := b.addListener()
		wg.Done()

		for data := range q {
			fmt.Println("service B received a event from broker - ", data)
		}
	}()

	go func() {
		//service C is not subscribing for the event
		fmt.Println("this is service C")
	}()

	wg.Wait()

	//some data that needs publishing so that all services subscribed to it receives that data.
	data := 5

	b.publish(data)

	time.Sleep(2 * time.Second)
}
