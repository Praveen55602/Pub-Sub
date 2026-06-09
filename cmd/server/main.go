package main

import (
	"fmt"
	"pubsub/m/internal/broker"
	"sync"
	"time"
)

func main() {
	wg := sync.WaitGroup{}

	ex := broker.NewExchange()

	topicX := "topic-X"
	topicY := "topic-Y"

	wg.Add(3)

	go func() {
		fmt.Println("this is service A")

		q := ex.Bind(topicX)
		wg.Done()

		for data := range q.Consume() {
			fmt.Printf("service A received data %v on event for topic %s\n", data, topicX)
		}
	}()

	go func() {
		fmt.Println("this is service B")

		q := ex.Bind(topicX)
		wg.Done()

		for data := range q.Consume() {
			fmt.Printf("service B received data %v on event for topic %s\n", data, topicX)
		}
	}()

	go func() {
		//service C is not subscribing for the event
		fmt.Println("this is service C")

		q := ex.Bind(topicY)
		wg.Done()

		for data := range q.Consume() {
			fmt.Printf("service C received data %v on event for topic %s\n", data, topicY)
		}
	}()

	wg.Wait()

	//some data that needs publishing so that all services subscribed to it receives that data.
	data1 := broker.NewMessage([]byte("hello"))
	data2 := broker.NewMessage([]byte("world!"))

	ex.Publish(data1, topicX)
	ex.Publish(data2, topicY)

	time.Sleep(2 * time.Second)
}
