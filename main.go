package main

import (
	"fmt"
	"time"
)

type broker struct {
	listeners []chan int
}

func (b *broker) addListener() chan int {
	newListener := make(chan int)
	b.listeners = append(b.listeners, newListener)
	return newListener
}

func main() {
	broker := broker{}

	go func() {
		fmt.Println("this is service A")
		q := broker.addListener()
		for data := range q {
			fmt.Println("service A received a event from broker - ", data)
		}
	}()

	go func() {
		fmt.Println("this is service B")
		q := broker.addListener()
		for data := range q {
			fmt.Println("service B received a event from broker - ", data)
		}
	}()

	go func() {
		//service C is not subscribing for the event
		fmt.Println("this is service C")
	}()

	//let the services intialize their queues on the broker
	time.Sleep(time.Second * 2)

	//some data that needs publishing so that all services subscribed to it receives that data.
	data := 5

	//similating a event send from the broker
	for _, c := range broker.listeners {
		c <- data
	}

	time.Sleep(2 * time.Second)
}
