# Go Event Broker

A progressively evolving event broker/pub-sub system built in Go to explore:

* Concurrency
* Pub/Sub architecture
* Message delivery
* Backpressure
* Fan-out systems
* Distributed systems concepts
* Broker scalability
* Fault tolerance

This project starts from the most naive implementation possible and improves incrementally commit-by-commit, demonstrating how real-world messaging systems evolve over time.

The goal is educational:
understanding *why* systems like Kafka, RabbitMQ, and NATS are designed the way they are.

---

# Current Features

* Publish/Subscribe model
* Multiple subscribers
* Fan-out event delivery
* Concurrent consumers
* Basic broker implementation using Go channels

---

# Project Evolution

## V0 — Basic Broker

* Simple pub/sub
* Fan-out broadcasting
* Uses Go channels

Problems:

* Race conditions
* Blocking subscribers
* No graceful shutdown
* No topic support

---

## Planned Evolution

### V1 — Thread Safe Broker

* Mutex protection
* Safe concurrent subscriptions

### V2 — Buffered Subscribers

* Handle slow consumers
* Introduce backpressure concepts

### V3 — Non-Blocking Publish

* Prevent publisher stalls
* Event dropping strategies

### V4 — Topics

* Topic-based subscriptions
* Topic routing

### V5 — Structured Events

* Event metadata
* Generic payload support

### V6 — Graceful Shutdown

* Cleanup logic
* Subscriber lifecycle management

### V7 — Persistence

* Durable event storage
* Recovery mechanisms

### V8 — Distributed Broker

* TCP communication
* Remote publishers/subscribers

---

# Architecture

Current architecture:

Publisher → Broker → Subscribers

Every subscriber receives a copy of every event.

---

# Running the Example

```bash
git clone https://github.com/<your-username>/go-event-broker.git

cd go-event-broker

go run examples/basic/main.go
```

---

# Example Output

```text
This is Service A
This is Service B
This is Service C

Service A received: 5
Service B received: 5
```

---

# Learning Goals

This project is intentionally designed to evolve gradually while exposing real-world engineering problems such as:

* Synchronization
* Backpressure
* Queue management
* Event durability
* Delivery guarantees
* Scalability
* Distributed coordination

---

# Tech Stack

* Go
* Goroutines
* Channels
* Mutexes
* Standard Library

---

# Future Ideas

* Retry queues
* Dead letter queues
* Event persistence
* Message acknowledgements
* Consumer groups
* Partitioning
* Distributed consensus

---

# Inspiration

Inspired by concepts used in:

* Apache Kafka
* RabbitMQ
* NATS
* Redis Pub/Sub

---

# License

MIT
