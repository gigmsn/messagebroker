package messagebroker

import (
	"fmt"

	"github.com/streadway/amqp"
)

// Broker wraps the necessary
// information to deal with rabbitmq
type Broker struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queue   amqp.Queue
}

// New creates a new broker
func New(addr, name string) (*Broker, error) {

	// connect over tpc using plain auth
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("could not connect to queue: %s", err)
	}

	// open unique connection to send messages to queue
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("could not create queue channel: %s", err)
	}

	// declare a queue to hold messages
	q, err := ch.QueueDeclare(
		name,  // queue name
		false, // durable
		false, // autodelete
		false, // exclusive
		false, // nowait
		nil,   // args amqp.Table
	)
	if err != nil {
		return nil, fmt.Errorf("could not declare queue: %s", err)
	}
	return &Broker{conn, ch, q}, nil
}

// Publish message to queue
func (b *Broker) Publish(msgCh chan []byte, doneCh chan bool) {
	for {
		select {
		case msg := <-msgCh:
			b.Channel.Publish(
				"",           // exchange string
				b.Queue.Name, // queue name
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					Body: msg, // amqp.Publishing
				},
			)
		case <-doneCh:
			close(msgCh)
			<-doneCh
		}
	}
}

// Close queue connection and channel
func (b *Broker) Close() error {
	if err := b.Conn.Close(); err != nil {
		return fmt.Errorf("could not close broker connection: %s", err)
	}
	if err := b.Channel.Close(); err != nil {
		return fmt.Errorf("could not close broker channel: %s", err)
	}
	return nil
}
