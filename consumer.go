package rabbitz

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

// Consumer de una cola
type Consumer struct {
	url      string
	queue    string
	ticker   *time.Ticker
	shutdown int32
}

var (
	on           int32
	shuttingDown int32 = 1
	off          int32 = 2
)

// NewConsumer crea un Consumer nuevo para la cola queueName del broker borekerUrl
func NewConsumer(brokerURL string, queueName string) *Consumer {

	return &Consumer{brokerURL, queueName, nil, off}
}

// Shutdown comienza el apagado del Consumer
func (c *Consumer) Shutdown(ctx context.Context) {

	if c.isOff() {
		return
	}

	atomic.StoreInt32(&c.shutdown, shuttingDown)
	c.ticker.Stop()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c.isOff() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Consumer) isOn() bool {
	return atomic.LoadInt32(&c.shutdown) == on
}

func (c *Consumer) isOff() bool {
	return !c.isOn()
}

// Listen comienza a escuchar mensajes. Devuelve un channel donde llegan los mensajes cuando
// aparecen en la cola.
func (c *Consumer) Listen() (chan []byte, error) {

	if c.isOn() {
		return nil, errors.New("Consumer is on")
	}

	conn, err := amqp.Dial(c.url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := c.createQueue(ch)
	if err != nil {
		return nil, err
	}

	messages := make(chan []byte)

	go func() {

		//creamos un ticker que cada 1s leemos de la cola.
		c.ticker = time.NewTicker(5 * time.Second)

		//esta funcion en la que lee de la cola
		read := func() {
			msgs, err := ch.Consume(
				q.Name, // queue
				"",     // consumer
				true,   // auto-ack
				false,  // exclusive
				false,  // no-local
				false,  // no-wait
				nil,    // args
			)
			//los mensajes los mandamos al channel messages que es donde deberia estar esperandolos
			if err == nil {
				for d := range msgs {
					messages <- d.Body
				}
			}
		}

		//aca nos quedamos por siempre hasta que cancelen el contexto y
		//y mientras en cada tick leemos mensajes
		for {
			<-c.ticker.C
			read()
			if atomic.LoadInt32(&c.shutdown) != on {
				defer close(messages)
				defer ch.Close()
				defer conn.Close()
				break
			}
		}
		atomic.StoreInt32(&c.shutdown, off) //we are done
	}()

	atomic.StoreInt32(&c.shutdown, on)
	return messages, nil
}

// esta funcion es idempotente, si la queue existe no hace nada
func (c *Consumer) createQueue(ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		c.queue, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	return q, err
}
