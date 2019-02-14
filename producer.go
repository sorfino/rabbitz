package rabbitz

import (
	"github.com/streadway/amqp"
)

//Producer representa a un productor de mensajes
type Producer struct {
	url   string
	queue string
}

// NewProducer crea un nuevo productor para poner mensajes en una cola determinada
func NewProducer(brokerUrl string, queueName string) *Producer {
	return &Producer{brokerUrl, queueName}
}

// esta funcion es idempotente, si la queue existe no hace nada
func (p *Producer) createQueue(ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		p.queue, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	return q, err
}

// Put Pone un mensaje en una cola
//todo: esto habria que cambiarlo a []byte
func (p *Producer) Put(mensaje string) error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := p.createQueue(ch)
	if err != nil {
		return err
	}
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(mensaje),
		})

	return err
}
