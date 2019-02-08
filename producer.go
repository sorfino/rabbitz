package rabbitz

import (
	"github.com/streadway/amqp"
)

type Producer struct {
	url   string
	queue string
}

func NewProducer(brokerUrl string, queueName string) *Producer {
	//result := new(Producer)
	//result.queue = queueName
	//	result.url = brokerUrl
	//return result
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
