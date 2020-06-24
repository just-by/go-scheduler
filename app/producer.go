package app

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type Rabbit struct {
	*amqp.Connection
}

func NewRabbit(rabbitUrl string) (*Rabbit, error) {
	rabbitConn, err := amqp.Dial(rabbitUrl)
	if err != nil {
		return nil, err
	}

	return &Rabbit{
		rabbitConn,
	}, nil
}

func (r *Rabbit) Close() error {
	return r.Connection.Close()
}

func (r *Rabbit) Publish(job *Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	ch, err := r.Connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.Publish(
		job.Queue,
		"",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         payload,
		})

	return err
}
