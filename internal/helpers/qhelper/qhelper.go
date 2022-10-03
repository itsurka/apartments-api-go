package qhelper

import (
	json2 "encoding/json"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"itsurka/apartments-api/internal/dto"
	eh "itsurka/apartments-api/internal/helpers/errhelper"
)

type Queue struct {
	ConnString string
	Сonnection *amqp.Connection
	Сhannel    *amqp.Channel
}

func (q *Queue) Publish(queueName string, message dto.QueueMessage) {
	q.init()
	amqpQueue := q.getQueue(queueName)

	body, err := json2.Marshal(message)
	eh.FailOnError(err)

	publishErr := q.Сhannel.Publish(
		"",
		amqpQueue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	eh.PanicOnError(publishErr)
}

func (q *Queue) Consume(queueName string) <-chan amqp.Delivery {
	q.init()
	amqpQueue := q.getQueue(queueName)

	messages, err := q.Сhannel.Consume(
		amqpQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	eh.FailOnError(err)

	return messages
}

func (q *Queue) getQueue(queueName string) amqp.Queue {
	amqpQueue, err := q.Сhannel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	eh.FailOnError(err)

	return amqpQueue
}

func (q *Queue) init() {
	q.initConnection()
	q.initChannel()
}

func (q *Queue) initChannel() {
	if q.Сhannel != nil {
		return
	}

	ch, err := q.Сonnection.Channel()
	eh.FailOnError(err)

	q.Сhannel = ch
}

func (q *Queue) initConnection() {
	if q.Сonnection != nil {
		return
	}

	conn, err := amqp.Dial(q.ConnString)
	eh.FailOnError(err)

	q.Сonnection = conn
}
