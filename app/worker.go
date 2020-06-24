package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Producer
type Job struct {
	Queue   string      `json:"queue"`
	Payload interface{} `json:"payload"`
}

type Producer interface {
	Publish(job *Job) error
}

// Consumer
type DelayedTask struct {
	ExecuteAt time.Time
	Body      string
}

func (t DelayedTask) ToJob() (*Job, error) {
	var job Job
	err := json.Unmarshal([]byte(t.Body), &job)

	return &job, err
}

type Consumer interface {
	First(queue string) (*DelayedTask, error)
	Reschedule(queue string, task *DelayedTask) error
}

var ErrConsumer = errors.New("consumer can't read message")
var ErrProducer = errors.New("producer can't publish message")
var ErrReschedule = errors.New("message reschedule failed")
var ErrMapping = errors.New("message mapping failed")

// Worker
type Clock interface {
	Now() time.Time
	Sleep()
}

type Worker struct {
	consumer Consumer
	producer Producer
	clock    Clock
}

func NewWorker(consumer Consumer, producer Producer, clock Clock) *Worker {
	return &Worker{
		consumer: consumer,
		producer: producer,
		clock:    clock,
	}
}

func (w Worker) Process(delayedQueue string) error {
	task, err := w.consumer.First(delayedQueue)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsumer, err)
	}

	if task.ExecuteAt.After(w.clock.Now()) {
		if err := w.consumer.Reschedule(delayedQueue, task); err != nil {
			return fmt.Errorf("%w: %v", ErrReschedule, err)
		}

		w.clock.Sleep()
		return nil
	}

	job, err := task.ToJob()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMapping, err)
	}

	if err := w.producer.Publish(job); err != nil {
		if err := w.consumer.Reschedule(delayedQueue, task); err != nil {
			return fmt.Errorf("%w: %v", ErrReschedule, err)
		}
		return fmt.Errorf("%w: %v", ErrProducer, err)
	}

	return nil
}
