package app

import (
	"testing"
	"time"
)

type ConsumerStub struct {
	Task *DelayedTask
}

func (c *ConsumerStub) First(queue string) (*DelayedTask, error) {
	return c.Task, nil
}

func (c *ConsumerStub) Reschedule(queue string, task *DelayedTask) error {
	c.Task = task
	return nil
}

type ProducerStub struct {
	Queue map[string]*Job
}

func (p *ProducerStub) Publish(job *Job) error {
	p.Queue[job.Queue] = job
	return nil
}

type FakeClock struct {
	Time time.Time
}

func (c *FakeClock) Now() time.Time {
	return c.Time
}

func (c *FakeClock) Sleep() {
	c.Time = c.Time.Add(5 * time.Second)
}

func (c *FakeClock) Rewind(duration time.Duration) {
	c.Time = c.Time.Add(duration)
}

func TestWorker(t *testing.T) {
	t.Run("successfully requeue task", func(t *testing.T) {
		executeAt := time.Now().Add(20 * time.Second)
		task := &DelayedTask{
			ExecuteAt: executeAt,
			Body:      `{"queue": "test_queue", "payload": {"site_id": 1, "agent_id": 25}}`,
		}
		consumerStub := &ConsumerStub{Task: task}
		producerStub := &ProducerStub{Queue: make(map[string]*Job)}

		clock := &FakeClock{Time: time.Now()}
		worker := NewWorker(consumerStub, producerStub, clock)

		if err := worker.Process("delayed_queue"); err != nil {
			t.Errorf("expected no errors, got %v", err)
		}
		if _, ok := producerStub.Queue["test_queue"]; ok == true {
			t.Error("didn't expect message in the queue, have to wait 20 seconds")
		}

		clock.Rewind(30 * time.Second)

		if err := worker.Process("delayed_queue"); err != nil {
			t.Errorf("expected no errors, got %v", err)
		}
		if _, ok := producerStub.Queue["test_queue"]; ok == false {
			t.Error("expected message in the queue")
		}
	})

	//todo implement unsuccessful cases tests
}
