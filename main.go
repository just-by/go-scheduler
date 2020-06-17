package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go-scheduler/app"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	config *app.Config
	log    *app.Logger
)

func init() {
	configPath := flag.String("env", ".env", "env file path")

	cfg, err := app.NewConfig(*configPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	config = cfg

	log = app.NewLogger(config.LogLevel)
}

func main() {
	termChan := make(chan bool)
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go signalListener(signalChan, termChan)

	ctx, cancelFunc := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	for i := 0; i < config.WorkersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx)
		}()
	}

	<-termChan
	log.Println("receive term signal, start shutdown..")
	cancelFunc()
	wg.Wait()
}

func signalListener(signalChan chan os.Signal, termChan chan bool) {
	for {
		sig := <-signalChan
		switch sig {
		case syscall.SIGINT,
			syscall.SIGTERM:
			termChan <- true
			return
		default:
			log.Println("received unknown signal", sig)
		}
	}
}

func worker(ctx context.Context) {
	consumer, err := app.NewRedis(config.RedisAddr, config.RedisPassword)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer consumer.Close()

	producer, err := app.NewRabbit(config.RabbitUrl)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer producer.Close()

	log.Println("worker started")

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopped")
			return
		default:
			process(consumer, producer)
		}
	}
}

func process(consumer app.Consumer, producer app.Producer) {
	task, err := consumer.First(config.DelayedQueue)
	if err != nil {
		log.Println(err)
		return
	}

	executeAt := time.Unix(task.ExecuteAt, 0)
	if executeAt.After(time.Now()) {
		if err = consumer.Reschedule(config.DelayedQueue, task); err != nil {
			log.Println(err)
			return
		}
		time.Sleep(5 * time.Second)
		return
	}

	job, err := transform(task)
	if err != nil {
		log.Println(err)
		return
	}

	if err = producer.Publish(job); err != nil {
		if err = consumer.Reschedule(config.DelayedQueue, task); err != nil {
			log.Println(err)
			return
		}
		log.Println(err)
		return
	}
}

func transform(task *app.DelayedTask) (*app.Job, error) {
	var job app.Job
	err := json.Unmarshal([]byte(task.Body), &job)

	return &job, err
}
