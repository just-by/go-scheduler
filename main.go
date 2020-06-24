package main

import (
	"context"
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
			runWorker(ctx)
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

type SimpleClock struct{}

func (c SimpleClock) Now() time.Time {
	return time.Now()
}

func (c SimpleClock) Sleep() {
	log.Debug("message rescheduled, waiting")
	time.Sleep(5 * time.Second)
}

func runWorker(ctx context.Context) {
	consumer, err := app.NewRedis(config.RedisAddr, config.RedisPassword)
	if err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}
	defer consumer.Close()

	producer, err := app.NewRabbit(config.RabbitUrl)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer producer.Close()

	worker := app.NewWorker(consumer, producer, &SimpleClock{})
	log.Println("worker started")
	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopped")
			return
		default:
			if err := worker.Process(config.DelayedQueue); err != nil {
				log.Warnf("failed to process task: %v", err)
			}
		}
	}
}
