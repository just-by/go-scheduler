package app

import (
	"errors"
	"github.com/go-redis/redis/v7"
	"time"
)

type DelayedTask struct {
	ExecuteAt int64
	Body      string
}

type Consumer interface {
	First(queue string) (*DelayedTask, error)
	Reschedule(queue string, task *DelayedTask) error
}

type Redis struct {
	*redis.Client
}

func NewRedis(addr string, password string) (*Redis, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	_, err := redisClient.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &Redis{
		redisClient,
	}, nil
}

func (r *Redis) Close() error {
	return r.Client.Close()
}

func (r *Redis) First(queue string) (*DelayedTask, error) {
	val, err := r.Client.BZPopMin(10*time.Second, queue).Result()
	if err != nil {
		return nil, err
	}

	var body, ok = val.Z.Member.(string)
	if !ok {
		return nil, errors.New("invalid set member")
	}

	task := DelayedTask{
		ExecuteAt: int64(val.Z.Score),
		Body:      body,
	}

	return &task, nil
}

func (r *Redis) Reschedule(queue string, task *DelayedTask) error {
	_, err := r.Client.ZAdd(
		queue,
		&redis.Z{
			Score:  float64(task.ExecuteAt),
			Member: task.Body,
		}).Result()

	return err
}
