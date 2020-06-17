package app

import (
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

type Config struct {
	WorkersNum    int    `env:"WORKERS_NUMBER" envDefault:"1"`
	RedisAddr     string `env:"REDIS_ADDR"`
	RedisPassword string `env:"REDIS_PASS"`
	DelayedQueue  string `env:"REDIS_QUEUE"`
	RabbitUrl     string `env:"RABBIT_URL"`
	LogLevel      string `env:"LOG_LEVEL"`
}

var config *Config

func NewConfig(configPath string) (*Config, error) {
	cfg := &Config{}

	if err := godotenv.Load(configPath); err != nil {
		return nil, err
	}

	if err := env.Parse(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func GetConfig() *Config {
	return config
}
