package app

import (
	"github.com/sirupsen/logrus"
	"os"
)

type Logger struct {
	*logrus.Logger
}

var logger *Logger

func NewLogger(logLevel string) *Logger {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.DebugLevel
	}

	externalLogger := logrus.New()
	externalLogger.Out = os.Stdout
	externalLogger.Level = level
	externalLogger.SetFormatter(&logrus.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "02-01 15:04:05.000",
		PadLevelText:    true,
	})

	logger = &Logger{externalLogger}

	return logger
}

func GetLogger() *Logger {
	return logger
}
