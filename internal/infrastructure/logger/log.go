package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

type LogLevel int

const (
	LevelWarn  LogLevel = LogLevel(logrus.WarnLevel)
	LevelInfo  LogLevel = LogLevel(logrus.InfoLevel)
	LevelDebug LogLevel = LogLevel(logrus.DebugLevel)
)

type Logger interface {
	Infof(format string, a ...interface{})
	Debugf(format string, a ...interface{})
	Warnf(format string, a ...interface{})
	Errorf(format string, a ...interface{})
	Fatalf(format string, a ...interface{})
	WithField(key string, value interface{}) Logger
	Name(name string) Logger
}

func NewWithLevel(ctx context.Context, level LogLevel) Logger {
	logrusLogger := logrus.New()
	logrusLogger.Level = logrus.Level(level)
	return Newlogrus(ctx, logrusLogger)
}

func NewDefault(ctx context.Context) Logger {
	return Newlogrus(ctx, logrus.New())
}
