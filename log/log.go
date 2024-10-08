package log

import (
	"sync/atomic"

	"go.uber.org/zap"
)

var logger atomic.Pointer[zap.SugaredLogger]

func SetLogger(l *zap.SugaredLogger) {
	logger.Store(l)
}

func init() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	SetLogger(l.Sugar())
}

func Logger() *zap.SugaredLogger {
	return logger.Load()
}
