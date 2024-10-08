package log

import (
	"sync/atomic"

	"go.uber.org/zap"
)

var logger atomic.Pointer[zap.SugaredLogger]

func init() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger.Store(l.Sugar())
}

func Logger() *zap.SugaredLogger {
	return logger.Load()
}
