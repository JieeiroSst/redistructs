package redistructs

import (
	"log"
)

type Logger interface {
	Errorf(format string, args ...interface{})
}

type DefaultLogger struct {
}

func (l DefaultLogger) Errorf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
