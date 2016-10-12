package kafka

import (
	"sync"

	"github.com/op/go-logging"
)

var log *logging.Logger
var logMu = &sync.Mutex{}

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	if log != nil {
		return
	}
	log = logging.MustGetLogger("KafkaClient")
	logging.SetLevel(logging.INFO, "KafkaClient")
}
