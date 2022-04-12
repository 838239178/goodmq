package goodmq

import (
	"github.com/streadway/amqp"
	"os"
	"testing"
	"time"
)

func TestAutoRecover(t *testing.T) {
	if addr, ok := os.LookupEnv("AMQP_ADDR"); ok {
		RecoverDelay = 5 * time.Second
		connection := NewAmqpConnection(addr)
		defer connection.Close()
		consumer, err := connection.NewConsumer()
		if err != nil {
			t.Error(err)
		}
		defer consumer.Close()
		consumer.QueName = "heartbeat.queue"
		consumer.Exchange = "apiServers"
		consumeChan, ok := consumer.Consume()

		//test dump after 2 sec
		go func() {
			select {
			case <-time.After(2 * time.Second):
				connection.notifyClose <- amqp.ErrClosed
			}
		}()

		//test dump after 10 sec
		go func() {
			select {
			case <-time.After(10 * time.Second):
				connection.notifyClose <- amqp.ErrCommandInvalid
			}
		}()

		for range time.Tick(2 * time.Second) {
			if ok {
				Info.Println("Heartbeat connect success")
				for range consumeChan {
					Info.Printf("Message")
				}
				ok = false
			} else {
				Warn.Println("Heartbeat connection closed! Recovering...")
				consumeChan, ok = consumer.Consume()
			}
		}
	} else {
		t.Error("OS env AMQP_ADDR required")
	}

}
