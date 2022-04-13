package goodmq

import (
	"github.com/streadway/amqp"
	"os"
	"testing"
	"time"
)

func TestAutoRecover(t *testing.T) {
	if addr, ok := os.LookupEnv("AMQP_ADDR"); ok {
		RecoverDelay = 2 * time.Second
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

		//test dump after 3 sec
		go func() {
			select {
			case <-time.After(3 * time.Second):
				connection.notifyClose <- amqp.ErrClosed
				connection.conn.Close()
				return
			}
		}()

		cnt := 0
		for range time.Tick(2 * time.Second) {
			if ok {
				Info.Println("Heartbeat connect success")
				//stop testing when recovered
				cnt++
				if cnt == 2 {
					break
				}
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
