package goodmq

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestAutoRecover(t *testing.T) {
	if addr, ok := os.LookupEnv("AMQP_ADDR"); ok {
		RecoverDelay = 1 * time.Second
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

		//test dump after 5 sec
		go func() {
			select {
			case <-time.After(5 * time.Second):
				connection.conn.Close()
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		//stop test after 10 sec
		go func(ctx context.Context) {
			time.Sleep(10 * time.Second)
			cancel()
		}(ctx)

		go func(ctx context.Context) {
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
		}(ctx)

		<-ctx.Done()
	} else {
		t.Error("OS env AMQP_ADDR required")
	}

}
