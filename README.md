# GoodMQ

A good `streadway/amqp` wrapper. Supporting connection reconnecting and channel recovering.

### Feature

- [ ]  More flexible configuration
- [ ]  TODO

## Consumer Example

1. Connection will try reconnect every 5 sec after close
2. When connection reconnect success, notify all channel recovering

```go
func ListenHeartbeat() {
	mq := rabbitmq.NewAmqpConnection(config.AmqpAddress)
	consumer, err := mq.NewConsumer()
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	consumer.QueName = "heartbeat.queue"
	consumer.Exchange = "apiServers"
	consumeChan, ok := consumer.Consume()

	//retry consume by yourself
	for range time.Tick(5 * time.Second) {
		if ok {
			log.Println("Hearbeat connect success")
			for msg := range consumeChan {
				m, e := string(msg.Body)
				log.Printf("Receive heartbeat from %v\n", m)
			}
			ok = false
		} else {
			log.Println("Hearbeat connection closed! Recovering...")
			consumeChan, ok = consumer.Consume()
		}
	}
}
```

### Logs

```log
2022/04/12 18:42:09 Hearbeat connect success
2022/04/12 18:42:43 Hearbeat connection closed! Recovering...
2022/04/12 18:42:43 Reconnect to amqp://gdfs:gdfs@localhost:5672/goodfs
2022/04/12 18:42:43 Consume heartbeat.queue error Exception (504) Reason: "channel/connection is not open"
2022/04/12 18:42:44 Hearbeat connection closed! Recovering...
2022/04/12 18:42:44 Consume heartbeat.queue error Exception (504) Reason: "channel/connection is not open"
2022/04/12 18:42:48 Reconnect to amqp://gdfs:gdfs@localhost:5672/goodfs fail
2022/04/12 18:42:49 Hearbeat connection closed! Recovering...
2022/04/12 18:42:49 Consume heartbeat.queue error Exception (504) Reason: "channel/connection is not open"
2022/04/12 18:42:53 Reconnect to amqp://gdfs:gdfs@localhost:5672/goodfs fail
2022/04/12 18:42:54 Hearbeat connection closed! Recovering...
2022/04/12 18:42:54 Consume heartbeat.queue error Exception (504) Reason: "channel/connection is not open"
2022/04/12 18:42:58 Reconnect to amqp://gdfs:gdfs@localhost:5672/goodfs fail
2022/04/12 18:42:59 Hearbeat connection closed! Recovering...
2022/04/12 18:42:59 Consume heartbeat.queue error Exception (504) Reason: "channel/connection is not open"
2022/04/12 18:43:03 Reconnect AMQP success
2022/04/12 18:43:03 Broadcasting recovering message..
2022/04/12 18:43:04 Hearbeat connection closed! Recovering...
2022/04/12 18:43:04 Consume heartbeat.queue error Exception (504) Reason: "channel/connection is not open"
2022/04/12 18:43:08 Recovering 4f62b158-b847-4ce6-b4c4-1cbe9451c78b channel...
2022/04/12 18:43:08 Recovering 4f62b158-b847-4ce6-b4c4-1cbe9451c78b channel success
2022/04/12 18:43:09 Hearbeat connection closed! Recovering...
2022/04/12 18:43:14 Hearbeat connect success
```