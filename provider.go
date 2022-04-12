package goodmq

import (
	"log"

	"github.com/streadway/amqp"
)

type AmqpProvider struct {
	Channel  *AmqpChannel
	Exchange string
	RouteKey string
}

func (p *AmqpProvider) Publish(msg amqp.Publishing) bool {
	if e := p.Channel.Publish(p.Exchange, p.RouteKey, msg); e != nil {
		log.Printf("Publish to %v (key=%v) error, %v", p.Exchange, p.RouteKey, e.Error())
		return false
	}
	return true
}

func (cm *AmqpProvider) Close() {
	cm.Channel.Close()
}
