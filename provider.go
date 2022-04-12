package goodmq

import (
	"github.com/streadway/amqp"
)

type AmqpProvider struct {
	Channel  *AmqpChannel
	Exchange string
	RouteKey string
}

func (p *AmqpProvider) Publish(msg amqp.Publishing) bool {
	if e := p.Channel.Publish(p.Exchange, p.RouteKey, msg); e != nil {
		Error.Printf("Publish to %v (key=%v) error, %v", p.Exchange, p.RouteKey, e.Error())
		return false
	}
	return true
}

func (p *AmqpProvider) Close() error {
	return p.Channel.Close()
}
