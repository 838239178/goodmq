package goodmq

import (
	"io"

	"github.com/streadway/amqp"
)

type IAmqpProvider interface {
	io.Closer
	PublishDirect(exchange, routeKey string, msg amqp.Publishing) bool
	Send(exchange string, msg amqp.Publishing) bool
}

type AmqpProvider struct {
	Channel  *AmqpChannel
	Exchange string
	RouteKey string
}

func (p *AmqpProvider) Publish(msg amqp.Publishing) bool {
	return p.PublishDirect(p.Exchange, p.RouteKey, msg)
}

func (p *AmqpProvider) PublishDirect(exchange, routeKey string, msg amqp.Publishing) bool {
	if e := p.Channel.Publish(exchange, routeKey, msg); e != nil {
		Error.Printf("Publish to %v (key=%v) error, %v", p.Exchange, p.RouteKey, e.Error())
		return false
	}
	return true
}

func (p *AmqpProvider) Send(exchg string, msg amqp.Publishing) bool {
	return p.PublishDirect(exchg, "", msg)
}

func (p *AmqpProvider) Close() error {
	return p.Channel.Close()
}
