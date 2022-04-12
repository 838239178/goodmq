package goodmq

import (
	"github.com/streadway/amqp"
)

type AmqpConsumer struct {
	Channel      *AmqpChannel
	queue        amqp.Queue
	hasQue       bool
	ConsumerName string //ConsumerName 默认为空，会自动生成唯一标识符
	RouteKey     string //RouteKey 默认为空
	Exchange     string //Exchange 需要初始化 否则panic
	QueName      string //QueName 默认为空，自动生成唯一队列并赋值
	AutoAck      bool   //AutoAck 默认为true
	Durable      bool   //Durable 默认为false
	DeleteUnused bool   //DeleteUnused 默认false
}

func (cm *AmqpConsumer) declareQueue() error {
	if cm.Exchange == "" {
		Error.Panicln("Please set AmqpConsumer.Exchange!")
	}

	var e error
	if que, e := cm.Channel.QueueDeclare(
		cm.QueName,      // name
		cm.Durable,      // durable
		cm.DeleteUnused, // delete when unused
		false,           // exclusive
		false,           // no-wait
	); e == nil {
		if e = cm.Channel.QueueBind(que.Name, cm.RouteKey, cm.Exchange, false); e != nil {
			return e
		}
		cm.QueName = que.Name
		cm.hasQue = true
		return nil
	}
	return e
}

func (cm *AmqpConsumer) Consume() (<-chan amqp.Delivery, bool) {
	if !cm.hasQue {
		err := cm.declareQueue()
		if err != nil {
			return nil, false
		}
	}
	c, e := cm.Channel.Consume(
		cm.QueName,
		cm.ConsumerName, //consumer name
		cm.AutoAck,      //auto ack
		false,
		false,
		false,
	)
	if e != nil {
		Error.Printf("Consume %v error %v\n", cm.QueName, e)
		return nil, false
	}
	return c, true
}

func (cm *AmqpConsumer) Close() error {
	return cm.Channel.Close()
}
