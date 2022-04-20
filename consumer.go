package goodmq

import (
	"errors"
	"github.com/streadway/amqp"
	"time"
)

type AmqpConsumer struct {
	Channel      *AmqpChannel
	queue        amqp.Queue
	hasQue       bool
	QueName      string //QueName 默认为空，自动生成唯一队列并赋值
	ConsumerName string //ConsumerName 用于Consume，默认为空，会自动生成唯一标识符
	AutoAck      bool   //AutoAck 用于Consume 默认为true
	RouteKey     string //RouteKey 用于BindQueue，默认为空，自动生成为队列名称
	Exchange     string //Exchange 用于BindQueue, 默认为空，绑定将返回异常
	Durable      bool   //Durable 用于QueueDeclare，默认为false
	DeleteUnused bool   //DeleteUnused（auto-delete） 用于QueueDeclare，默认false
}

func (cm *AmqpConsumer) DeclareQueue() error {
	if cm.hasQue {
		return nil
	}
	var e error
	if que, e := cm.Channel.QueueDeclare(
		cm.QueName,      // name
		cm.Durable,      // durable
		cm.DeleteUnused, // delete when unused
		false,           // exclusive
		false,           // no-wait
	); e == nil {
		if e = cm.Bind(); e != nil {
			Info.Printf("%v, declared a unbinding queue\n", e)
		}
		cm.QueName = que.Name
		cm.hasQue = true
	}
	return e
}

func (cm *AmqpConsumer) Bind() error {
	if cm.Exchange == "" {
		return errors.New("not set exchange")
	}
	if e := cm.Channel.QueueBind(cm.QueName, cm.RouteKey, cm.Exchange, false); e != nil {
		return e
	}
	return nil
}

func (cm *AmqpConsumer) SetQueue(q amqp.Queue) {
	cm.hasQue = true
	cm.queue = q
	cm.QueName = q.Name
}

func (cm *AmqpConsumer) Consume() (<-chan amqp.Delivery, bool) {
	if !cm.hasQue {
		Info.Printf("Not queue available, declaring %v\n", cm.QueName)
		err := cm.DeclareQueue()
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

func (cm *AmqpConsumer) ConsumeAuto(fn func(delivery amqp.Delivery), interval time.Duration) {
	consumeChan, ok := cm.Consume()
	//immediately start first consume if ok
	if ok {
		Info.Printf("Start consume exchange=%v queue=%v", cm.Exchange, cm.QueName)
		for d := range consumeChan {
			fn(d)
		}
		ok = false
	}
	//start to auto recovery when channel closed first time
	for range time.Tick(interval) {
		if ok {
			Info.Printf("Start consume exchange=%v queue=%v\n", cm.Exchange, cm.QueName)
			for d := range consumeChan {
				fn(d)
			}
			ok = false
		} else {
			Warn.Printf("Disconnected! Recovering consume exchange=%v queue=%v...\n", cm.Exchange, cm.QueName)
			consumeChan, ok = cm.Consume()
		}
	}
}

//AckOne multiply=false
func (cm *AmqpConsumer) AckOne(tag uint64) bool {
	if e := cm.Channel.Ack(tag, false); e != nil {
		Error.Printf("Error in ack tag=%v, %v\n", tag, e)
		return false
	}
	return true
}

//NackSafe multiply=false, requeue=true
func (cm *AmqpConsumer) NackSafe(tag uint64) bool {
	if e := cm.Channel.Nack(tag, false, true); e != nil {
		Error.Printf("Error in nack tag=%v, %v\n", tag, e)
		return false
	}
	return true
}

func (cm *AmqpConsumer) Close() error {
	return cm.Channel.Close()
}
