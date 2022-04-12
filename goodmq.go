package goodmq

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type AmqpConnection struct {
	addr           string
	conn           *amqp.Connection
	notifyClose    chan *amqp.Error
	notifyRemove   chan uuid.UUID
	notifyRecovers *sync.Map
	connLock       sync.RWMutex
}

func NewAmqpConnection(addr string) *AmqpConnection {
	var am AmqpConnection
	conn, e := amqp.Dial(addr)
	if e != nil {
		log.Panicf("Dail %v failed, %v\n", addr, e)
	}
	am.addr = addr
	am.connLock = sync.RWMutex{}
	am.conn = conn
	am.notifyRemove = make(chan uuid.UUID, 16)
	am.notifyClose = make(chan *amqp.Error)
	am.notifyRecovers = &sync.Map{}
	am.conn.NotifyClose(am.notifyClose)

	go handleReconnect(&am)
	go handelRemoveChan(&am)

	return &am
}

func handleReconnect(c *AmqpConnection) {
	for range c.notifyClose {
		log.Printf("Reconnect to %v\n", c.addr)
		for range time.Tick(5 * time.Second) {
			if e := c.reconnect(); e == nil {
				log.Println("Reconnect AMQP success")
				//广播到所有channel
				go c.broadcastRecover()
				break
			} else {
				log.Printf("Reconnect to %v fail\n", c.addr)
			}
		}
	}
}

func handelRemoveChan(c *AmqpConnection) {
	for id := range c.notifyRemove {
		c.RemoveChan(id)
	}
}

func (c *AmqpConnection) broadcastRecover() {
	log.Println("Broadcasting recovering message..")
	c.notifyRecovers.Range(func(key, value any) bool {
		if chans, ok := value.(chan *AmqpConnection); ok {
			chans <- c
		}
		return true
	})
}

func (c *AmqpConnection) reconnect() error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	if !c.conn.IsClosed() {
		return nil
	}

	conn, e := amqp.Dial(c.addr)
	if e != nil {
		return e
	}
	c.conn = conn
	c.conn.NotifyClose(c.notifyClose)
	return nil
}

func (c *AmqpConnection) RemoveChan(chanId uuid.UUID) {
	if value, ok := c.notifyRecovers.LoadAndDelete(chanId); ok {
		if chans, ok := value.(chan *AmqpConnection); ok {
			close(chans)
		}
	}
}

func (c *AmqpConnection) NewChannel() (*amqp.Channel, error) {
	c.connLock.RLock()
	channel, e := c.conn.Channel()
	c.connLock.RUnlock()
	if e != nil {
		return nil, e
	}
	return channel, nil
}

func (c *AmqpConnection) NewConsumer() (*AmqpConsumer, error) {
	ac, err := NewAmqpChannel(c)
	if err != nil {
		return nil, err
	}
	return &AmqpConsumer{
		Channel: ac,
		AutoAck: true,
	}, nil
}

func (c *AmqpConnection) NewProvider() (*AmqpProvider, error) {
	ac, err := NewAmqpChannel(c)
	if err != nil {
		return nil, err
	}
	return &AmqpProvider{Channel: ac}, nil
}

func (c *AmqpConnection) Close() error {
	close(c.notifyClose)
	close(c.notifyRemove)
	//close all recover chan
	c.notifyRecovers.Range(func(key, value any) bool {
		if chans, ok := value.(chan *AmqpConnection); ok {
			close(chans)
		}
		return true
	})
	//help GC olden data
	c.notifyRecovers = nil
	return c.conn.Close()
}

type AmqpChannel struct {
	channel       *amqp.Channel
	chanId        uuid.UUID
	notifyRecover <-chan *AmqpConnection
	notifyRemove  chan<- uuid.UUID
	chLock        sync.RWMutex
}

func NewAmqpChannel(c *AmqpConnection) (*AmqpChannel, error) {
	channel, e := c.NewChannel()
	if e != nil {
		return nil, e
	}
	var ch AmqpChannel
	ch.channel = channel
	chans := make(chan *AmqpConnection)
	ch.chanId, e = uuid.NewRandom()
	if e != nil {
		return nil, e
	}
	c.notifyRecovers.Store(ch.chanId, chans)
	ch.notifyRemove = c.notifyRemove
	ch.notifyRecover = chans
	go handleRecover(&ch)
	return &ch, nil
}

func handleRecover(ch *AmqpChannel) {
	for conn := range ch.notifyRecover {
		for range time.Tick(5 * time.Second) {
			log.Printf("Recovering %v channel...\n", ch.chanId)
			if e := ch.recover(conn); e == nil {
				log.Printf("Recovering %v channel success\n", ch.chanId)
				break
			} else {
				log.Printf("Recover %v fail\n", ch.chanId)
			}
		}
	}
}

func (ch *AmqpChannel) recover(conn *AmqpConnection) error {
	if succ := ch.chLock.TryLock(); succ {
		defer ch.chLock.Unlock()
		if res, e := conn.NewChannel(); e == nil {
			ch.channel = res
		} else {
			return e
		}
	}
	return nil
}

func (ch *AmqpChannel) Publish(exchange, routeKey string, msg amqp.Publishing) error {
	ch.chLock.RLock()
	defer ch.chLock.RUnlock()
	return ch.channel.Publish(exchange, routeKey, false, false, msg)
}

func (ch *AmqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool) (amqp.Queue, error) {
	ch.chLock.RLock()
	defer ch.chLock.RUnlock()
	return ch.channel.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		nil,
	)
}

func (ch *AmqpChannel) QueueBind(name, key, exchange string, noWait bool) error {
	ch.chLock.RLock()
	defer ch.chLock.RUnlock()
	return ch.channel.QueueBind(name, key, exchange, noWait, nil)
}

func (ch *AmqpChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool) (<-chan amqp.Delivery, error) {
	ch.chLock.RLock()
	defer ch.chLock.RUnlock()
	return ch.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, nil)
}

func (ch *AmqpChannel) Close() error {
	ch.chLock.Lock()
	defer ch.chLock.Lock()
	ch.notifyRemove <- ch.chanId
	return ch.channel.Close()
}
