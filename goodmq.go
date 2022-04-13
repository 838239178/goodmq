package goodmq

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

var (
	Info  = log.New(os.Stderr, "[GoodMQ] |INFO | ", log.LstdFlags)
	Error = log.New(os.Stderr, "[GoodMQ] |ERROR| ", log.LstdFlags)
	Warn  = log.New(os.Stderr, "[GoodMQ] |WARN | ", log.LstdFlags)
	//Debug = log.New(os.Stderr, "[GoodMQ] |DEBUG|", log.LstdFlags|log.Lshortfile)
)

var (
	RecoverDelay = 5 * time.Second
)

type AmqpConnection struct {
	addr           string
	conn           *amqp.Connection
	notifyClose    chan *amqp.Error
	notifyRemove   chan uuid.UUID
	notifyRecovers *sync.Map
	connLock       sync.RWMutex
}

func DailSync(addr string) <-chan *amqp.Connection {
	ch := make(chan *amqp.Connection)
	go func() {
		defer close(ch)
		conn, e := amqp.Dial(addr)
		if e != nil {
			Error.Printf("Dail %v failed, %v\n", addr, e)
			ch <- nil
		} else {
			ch <- conn
		}
	}()
	return ch
}

func DailWithTimeout(addr string, timeout time.Duration) (*amqp.Connection, error) {
	dail := DailSync(addr)
	var conn *amqp.Connection
	select {
	case conn = <-dail:
		if conn == nil {
			return nil, errors.New("unknown")
		}
		return conn, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout")
	}
}

func NewAmqpConnection(addr string) *AmqpConnection {
	conn, e := DailWithTimeout(addr, 10*RecoverDelay)
	if e != nil {
		Error.Panicf("Connection to %v timeout\n", addr)
	}
	var am AmqpConnection
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
	//Debug.Println("Start listening connection notifyClose...")
	for closeErr := range c.notifyClose {
		Warn.Printf("Error:%v. Recoverable: %v, Trying to reconnect to %v\n", closeErr.Reason, closeErr.Recover, c.addr)
		for range time.Tick(RecoverDelay) {
			if e := c.reconnect(); e == nil {
				Info.Printf("Reconnect to %v success\n", c.addr)
				//广播到所有channel
				go c.broadcastRecover()
				break
			} else {
				Warn.Printf("Reconnect to %v fail\n", c.addr)
			}
		}
	}
	//Debug.Println("End listening connection notifyClose...")
}

func handelRemoveChan(c *AmqpConnection) {
	for id := range c.notifyRemove {
		c.RemoveChan(id)
	}
}

func (c *AmqpConnection) broadcastRecover() {
	Info.Println("Broadcasting recovering message..")
	//var cnt int
	c.notifyRecovers.Range(func(key, value any) bool {
		//cnt++
		if recoverChan, ok := value.(chan *AmqpConnection); ok {
			//Debug.Printf("Notify recovering to chan %v\n", key)
			recoverChan <- c
		}
		return true
	})
	//Debug.Printf("Finished broadcasting recovering, total %v\n", cnt)
}

func (c *AmqpConnection) reconnect() error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	if !c.conn.IsClosed() {
		return nil
	}
	conn, e := DailWithTimeout(c.addr, 10*RecoverDelay)
	if e != nil {
		return e
	}
	c.conn = conn
	//pre notifyClose chan will be close in amqp.shutdown
	c.notifyClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.notifyClose)
	go handleReconnect(c)
	return nil
}

func (c *AmqpConnection) RemoveChan(chanId uuid.UUID) {
	if value, ok := c.notifyRecovers.LoadAndDelete(chanId); ok {
		Info.Printf("Remove channel %v \n", chanId)
		if recoverChan, ok2 := value.(chan *AmqpConnection); ok2 {
			close(recoverChan)
			//Debug.Printf("Close chan %v \n", chanId)
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
	Info.Printf("AMPQ normally closed...")
	//chan notifyClose will close in c.conn.Close()
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
	ch.chanId, e = uuid.NewRandom()
	if e != nil {
		return nil, e
	}
	notifyRecoverChan := make(chan *AmqpConnection)
	c.notifyRecovers.Store(ch.chanId, notifyRecoverChan)
	ch.channel = channel
	ch.notifyRecover = notifyRecoverChan
	ch.notifyRemove = c.notifyRemove
	go handleRecover(&ch)
	return &ch, nil
}

func handleRecover(ch *AmqpChannel) {
	//Debug.Printf("Start listening %v recover message\n", ch.chanId)
	for conn := range ch.notifyRecover {
		for range time.Tick(RecoverDelay) {
			//Debug.Printf("Recovering %v channel...\n", ch.chanId)
			if e := ch.recover(conn); e == nil {
				Info.Printf("Recover %v channel success\n", ch.chanId)
				break
			} else {
				Warn.Printf("Recover %v channel fail\n", ch.chanId)
			}
		}
	}
	//Debug.Printf("Stop listening %v recover message\n", ch.chanId)
}

func (ch *AmqpChannel) recover(conn *AmqpConnection) error {
	if ok := ch.chLock.TryLock(); ok {
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

func (ch *AmqpChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool) error {
	ch.chLock.RLock()
	defer ch.chLock.RUnlock()
	return ch.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, nil)
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
	defer ch.chLock.Unlock()
	ch.notifyRemove <- ch.chanId
	return ch.channel.Close()
}
