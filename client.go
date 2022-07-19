package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// DefaultConnectionTimeout is the default duration the client will wait until a successful connection.
	DefaultConnectionTimeout = time.Minute * 1
	// DefaultConnectionRetryDelay is the delay between each connection attempt.
	DefaultConnectionRetryDelay = time.Second * 5
	// DefaultChannelInitializationRetryDelay is the delay between each channel initialization attempt.
	DefaultChannelInitializationRetryDelay = time.Second * 5
	// DefaultConsumerInitializationRetryDelay is the delay between each consumer initialization attempt.
	DefaultConsumerInitializationRetryDelay = time.Second * 5
)

var (
	// ErrNotConnect is returned when an operation which requires the client to be connected to the server
	// is invoked but the client still isn't connected.
	ErrNotConnected = errors.New("not connected to the server")
	// ErrAlreadyClosed is retured when trying to close the client more than once.
	ErrAlreadyClosed = errors.New("the client is already closed")
)

type ClientConfig struct {
	ConnectionConfig ConnectionConfig
	ChannelConfig    ChannelConfig
	QueueConfig      QueueConfig
	ConsumerConfig   ConsumerConfig
}

type ConnectionConfig struct {
	URL         string
	Username    string
	Password    string
	Host        string
	Port        string
	VirtualHost string
	RetryDelay  time.Duration
	Timeout     time.Duration
}

var DefaultConnectionConfig = ConnectionConfig{
	Username:   "guest",
	Password:   "guest",
	Host:       "localhost",
	Port:       "5672",
	RetryDelay: DefaultConnectionRetryDelay,
	Timeout:    DefaultConnectionTimeout,
}

type ChannelConfig struct {
	PrefetchCount            int
	PrefetchSize             int
	Global                   bool
	InitializationRetryDelay time.Duration
}

var DefaultChannelConfig = ChannelConfig{
	PrefetchCount:            1,
	InitializationRetryDelay: DefaultChannelInitializationRetryDelay,
}

type ConsumerConfig struct {
	Name                     string
	AutoAck                  bool
	IsExclusive              bool
	NoWait                   bool
	NoLocal                  bool
	InitializationRetryDelay time.Duration
}

var DefaultConsumerConfig = ConsumerConfig{
	InitializationRetryDelay: DefaultConsumerInitializationRetryDelay,
}

type QueueConfig struct {
	Name        string
	IsDurable   bool
	IsExclusive bool
	AutoDelete  bool
	NoWait      bool
}

var DefaultQueueConfig = QueueConfig{
	IsDurable: true,
}

// Client is a reliable wrapper around an AMQP connection which automatically recover
// from connection errors.
type Client struct {
	config ClientConfig
	// TODO: Look for other logger.
	logger          *log.Logger
	connection      *amqp.Connection
	connectionMu    sync.RWMutex
	channel         *amqp.Channel
	channelMu       sync.RWMutex
	queue           amqp.Queue
	ready           bool
	readyMu         sync.RWMutex
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	cancel          func()
	wg              sync.WaitGroup
}

// NewClient creates a new client instance.
func NewClient(logger *log.Logger, opts ...ClientOption) *Client {
	client := &Client{
		logger: logger,
		config: ClientConfig{
			ConnectionConfig: DefaultConnectionConfig,
			ChannelConfig:    DefaultChannelConfig,
			QueueConfig:      DefaultQueueConfig,
			ConsumerConfig:   DefaultConsumerConfig,
		},
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.config.ConnectionConfig.URL == "" {
		client.config.ConnectionConfig.URL = fmt.Sprintf(
			"amqp://%s:%s@%s:%s/%s",
			client.config.ConnectionConfig.Username,
			client.config.ConnectionConfig.Password,
			client.config.ConnectionConfig.Host,
			client.config.ConnectionConfig.Port,
			client.config.ConnectionConfig.VirtualHost,
		)
	}

	return client
}

// Connect starts a job which will asynchronously try to connect to the server at the given URL
// and recover from future connection errors. A call to this method will block until the first
// successful connection.
//
// The caller must as well call the Close() method when he is done with the client in order
// to avoid memory leaks.
func (c *Client) Connect(ctx context.Context) error {
	connectionHandlerCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.handleConnection(connectionHandlerCtx)
	}()

	for {
		select {
		case <-ctx.Done():
			cancel()
			c.wg.Wait()
			return ctx.Err()
		default:
			// TODO: Might check for a cleaner way to do this.
			if c.isReady() {
				return nil
			}
		}
	}
}

func (c *Client) handleConnection(ctx context.Context) {
	timeout := time.After(c.config.ConnectionConfig.Timeout)
	for {
		c.logger.Println("Attempting to connect to the server...")
		err := c.connect(ctx)
		if err != nil {
			c.logger.Printf("Connection failed: %v\n", err)
			select {
			case <-timeout:
				c.logger.Println("Stopping the connection handler: connection timed out")
				return
			case <-ctx.Done():
				c.logger.Println(ctx.Err())
				return
			case <-time.After(c.config.ConnectionConfig.RetryDelay):
				continue
			}
		}
		c.logger.Println("Succesfully connected!")

		if done := c.handleInit(ctx); done {
			break
		}

		c.setIsReady(false)

		timeout = time.After(c.config.ConnectionConfig.Timeout)
	}
}

func (c *Client) handleInit(ctx context.Context) bool {
	for {
		c.logger.Println("Attempting to initialize the channel and the queue...")
		err := c.init(ctx)
		if err != nil {
			c.logger.Printf("Failed to initialize the channel and the queue: %v\n", err)
			select {
			case <-ctx.Done():
				c.logger.Println(ctx.Err())
				return true
			case err = <-c.notifyConnClose:
				c.logger.Printf("Connection closed: %v\n", err)
				return false
			case <-time.After(c.config.ChannelConfig.InitializationRetryDelay):
				continue
			}
		}

		select {
		case <-ctx.Done():
			c.logger.Println(ctx.Err())
			return true
		case err = <-c.notifyConnClose:
			c.logger.Printf("Connection closed: %v\n", err)
			return false
		case err := <-c.notifyChanClose:
			c.logger.Printf("Channel closed: %v\n", err)
		}

		c.setIsReady(false)
	}
}

func (c *Client) connect(ctx context.Context) error {
	c.logger.Printf("Attempting to connect to %s\n", c.config.ConnectionConfig.URL)
	conn, err := amqp.Dial(c.config.ConnectionConfig.URL)
	if err != nil {
		return err
	}
	c.setConnection(conn)
	return nil
}

func (c *Client) init(ctx context.Context) error {
	ch, err := c.getConnection().Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		c.config.QueueConfig.Name,
		c.config.QueueConfig.IsDurable,
		c.config.QueueConfig.AutoDelete,
		c.config.QueueConfig.IsExclusive,
		c.config.QueueConfig.NoWait,
		nil,
	)
	if err != nil {
		return err
	}
	// Queue can be declared without a name and they'll be given a random one. Therefore
	// we need to keep the queue around to access its name when creating new consumers.
	c.queue = q

	err = ch.Qos(
		c.config.ChannelConfig.PrefetchCount,
		c.config.ChannelConfig.PrefetchSize,
		c.config.ChannelConfig.Global,
	)
	if err != nil {
		return err
	}

	c.setChannel(ch)

	c.logger.Println("Successfully initialized channel and queue!")

	c.setIsReady(true)

	return nil
}

// Close gracefully shutdown the client. It must be called after any call to Connect().
// It will stop the background job handling the client's connection and close both the
// AMQP channel and connection.
func (c *Client) Close() error {
	c.logger.Println("Closing the client...")
	// Cancel the context of the background connection handler.
	c.cancel()
	c.wg.Wait()

	if !c.getChannel().IsClosed() {
		err := c.getChannel().Close()
		if err != nil {
			return err
		}
	}

	if !c.getConnection().IsClosed() {
		err := c.getConnection().Close()
		if err != nil {
			return err
		}
	}

	c.setIsReady(false)

	c.logger.Println("Successfully closed the client.")
	return nil
}

// Consume returns a channel which delivers queued messages. You can cancel the context
// to stop the delivery.
//
// The client must be connected to use this method.
// TODO: Might want to implement a consumer/channel pool in the future if we encounter
// any performance issue.
func (c *Client) Consume(ctx context.Context) (<-chan amqp.Delivery, error) {
	if !c.isReady() {
		return nil, ErrNotConnected
	}

	// TODO: Benchmark the buffer size.
	out := make(chan amqp.Delivery, 1)

	go func() {
		defer close(out)

		var done bool
		for {
			c.logger.Println("Attempting to start a consumer...")
			msgs, err := c.getChannel().Consume(
				c.queue.Name,
				c.config.ConsumerConfig.Name,
				c.config.ConsumerConfig.AutoAck,
				c.config.ConsumerConfig.IsExclusive,
				c.config.ConsumerConfig.NoLocal,
				c.config.ConsumerConfig.NoWait,
				nil,
			)
			if err != nil {
				c.logger.Printf("Could not start to consume the deliveries: %v\n", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(c.config.ConsumerConfig.InitializationRetryDelay):
					continue
				}
			}
			c.logger.Println("Successfully started the consumer!")

			done = false
		loop:
			for {
				select {
				case <-ctx.Done():
					if !done {
						c.logger.Println("Canceling the delivery...")
						c.getChannel().Cancel(c.config.ConsumerConfig.Name, c.config.ConsumerConfig.NoWait)
						done = true
					}
				case msg, ok := <-msgs:
					if !ok {
						c.logger.Println("Consumed all remaining messages!")
						if done {
							return
						}
						break loop
					}
					out <- msg
				}
			}
		}
	}()
	return out, nil
}

func (c *Client) setIsReady(isReady bool) {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	c.ready = isReady
}

func (c *Client) isReady() bool {
	c.readyMu.RLock()
	defer c.readyMu.RUnlock()
	return c.ready
}

func (c *Client) setChannel(channel *amqp.Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	c.channel = channel
	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)
}

func (c *Client) getChannel() *amqp.Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	return c.channel
}

func (c *Client) setConnection(connection *amqp.Connection) {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	c.connection = connection
	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.connection.NotifyClose(c.notifyConnClose)
}

func (c *Client) getConnection() *amqp.Connection {
	c.connectionMu.RLock()
	defer c.connectionMu.RUnlock()
	return c.connection
}
