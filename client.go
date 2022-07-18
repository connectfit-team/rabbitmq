package rabbitmq

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// DefaultConnectionRetryDelay is the delay between each connection attempt.
	DefaultConnectionRetryDelay = time.Second * 5
	// DefaultConnectionTimeout is the default duration the client will wait until a successful connection.
	DefaultConnectionTimeout = time.Minute * 1
)

var (
	// ErrNotConnect is returned when an operation which requires the client to be connected to the server
	// is invoked but the client still isn't connected.
	ErrNotConnected = errors.New("not connected to the server")
	// ErrAlreadyClosed is retured when trying to close the client more than once.
	ErrAlreadyClosed = errors.New("the client is already closed")
)

type clientConfig struct {
	QueueName            string
	IsQueueDurable       bool
	IsQueueExclusive     bool
	QueueAutoDelete      bool
	QueueNoWait          bool
	ConsumerName         string
	AutoAck              bool
	IsConsumerExclusive  bool
	ConsumerNoWait       bool
	NoLocal              bool
	ConnectionRetryDelay time.Duration
	ConnectionTimeout    time.Duration
	ReInitDelay          time.Duration
}

// Client is reliable a wrapper around an AMQP connection which automatically recover
// from connection errors.
type Client struct {
	config clientConfig
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
		config: clientConfig{
			ConnectionRetryDelay: DefaultConnectionRetryDelay,
			ConnectionTimeout:    DefaultConnectionTimeout,
		},
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

// Connect starts a job which will asynchronously try to connect to the server at the given URL
// and recover from future connection errors. A call to this method will block until the first
// successful connection.
//
// The caller must as well call the Close() method when he is done with the client in order
// to avoid memory leaks.
func (c *Client) Connect(ctx context.Context, url string) error {
	connectionHandlerCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.handleConnection(connectionHandlerCtx, url)
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

func (c *Client) handleConnection(ctx context.Context, url string) {
	timeout := time.After(c.config.ConnectionTimeout)
	for {
		c.logger.Println("Attempting to connect to the server...")
		err := c.connect(ctx, url)
		if err != nil {
			c.logger.Printf("Connection failed: %v\n", err)
			select {
			case <-timeout:
				c.logger.Println("Stopping the connection handler: connection timed out")
				return
			case <-ctx.Done():
				c.logger.Println(ctx.Err())
				return
			case <-time.After(c.config.ConnectionRetryDelay):
				continue
			}
		}
		c.logger.Println("Succesfully connected!")

		if done := c.handleInit(ctx); done {
			break
		}

		c.setIsReady(false)

		timeout = time.After(c.config.ConnectionTimeout)
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
			case <-time.After(c.config.ReInitDelay):
				continue
			}
		}
		c.logger.Println("Successfully initialized channel and queue!")

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

func (c *Client) connect(ctx context.Context, url string) error {
	conn, err := amqp.Dial(url)
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
		c.config.QueueName,
		c.config.IsQueueDurable,
		c.config.QueueAutoDelete,
		c.config.IsQueueExclusive,
		c.config.QueueNoWait,
		nil,
	)
	if err != nil {
		return err
	}
	// Queue can be declared without a name and they'll be given a random one. Therefore
	// we need to keep the queue around to access its name when creating new consumers.
	c.queue = q

	c.setChannel(ch)

	c.setIsReady(true)
	return nil
}

// Close gracefully shutdown the client. It must be called after any call to Connect().
// It will stop the background job handling the client's connection and close both the
// AMQP channel and connection.
func (c *Client) Close() error {
	// Cancel the context of the background connection handler.
	c.cancel()

	err := c.getChannel().Cancel(c.config.ConsumerName, false)
	if err != nil {
		return err
	}
	err = c.getChannel().Close()
	if err != nil {
		return err
	}

	err = c.getConnection().Close()
	if err != nil {
		return err
	}

	c.setIsReady(false)

	c.wg.Wait()
	return nil
}

// Consume returns a channel which delivers queued messages.
//
// The client must be connected to use this method.
func (c *Client) Consume() (<-chan amqp.Delivery, error) {
	if !c.isReady() {
		return nil, ErrNotConnected
	}

	return c.channel.Consume(
		c.queue.Name,
		c.config.ConsumerName,
		c.config.AutoAck,
		c.config.IsConsumerExclusive,
		c.config.NoLocal,
		c.config.ConsumerNoWait,
		nil,
	)
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
