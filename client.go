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

var (
	// ErrNotConnect is returned when an operation which requires the client to be connected to the server
	// is invoked but the client still isn't connected.
	ErrNotConnected = errors.New("not connected to the server")
	// ErrPublishTimeout is returned when the client did not succeed to publish a message after the configured
	// publish timeout duration.
	ErrPublishTimeout = errors.New("publish timeout")
)

var (
	// errConnectionTimeout is returned when the client did not succeed to connect to the server after the
	// configured connection timeout duration.
	errConnectionTimeout = errors.New("connection timeout")
)

// Client is a reliable wrapper around an AMQP connection which automatically recover
// from connection errors.
type Client struct {
	config          ClientConfig
	logger          *log.Logger // TODO: Look for another logger
	connection      *amqp.Connection
	connectionMu    sync.RWMutex
	channel         *amqp.Channel
	channelMu       sync.RWMutex
	ready           bool
	readyMu         sync.RWMutex
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyPublish   chan amqp.Confirmation
	cancel          func()
	wg              sync.WaitGroup
}

// NewClient creates a new client instance.
func NewClient(logger *log.Logger, opts ...ClientOption) *Client {
	cfg := ClientConfig{
		ConnectionConfig: DefaultConnectionConfig,
		ChannelConfig:    DefaultChannelConfig,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	// Format the URL if no URL provided through the options.
	if cfg.ConnectionConfig.URL == "" {
		cfg.ConnectionConfig.URL = fmt.Sprintf(
			"amqp://%s:%s@%s:%s/%s",
			cfg.ConnectionConfig.Username,
			cfg.ConnectionConfig.Password,
			cfg.ConnectionConfig.Host,
			cfg.ConnectionConfig.Port,
			cfg.ConnectionConfig.VirtualHost,
		)
	}

	return &Client{
		logger: logger,
		config: cfg,
	}
}

// Connect starts a job which will asynchronously try to connect to the server at the given URL
// and recover from future connection errors. A call to this method will block until the first
// successful connection.
//
// The caller must as well call the Close() method when he is done with the client in order
// to avoid memory leaks.
func (c *Client) Connect(ctx context.Context) error {
	// Inner context of the client's connection lifetime.
	// Should call the Close method to cancel it.
	connectionHandlerCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := c.handleConnection(connectionHandlerCtx)
		if err != nil {
			c.logger.Printf("Connection lost: %v\n", err)
		}
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

func (c *Client) handleConnection(ctx context.Context) error {
	timeout := time.After(c.config.ConnectionConfig.Timeout)
	for {
		c.logger.Println("Attempting to connect to the server...")
		err := c.connect(ctx)
		if err != nil {
			c.logger.Printf("Connection attempt failed: %v\n", err)
			select {
			case <-timeout:
				return errConnectionTimeout
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.config.ConnectionConfig.RetryDelay):
				continue
			}
		}
		c.logger.Println("Succesfully connected!")

		err = c.handleChannel(ctx)
		if err != nil {
			return err
		}

		c.setIsReady(false)

		timeout = time.After(c.config.ConnectionConfig.Timeout)
	}
}

func (c *Client) handleChannel(ctx context.Context) error {
	for {
		c.logger.Println("Attempting to initialize the channel and the queue...")
		err := c.initChannel(ctx)
		if err != nil {
			c.logger.Printf("Failed to initialize the channel and the queue: %v\n", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err = <-c.notifyConnClose:
				c.logger.Printf("Connection closed: %v\n", err)
				return nil
			case <-time.After(c.config.ChannelConfig.InitializationRetryDelay):
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-c.notifyConnClose:
			c.logger.Printf("Connection closed: %v\n", err)
			return nil
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

func (c *Client) initChannel(ctx context.Context) error {
	ch, err := c.getConnection().Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	err = ch.Qos(
		c.config.ChannelConfig.PrefetchCount,
		c.config.ChannelConfig.PrefetchSize,
		c.config.ChannelConfig.IsGlobal,
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
func (c *Client) Consume(ctx context.Context, opts ...ConsumerOption) (<-chan amqp.Delivery, error) {
	if !c.isReady() {
		return nil, ErrNotConnected
	}

	consumerCfg := DefaultConsumerConfig
	for _, opt := range opts {
		opt(&consumerCfg)
	}

	// We declare the queue before consuming from it in case it doesn't already exist.
	// It is not a problem to redeclare a queue as long as the parameter used to declare
	// it are the same since this operation is idempotent.
	queue, err := c.getChannel().QueueDeclare(
		consumerCfg.QueueConfig.Name,
		consumerCfg.QueueConfig.IsDurable,
		consumerCfg.QueueConfig.AutoDelete,
		consumerCfg.QueueConfig.IsExclusive,
		consumerCfg.QueueConfig.NoWait,
		consumerCfg.QueueConfig.Arguments,
	)
	if err != nil {
		return nil, err
	}

	// Cannot redeclare the default exchange.
	if consumerCfg.ExchangeConfig.Name != "" {
		err = c.getChannel().ExchangeDeclare(
			consumerCfg.ExchangeConfig.Name,
			consumerCfg.ExchangeConfig.Type.String(),
			consumerCfg.ExchangeConfig.IsDurable,
			consumerCfg.ExchangeConfig.IsAutoDelete,
			consumerCfg.ExchangeConfig.IsInternal,
			consumerCfg.ExchangeConfig.IsNoWait,
			consumerCfg.ExchangeConfig.Arguments,
		)
		if err != nil {
			return nil, err
		}

		err = c.getChannel().QueueBind(
			queue.Name,
			consumerCfg.QueueBindRoutingKey,
			consumerCfg.ExchangeConfig.Name,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	// TODO: Benchmark the buffer size.
	out := make(chan amqp.Delivery, 1)

	go func() {
		defer close(out)

		var done bool
		for {
			c.logger.Println("Attempting to start a consumer...")
			msgs, err := c.getChannel().Consume(
				queue.Name,
				consumerCfg.Name,
				consumerCfg.AutoAck,
				consumerCfg.IsExclusive,
				consumerCfg.IsNoLocal,
				consumerCfg.IsNoWait,
				consumerCfg.Arguments,
			)
			if err != nil {
				c.logger.Printf("Could not start to consume the deliveries: %v\n", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(consumerCfg.InitializationRetryDelay):
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
						c.getChannel().Cancel(consumerCfg.Name, consumerCfg.IsNoWait)
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

// delayedQueueSuffix is the suffix attached to the the routing key
// in case no delayed queue name is provided when publishing a message with delay.
const delayedQueueSuffix = "delayed"

// DelayedPublish acts like the Publish method to the little difference that the message is sent
// through the default exchange to a queue dedicated for delayed message with a TTL. Once the
// message expires, it is sent to the user defined exchange with the associated routing key.
//
// Client -> Default exchange -> Delayed queue -> User defined exchange
//
// The name of the queue dedicated to delayed messages is formated as follows: "<routing_key>.delayed".
//
// https://www.cloudamqp.com/docs/delayed-messages.html
func (c *Client) DelayedPublish(ctx context.Context, msg amqp.Publishing, delay time.Duration, opts ...PublishOption) error {
	if !c.isReady() {
		return ErrNotConnected
	}

	publishCfg := DefaultPublishConfig
	for _, opt := range opts {
		opt(&publishCfg)
	}

	// Declare the delayed queue where the messages will wait until expiration.
	delayedQueue, err := c.getChannel().QueueDeclare(
		joinStringsWithDots(publishCfg.RoutingKey, delayedQueueSuffix),
		false,
		false,
		false,
		false,
		amqp.Table{
			DeadLetterExchangeNameArgument: publishCfg.ExchangeConfig.Name,
			DeadLetterRoutingKeyArgument:   publishCfg.RoutingKey,
		},
	)
	if err != nil {
		return err
	}

	// Cannot redeclare the default exchange.
	if publishCfg.ExchangeConfig.Name != "" {
		err := c.getChannel().ExchangeDeclare(
			publishCfg.ExchangeConfig.Name,
			publishCfg.ExchangeConfig.Type.String(),
			publishCfg.ExchangeConfig.IsDurable,
			publishCfg.ExchangeConfig.IsAutoDelete,
			publishCfg.ExchangeConfig.IsInternal,
			publishCfg.ExchangeConfig.IsNoWait,
			publishCfg.ExchangeConfig.Arguments,
		)
		if err != nil {
			return err
		}
	}

	timeout := time.After(publishCfg.Timeout)
	for {
		if publishCfg.MessageHeaders != nil {
			msg.Headers = publishCfg.MessageHeaders
		}
		msg.Expiration = durationToMillisecondsString(delay)
		err := c.channel.Publish(
			"",
			delayedQueue.Name,
			publishCfg.IsMandatory,
			publishCfg.IsImmediate,
			msg,
		)
		if err != nil {
			c.logger.Printf("Failed to publish the message: %v\n", err)
			select {
			case <-timeout:
				return ErrPublishTimeout
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(publishCfg.RetryDelay):
				continue
			}
		}
		confirmation := <-c.notifyPublish
		if confirmation.Ack {
			return nil
		}

		timeout = time.After(publishCfg.Timeout)
	}
}

// Publish tries to publish a message in the channel until it receives a confirmation
// from the server that the message as been successfully published or until it reaches
// the configured timeout.
//
// The client must be connected to use this method.
func (c *Client) Publish(ctx context.Context, msg amqp.Publishing, opts ...PublishOption) error {
	if !c.isReady() {
		return ErrNotConnected
	}

	publishCfg := DefaultPublishConfig
	for _, opt := range opts {
		opt(&publishCfg)
	}

	// Cannot redeclare the default exchange.
	if publishCfg.ExchangeConfig.Name != "" {
		err := c.getChannel().ExchangeDeclare(
			publishCfg.ExchangeConfig.Name,
			publishCfg.ExchangeConfig.Type.String(),
			publishCfg.ExchangeConfig.IsDurable,
			publishCfg.ExchangeConfig.IsAutoDelete,
			publishCfg.ExchangeConfig.IsInternal,
			publishCfg.ExchangeConfig.IsNoWait,
			publishCfg.ExchangeConfig.Arguments,
		)
		if err != nil {
			return err
		}
	}

	timeout := time.After(publishCfg.Timeout)
	for {
		if publishCfg.TTL != "" {
			msg.Expiration = publishCfg.TTL
		}
		if publishCfg.MessageHeaders != nil {
			msg.Headers = publishCfg.MessageHeaders
		}
		err := c.channel.Publish(
			publishCfg.ExchangeConfig.Name,
			publishCfg.RoutingKey,
			publishCfg.IsMandatory,
			publishCfg.IsImmediate,
			msg,
		)
		if err != nil {
			c.logger.Printf("Failed to publish the message: %v\n", err)
			select {
			case <-timeout:
				return ErrPublishTimeout
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(publishCfg.RetryDelay):
				continue
			}
		}
		confirmation := <-c.notifyPublish
		if confirmation.Ack {
			return nil
		}

		timeout = time.After(publishCfg.Timeout)
	}
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

func (c *Client) setChannel(channel *amqp.Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	c.channel = channel

	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)

	c.notifyPublish = make(chan amqp.Confirmation)
	c.channel.NotifyPublish(c.notifyPublish)

}

func (c *Client) getChannel() *amqp.Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	return c.channel
}
