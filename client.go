package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/exp/slog"
)

var (
	// ErrNotConnect is returned when an operation which requires the client to be connected to the server
	// is invoked but the client still isn't connected.
	ErrNotConnected = errors.New("not connected to the server")
	// ErrAlreadyConnected is returned when trying to connect while the client is already connected to the server.
	ErrAlreadyConnected = errors.New("already connected to the server")
	// ErrPublishTimeout is returned when the client did not succeed to publish a message after the configured
	// publish timeout duration.
	ErrPublishTimeout = errors.New("publish timeout")
	// ErrEmptyConsumerName is returned when a the length of the given consumer name equals zero.
	ErrEmptyConsumerName = errors.New("consumer name should contain at least 1 character")
	// ErrEmptyQueueName is returned when a the length of the given queue name equals zero.
	ErrEmptyQueueName = errors.New("queue name should contain at least 1 character")
)

// Client is a reliable wrapper around an AMQP connection which automatically recover
// from connection errors.
type Client struct {
	config             ClientConfig
	logger             *slog.Logger
	connection         *amqp.Connection
	channel            *amqp.Channel
	isReady            atomic.Bool
	notifyConnClose    chan *amqp.Error
	notifyChanClose    chan *amqp.Error
	notifyPublish      chan amqp.Confirmation
	publishConformWait bool
	cancel             func()
	wg                 sync.WaitGroup
}

// NewClient creates a new client instance.
func NewClient(opts ...ClientOption) *Client {
	cfg := ClientConfig{
		ConnectionConfig: DefaultConnectionConfig,
		ChannelConfig:    DefaultChannelConfig,
		Logger:           slog.Default(),
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
		logger: cfg.Logger,
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
	if c.isReady.Load() {
		return ErrAlreadyConnected
	}

	// Inner context of the client's connection lifetime.
	// Should call the Close method to cancel it.
	connectionHandlerCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		err := c.handleConnection(connectionHandlerCtx)
		if err != nil {
			c.logger.Error("Connection lost", err)
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
			if c.isReady.Load() {
				return nil
			}
		}
	}
}

func (c *Client) handleConnection(ctx context.Context) error {
	for {
		c.logger.Info("Attempting to connect to the server...")

		err := c.connect(ctx)
		if err != nil {
			c.logger.Error("Connection attempt failed", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.config.ConnectionConfig.RetryDelay):
				continue
			}
		}
		c.logger.Info("Succesfully connected!")

		err = c.handleChannel(ctx)
		if err != nil {
			return err
		}

		c.isReady.Store(false)
	}
}

func (c *Client) handleChannel(ctx context.Context) error {
	for {
		c.logger.Info("Attempting to initialize the channel...")

		err := c.initChannel(ctx)
		if err != nil {
			c.logger.Error("Failed to initialize the channel", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err = <-c.notifyConnClose:
				c.logger.Error("Connection closed", err)
				return nil
			case <-time.After(c.config.ChannelConfig.InitializationRetryDelay):
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-c.notifyConnClose:
			c.logger.Error("Connection closed", err)
			return nil
		case err := <-c.notifyChanClose:
			c.logger.Error("Channel closed", err)
		}

		c.isReady.Store(false)
	}
}

func (c *Client) connect(ctx context.Context) error {
	c.logger.Info("Attempting to connect to the broker",
		"broker_url", c.config.ConnectionConfig.URL,
	)

	conn, err := amqp.Dial(c.config.ConnectionConfig.URL)
	if err != nil {
		return err
	}
	c.setConnection(conn)
	return nil
}

func (c *Client) initChannel(ctx context.Context) error {
	ch, err := c.connection.Channel()
	if err != nil {
		return err
	}

	if err = c.setChannel(ch); err != nil {
		return err
	}

	c.logger.Info("Successfully initialized channel!")

	c.isReady.Store(true)

	return nil
}

// Consume returns a channel which delivers queued messages. You can cancel the context
// to stop the delivery.
//
// The client must be connected to use this method.
func (c *Client) Consume(ctx context.Context, consumerName, queueName string, opts ...ConsumerOption) (<-chan amqp.Delivery, error) {
	if !c.isReady.Load() {
		return nil, ErrNotConnected
	}

	if consumerName == "" {
		return nil, ErrEmptyConsumerName
	}

	if queueName == "" {
		return nil, ErrEmptyQueueName
	}

	consumerCfg := DefaultConsumerConfig
	for _, opt := range opts {
		opt(&consumerCfg)
	}

	// TODO: Benchmark the buffer size.
	out := make(chan amqp.Delivery, 1)

	go func() {
		defer close(out)

		var done bool
		for {
			c.logger.Info("Starting consumer",
				"queue_name", queueName,
			)

			msgs, err := c.channel.Consume(
				queueName,
				consumerName,
				consumerCfg.AutoAck,
				consumerCfg.IsExclusive,
				consumerCfg.IsNoLocal,
				consumerCfg.IsNoWait,
				consumerCfg.Arguments,
			)
			if err != nil {
				c.logger.Error("Failed to start the consumer", err,
					"queue_name", queueName,
				)
				select {
				case <-ctx.Done():
					return
				case <-time.After(consumerCfg.InitializationRetryDelay):
					continue
				}
			}
			c.logger.Info("Consumer successfully started!",
				"consumer_name", consumerName,
				"queue_name", queueName,
			)

			done = false
		loop:
			for {
				select {
				case <-ctx.Done():
					if !done {
						c.logger.Info("Canceling the delivery...")

						err := c.channel.Cancel(consumerName, consumerCfg.IsNoWait)
						if err != nil {
							c.logger.Error("Failed to cancel the delivery", err,
								"consumer_name", consumerName,
							)
							return
						}

						done = true
					}
				case msg, ok := <-msgs:
					if !ok {
						c.logger.Info("Consumed all remaining messages!")

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

// Publish tries to publish a message in the channel until it receives a confirmation
// from the server that the message as been successfully published or until it reaches
// the configured timeout.
//
// The client must be connected to use this method.
func (c *Client) Publish(ctx context.Context, msg amqp.Publishing, routingKey string, opts ...PublishOption) error {
	if !c.isReady.Load() {
		return ErrNotConnected
	}

	publishCfg := DefaultPublishConfig
	for _, opt := range opts {
		opt(&publishCfg)
	}

	timeout := time.After(publishCfg.Timeout)
	for {
		if publishCfg.TTL != "" {
			msg.Expiration = publishCfg.TTL
		}
		if publishCfg.MessageHeaders != nil {
			msg.Headers = publishCfg.MessageHeaders
		}
		err := c.channel.PublishWithContext(
			ctx,
			publishCfg.ExchangeName,
			routingKey,
			publishCfg.IsMandatory,
			publishCfg.IsImmediate,
			msg,
		)
		if err != nil {
			c.logger.Error("Failed to publish the message", err,
				"routing_key", routingKey,
			)
			select {
			case <-timeout:
				return ErrPublishTimeout
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(publishCfg.RetryDelay):
				continue
			}
		}
		if c.publishConformWait {
			confirmation := <-c.notifyPublish
			if confirmation.Ack {
				return nil
			}
		}
		timeout = time.After(publishCfg.Timeout)
	}
}

// ExchangeDeclare creates an exchange if it does not already exist, and if the exchange exists,
// verifies that it is of the correct and expected class.
func (c *Client) ExchangeDeclare(name string, opts ...ExchangeOption) error {
	if !c.isReady.Load() {
		return ErrNotConnected
	}

	exchangeCfg := DefaultExchangeConfig

	for _, opt := range opts {
		opt(&exchangeCfg)
	}

	return c.channel.ExchangeDeclare(
		name,
		exchangeCfg.Type.String(),
		exchangeCfg.IsDurable,
		exchangeCfg.IsAutoDelete,
		exchangeCfg.IsInternal,
		exchangeCfg.IsNoWait,
		exchangeCfg.Arguments,
	)
}

// QueueDeclare creates a queue if it does not already exist, and if the queue exists,
// verifies that it is of the correct and expected class.
func (c *Client) QueueDeclare(name string, opts ...QueueOption) (amqp.Queue, error) {
	if !c.isReady.Load() {
		return amqp.Queue{}, ErrNotConnected
	}

	queueCfg := DefaultQueueConfig

	for _, opt := range opts {
		opt(&queueCfg)
	}

	return c.channel.QueueDeclare(
		name,
		queueCfg.IsDurable,
		queueCfg.AutoDelete,
		queueCfg.IsExclusive,
		queueCfg.NoWait,
		queueCfg.Arguments,
	)
}

// QueueBind binds a queue to an exchange.
func (c *Client) QueueBind(queue, exchange, routingKey string) error {
	if !c.isReady.Load() {
		return ErrNotConnected
	}

	return c.channel.QueueBind(queue, routingKey, exchange, false, nil)
}

// Close gracefully shutdown the client. It must be called after any call to Connect().
// It will stop the background job handling the client's connection and close both the
// AMQP channel and connection.
func (c *Client) Close() error {
	c.logger.Info("Closing the client...")
	// Cancel the context of the background connection handler.
	c.cancel()
	c.wg.Wait()

	if !c.channel.IsClosed() {
		err := c.channel.Close()
		if err != nil {
			return err
		}
	}

	if !c.connection.IsClosed() {
		err := c.connection.Close()
		if err != nil {
			return err
		}
	}

	c.isReady.Store(false)

	c.logger.Info("Successfully closed the client.")
	return nil
}

func (c *Client) IsReady() bool {
	return c.isReady.Load()
}

func (c *Client) setConnection(connection *amqp.Connection) {
	c.connection = connection

	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.connection.NotifyClose(c.notifyConnClose)
}

func (c *Client) setChannel(channel *amqp.Channel) error {
	c.channel = channel

	if err := c.channel.Qos(
		c.config.ChannelConfig.PrefetchCount,
		c.config.ChannelConfig.PrefetchSize,
		c.config.ChannelConfig.IsGlobal,
	); err != nil {
		return err
	}

	if !c.config.ChannelConfig.PublishConfirmNoWait {
		c.publishConformWait = true

		if err := c.channel.Confirm(false); err != nil {
			return err
		}

		if c.config.ChannelConfig.NotifyPublishCapacity < 1 {
			c.config.ChannelConfig.NotifyPublishCapacity = 1
		}
		c.notifyPublish = make(chan amqp.Confirmation, c.config.ChannelConfig.NotifyPublishCapacity)
		c.channel.NotifyPublish(c.notifyPublish)
	}

	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)

	return nil
}
