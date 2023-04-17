package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/exp/slog"
)

var (
	// ErrNotConnected is returned when an operation which requires the client to be connected to the server
	// is invoked but the client still isn't connected.
	ErrNotConnected = errors.New("not connected to the server")
	// ErrPublishTimeout is returned when the client did not succeed to publish a message after the configured
	// publish timeout duration.
	ErrPublishTimeout = errors.New("publish timeout")
	// ErrEmptyConsumerName is returned when a the length of the given consumer name equals zero.
	ErrEmptyConsumerName = errors.New("consumer name should contain at least 1 character")
	// ErrEmptyQueueName is returned when a the length of the given queue name equals zero.
	ErrEmptyQueueName = errors.New("queue name should contain at least 1 character")
)

var (
	// errConnectionTimeout is returned when the client did not succeed to connect to the server after the
	// configured connection timeout duration.
	errConnectionTimeout = errors.New("connection timeout")
)

// Client is a reliable wrapper around an AMQP connection which automatically recover
// from connection errors.
type Client struct {
	// has unexported fields
	config          ClientConfig
	logger          *slog.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	isConnected     atomic.Bool
	notifyConnState chan ConnStateNotification
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyPublish   chan amqp.Confirmation
	cancel          func()
	wg              sync.WaitGroup
}

// NewClient creates a new client instance with the given options.
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
	// See https://www.rabbitmq.com/uri-spec.html
	if cfg.ConnectionConfig.URL == "" {
		u := &url.URL{
			Scheme: "amqp",
			User:   url.UserPassword(cfg.ConnectionConfig.Username, cfg.ConnectionConfig.Password),
			Host:   net.JoinHostPort(cfg.ConnectionConfig.Host, cfg.ConnectionConfig.Port),
			Path:   cfg.ConnectionConfig.VirtualHost,
		}
		cfg.ConnectionConfig.URL = u.String()
	}

	return &Client{
		logger: cfg.Logger,
		config: cfg,
	}
}

// ConnStateNotification represents a notification sent by the client when the
// connection state changes.
// If the connection is lost, the Err field will contain the error which caused
// the connection to be lost.
type ConnStateNotification struct {
	// Connected is true if the client is connected to the server.
	Connected bool
	// Err contains the error which caused the connection to be lost.
	Err error
}

// Connect starts a background job which will try to connect to the broker
// and recover from connection errors. The returned channel will be notified
// when the connection state changes.
//
// If you want a blocking behavior, you can use the following pattern:
//
//	c := rabbitmq.NewClient()
//	notifyConnState := c.Connect()
//	if cs := <-notifyConnState && cs.Err != nil {
//	  // handle error
//	}
//	defer c.Close()
//
// If you want a non-blocking behavior, you can use the following pattern:
//
//	c := rabbitmq.NewClient()
//	notifyConnState := c.Connect()
//	defer c.Close()
//	go func() {
//	  for cs := range notifyConnState {
//	    // handle connection state change
//	  }
//	}()
//
// Timeout and retry delay are configurable through the ClientConfig.
//
// The caller must call Close() when the client is no longer needed to
// release the resources.
func (c *Client) Connect() <-chan ConnStateNotification {
	c.notifyConnState = make(chan ConnStateNotification, 1)

	if c.IsConnected() {
		c.notifyConnState <- ConnStateNotification{Connected: true}
		return c.notifyConnState
	}

	connectionHandlerCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		c.logger.Debug("Starting connection handler")
		err := c.handleConnection(connectionHandlerCtx)
		if err != nil {
			c.notifyConnState <- ConnStateNotification{
				Connected: false,
				Err:       fmt.Errorf("disconnected: %w", err),
			}
		}
	}()

	return c.notifyConnState
}

func (c *Client) handleConnection(ctx context.Context) error {
	timeout := time.After(c.config.ConnectionConfig.Timeout)
	for {
		err := c.connect()
		if err != nil {
			c.logger.Error("Connection attempt failed", slog.Any("err", err))
			select {
			case <-ctx.Done():
				c.logger.Debug("[handleConnection] Context canceled")
				return ctx.Err()
			case <-timeout:
				return errConnectionTimeout
			case <-time.After(c.config.ConnectionConfig.RetryDelay):
				continue
			}
		}

		err = c.handleChannel(ctx)
		if err != nil {
			return err
		}

		timeout = time.After(c.config.ConnectionConfig.Timeout)
	}
}

func (c *Client) connect() error {
	c.logger.Debug("Attempting to connect to the broker",
		slog.String("broker_url", c.config.ConnectionConfig.URL),
	)

	conn, err := amqp.Dial(c.config.ConnectionConfig.URL)
	if err != nil {
		return err
	}
	c.setConnection(conn)

	c.logger.Debug("Successfully connected to the broker!")
	return nil
}

func (c *Client) handleChannel(ctx context.Context) error {
	for {
		err := c.initChannel()
		if err != nil {
			c.logger.Error("Failed to initialize the channel", slog.Any("err", err))
			select {
			case <-ctx.Done():
				c.logger.Debug("[handleChannel] Context canceled")
				return ctx.Err()
			case err = <-c.notifyConnClose:
				c.logger.Error("Connection closed", slog.Any("err", err))
				return nil
			case <-time.After(c.config.ChannelConfig.InitializationRetryDelay):
				continue
			}
		}

		c.notifyConnState <- ConnStateNotification{Connected: true}
		c.isConnected.Store(true)

		c.logger.Debug("Successfully initialized the AMQP channel")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-c.notifyConnClose:
			c.notifyConnState <- ConnStateNotification{
				Connected: false,
				Err:       fmt.Errorf("connection closed: %w", err),
			}
			return nil
		case err = <-c.notifyChanClose:
			c.notifyConnState <- ConnStateNotification{
				Connected: false,
				Err:       fmt.Errorf("channel closed: %w", err),
			}
		}

		c.isConnected.Store(false)
	}
}

func (c *Client) initChannel() error {
	c.logger.Debug("Attempting to initialize the AMQP channel")

	ch, err := c.connection.Channel()
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
		c.config.ChannelConfig.Global,
	)
	if err != nil {
		return err
	}

	c.setChannel(ch)

	return nil
}

// Consume returns a channel which delivers queued messages. You can cancel the context
// to stop the delivery.
//
// The client must be connected to use this method.
func (c *Client) Consume(ctx context.Context, consumerName, queueName string, opts ...ConsumerOption) (<-chan amqp.Delivery, error) {
	if !c.IsConnected() {
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
			c.logger.Debug("Starting consumer",
				slog.String("queue_name", queueName),
			)

			msgs, err := c.channel.Consume(
				queueName,
				consumerName,
				consumerCfg.AutoAck,
				consumerCfg.Exclusive,
				consumerCfg.NoLocal,
				consumerCfg.NoWait,
				consumerCfg.Arguments,
			)
			if err != nil {
				c.logger.Error("Failed to start the consumer",
					slog.Any("err", err),
					slog.String("queue_name", queueName),
				)
				select {
				case <-ctx.Done():
					return
				case <-time.After(consumerCfg.InitializationRetryDelay):
					continue
				}
			}
			c.logger.Debug("Consumer successfully started",
				slog.String("consumer_name", consumerName),
				slog.String("queue_name", queueName),
			)

			done = false

			// TODO: Check if the delivered message are all consumed.
		loop:
			for {
				select {
				case <-ctx.Done():
					if !done {
						c.logger.Debug("Canceling the delivery")

						err := c.channel.Cancel(consumerName, consumerCfg.NoWait)
						if err != nil {
							c.logger.Error("Failed to cancel the delivery",
								slog.Any("err", err),
								slog.String("consumer_name", consumerName),
							)
							return
						}

						done = true
					}
				case msg, ok := <-msgs:
					if !ok {
						c.logger.Debug("Consumed all remaining messages!")

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
	if !c.IsConnected() {
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
			publishCfg.Mandatory,
			publishCfg.Immediate,
			msg,
		)
		if err != nil {
			c.logger.Error("Failed to publish the message",
				slog.Any("err", err),
				slog.String("routing_key", routingKey),
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
		confirmation := <-c.notifyPublish
		if confirmation.Ack {
			return nil
		}

		timeout = time.After(publishCfg.Timeout)
	}
}

// ExchangeDeclare creates an exchange if it does not already exist, and if the exchange exists,
// verifies that it is of the correct and expected class.
func (c *Client) ExchangeDeclare(name string, opts ...ExchangeOption) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	exchangeCfg := DefaultExchangeConfig

	for _, opt := range opts {
		opt(&exchangeCfg)
	}

	return c.channel.ExchangeDeclare(
		name,
		exchangeCfg.Type.String(),
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Arguments,
	)
}

// QueueDeclare creates a queue if it does not already exist, and if the queue exists,
// verifies that it is of the correct and expected class.
func (c *Client) QueueDeclare(name string, opts ...QueueOption) (amqp.Queue, error) {
	if !c.IsConnected() {
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
	if !c.IsConnected() {
		return ErrNotConnected
	}

	return c.channel.QueueBind(queue, routingKey, exchange, false, nil)
}

// Close gracefully shutdown the client. It must be called after any call to Connect().
// It will stop the background job handling the client's connection and close both the
// AMQP channel and connection.
func (c *Client) Close() error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	c.logger.Debug("Closing the client")

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

	c.notifyConnState <- ConnStateNotification{Connected: false}
	close(c.notifyConnState)

	c.isConnected.Store(false)

	c.logger.Debug("Successfully closed the client")
	return nil
}

func (c *Client) IsConnected() bool {
	return c.isConnected.Load()
}

func (c *Client) setConnection(connection *amqp.Connection) {
	c.connection = connection

	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.connection.NotifyClose(c.notifyConnClose)
}

func (c *Client) setChannel(channel *amqp.Channel) {
	c.channel = channel

	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)

	c.notifyPublish = make(chan amqp.Confirmation, 1)
	c.channel.NotifyPublish(c.notifyPublish)
}
