package rabbitmq

import "time"

// ClientOption are options used to configure the client.
type ClientOption func(*Client)

// WithQueueName configures the queue name that will be used by the client to declare the queue
// in case you don't RabbitMQ to generate a default name for you.
func WithQueueName(name string) ClientOption {
	return func(c *Client) {
		c.config.QueueName = name
	}
}

// WithQueueDurable ensures the queue declared by the client will be durable and will survive
// server restarts.
func WithQueueDurable(isQueueDurable bool) ClientOption {
	return func(c *Client) {
		c.config.IsQueueDurable = isQueueDurable
	}
}

// WithConsumerName configures the name the client will use when creating new consumers.
func WithConsumerName(name string) ClientOption {
	return func(c *Client) {
		c.config.ConsumerName = name
	}
}

// WithConnectionTimeout configures the time the client will wait until a succesful connection.
func WithConnectionTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ConnectionTimeout = timeout
	}
}

// WithConnectRetryDelay configures the delay used by the client between each connection retry.
func WithConnectionRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ConnectionRetryDelay = delay
	}
}

// WithInitRetryDelay configures the delay used by the client between each
// initialization(channel and queue declaration).
func WithInitRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ReInitDelay = delay
	}
}
