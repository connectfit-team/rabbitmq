package rabbitmq

import "time"

// ClientOption are options used to configure the client.
type ClientOption func(*Client)

// WithURL configures the URL the client will use to dial with the server.
// If this option is used, username, password, host and virtual host options
// will be ignored.
func WithURL(url string) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.URL = url
	}
}

// WithUsername configures the username the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithUsername(username string) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.Username = username
	}
}

// WithPassword configures the password the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithPassword(password string) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.Password = password
	}
}

// WithHost configures the host the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithHost(host string) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.Host = host
	}
}

// WithPort configures the port the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithPort(port string) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.Port = port
	}
}

// WithVirtualHost configures the virtual host the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithVirtualHost(vhost string) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.VirtualHost = vhost
	}
}

// WithQueueName configures the queue name that will be used by the client to declare the queue
// in case you don't RabbitMQ to generate a default name for you.
func WithQueueName(name string) ClientOption {
	return func(c *Client) {
		c.config.QueueConfig.Name = name
	}
}

// WithQueueDurable ensures the queue declared by the client will be durable and will survive
// server restarts.
func WithQueueDurable(isQueueDurable bool) ClientOption {
	return func(c *Client) {
		c.config.QueueConfig.IsDurable = isQueueDurable
	}
}

// WithConsumerName configures the name the client will use when creating new consumers.
func WithConsumerName(name string) ClientOption {
	return func(c *Client) {
		c.config.ConsumerConfig.Name = name
	}
}

// WithConnectionTimeout configures the time the client will wait until a succesful connection.
func WithConnectionTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.Timeout = timeout
	}
}

// WithConnectRetryDelay configures the delay used by the client between each connection retry.
func WithConnectionRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ConnectionConfig.RetryDelay = delay
	}
}

// WithChannelInitializationRetryDelay configures the delay used by the client between each
// initialization(channel and queue declaration).
func WithChannelInitializationRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ChannelConfig.InitializationRetryDelay = delay
	}
}

// WithPrefetchCount configures the number of messages the channel will keep for
// a consumer before receiving acknowledgment.
func WithPrefetchCount(prefetchCount int) ClientOption {
	return func(c *Client) {
		c.config.ChannelConfig.PrefetchCount = prefetchCount
	}
}
