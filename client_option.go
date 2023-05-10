package rabbitmq

import (
	"time"

	"golang.org/x/exp/slog"
)

// ClientOption are options used to configure the client.
type ClientOption func(*ClientConfig)

func WithLogger(logger *slog.Logger) ClientOption {
	return func(cc *ClientConfig) {
		cc.Logger = logger
	}
}

// WithURL configures the URL the client will use to dial with the server.
// If this option is used, username, password, host and virtual host options
// will be ignored.
func WithURL(url string) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.URL = url
	}
}

// WithUsername configures the username the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithUsername(username string) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.Username = username
	}
}

// WithPassword configures the password the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithPassword(password string) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.Password = password
	}
}

// WithHost configures the host the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithHost(host string) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.Host = host
	}
}

// WithPort configures the port the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithPort(port string) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.Port = port
	}
}

// WithVirtualHost configures the virtual host the client will use to connect to the server.
// This option is ignored if you supplied an URL in the option.
func WithVirtualHost(vhost string) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.VirtualHost = vhost
	}
}

// WithConnectionRetryDelay configures the delay used by the client between each connection retry.
func WithConnectionRetryDelay(delay time.Duration) ClientOption {
	return func(cc *ClientConfig) {
		cc.ConnectionConfig.RetryDelay = delay
	}
}

// WithChannelInitializationRetryDelay configures the delay used by the client between each
// initialization(channel declaration).
func WithChannelInitializationRetryDelay(delay time.Duration) ClientOption {
	return func(cc *ClientConfig) {
		cc.ChannelConfig.InitializationRetryDelay = delay
	}
}

// WithPrefetchCount configures the number of messages the channel will keep for
// a consumer before receiving acknowledgment.
func WithPrefetchCount(count int) ClientOption {
	return func(cc *ClientConfig) {
		cc.ChannelConfig.PrefetchCount = count
	}
}
