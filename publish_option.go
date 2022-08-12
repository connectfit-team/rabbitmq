package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishOption configures a Publish call.
type PublishOption func(*PublishConfig)

// WithPublishExchangeName specifies the name of the exchange to publish to.
func WithPublishExchangeName(name string) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeName = name
	}
}

// WithPublishRoutingKey specifies the routing key for the message. The routing key
// is used for routing messages depending on the exchange configuration.
func WithPublishRoutingKey(routingKey string) PublishOption {
	return func(pc *PublishConfig) {
		pc.RoutingKey = routingKey
	}
}

// WithMessageTTL will overrides the message expiration property with the given TTL.
//
// For further information: https://www.rabbitmq.com/ttl.html
func WithMessageTTL(ttl time.Duration) PublishOption {
	return func(pc *PublishConfig) {
		pc.TTL = durationToMillisecondsString(ttl)
	}
}

// WithPublishRetryDelay specifies the delay between each publishing attempt.
func WithPublishRetryDelay(delay time.Duration) PublishOption {
	return func(pc *PublishConfig) {
		pc.RetryDelay = delay
	}
}

// WithPublishTimeout specifies the timeout after which the client will stop
// attempting to publish the message.
func WithPublishTimeout(timeout time.Duration) PublishOption {
	return func(pc *PublishConfig) {
		pc.Timeout = timeout
	}
}

// WithMessageHeaders overrides the message headers with the given ones.
func WithMessageHeaders(headers amqp.Table) PublishOption {
	return func(pc *PublishConfig) {
		pc.MessageHeaders = headers
	}
}

// WithMessageHeaders sets a key/value entry in the message headers table.
func WithMessageHeader(key, value string) PublishOption {
	return func(pc *PublishConfig) {
		if pc.MessageHeaders == nil {
			pc.MessageHeaders = make(amqp.Table)
		}
		pc.MessageHeaders[key] = value
	}
}

// WithMessageDelay specifies how long the message should be delayed through
// the delayed exchange. It updates the "x-delay" header of the message.
//
// For further information: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
func WithMessageDelay(delay time.Duration) PublishOption {
	return func(pc *PublishConfig) {
		WithMessageHeader(DelayMessageHeader, durationToMillisecondsString(delay))(pc)
	}
}
