package rabbitmq

import (
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// PublishOption configures a Publish call.
type PublishOption func(*PublishConfig)

// WithPublishExchangeName specifies the name of the exchange to publish to.
// The exchange name consists of a non-empty sequence of these characters:
// letters, digits, hyphen, underscore, period, or colon.
// Exchange names starting with "amq." are reserved for pre-declared and standardised exchanges.
func WithPublishExchangeName(name string) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.Name = name
	}
}

// WithPublishExchangeType specifies the type of the exchange to publish to.
// The exchange types define the functionality of the exchange - i.e. how messages are routed through it.
// It is not valid nor meaningful to attempt to change the type of an existing exchange.
//
// For further information: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
func WithPublishExchangeType(exchangeType ExchangeType) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.Type = exchangeType
	}
}

// WithPublishExchangeDurable determines whether the exchange to publish to is durable or not.
// If true, the exchange will be marked as durable. Durable exchanges remain active when a server restarts.
// Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
func WithPublishExchangeDurable(durable bool) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.IsDurable = durable
	}
}

// WithPublishExchangeAutoDelete determines whether the exchange is deleted when
// all queues have finished using it or not.
func WithPublishExchangeAutoDelete(autoDelete bool) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.IsAutoDelete = autoDelete
	}
}

// WithPublishExchangeInternal determines whether the exchange may not be used directly by publishers,
// but only when bound to other exchanges or not. Internal exchanges are used to construct
// wiring that is not visible to applications.
func WithPublishExchangeInternal(internal bool) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.IsInternal = internal
	}
}

// WithPublishExchangeNoWait determines whether the server will respond to the declare exchange
// method or not.
// If true, the client should not wait for a reply method. If the server could not complete
// the method it will raise a channel or connection exception.
func WithPublishExchangeNoWait(noWait bool) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.IsNoWait = noWait
	}
}

// WithPublishExchangeArguments sets the optional arguments for the exchange declaration.
func WithPublishExchangeArguments(arguments amqp091.Table) PublishOption {
	return func(pc *PublishConfig) {
		pc.ExchangeConfig.Arguments = arguments
	}
}

// WithPublishExchangeArgument sets an optional argument of the exchange declaration.
func WithPublishExchangeArgument(key, value string) PublishOption {
	return func(pc *PublishConfig) {
		if pc.ExchangeConfig.Arguments == nil {
			pc.Arguments = make(amqp091.Table)
		}
		pc.Arguments[key] = value
	}
}

// WithDelayMessageExchangeType configures the exchange as a delayed message exchange
// and sets its exchange type.
//
// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
func WithDelayedMessageExchangeType(exchangeType ExchangeType) PublishOption {
	return func(pc *PublishConfig) {
		WithPublishExchangeType(DelayedMessageExchangeType)(pc)
		WithPublishExchangeArgument(DelayedTypeArgument, exchangeType.String())(pc)
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
func WithMessageHeaders(headers amqp091.Table) PublishOption {
	return func(pc *PublishConfig) {
		pc.MessageHeaders = headers
	}
}

// WithMessageHeaders sets a key/value entry in the message headers table.
func WithMessageHeader(key, value string) PublishOption {
	return func(pc *PublishConfig) {
		if pc.MessageHeaders == nil {
			pc.MessageHeaders = make(amqp091.Table)
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
