package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type ExchangeOption func(*ExchangeConfig)

// WithExchangeName specifies the name of the exchange.
// The exchange name consists of a non-empty sequence of these characters:
// letters, digits, hyphen, underscore, period, or colon.
// Exchange names starting with "amq." are reserved for pre-declared and standardised exchanges.
func WithExchangeName(name string) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.Name = name
	}
}

// WithExchangeType specifies the type of the exchange.
// The exchange types define the functionality of the exchange - i.e. how messages are routed through it.
// It is not valid nor meaningful to attempt to change the type of an existing exchange.
//
// For further information: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
func WithExchangeType(exchangeType ExchangeType) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.Type = exchangeType
	}
}

// WithExchangeDurable determines whether the exchange is durable or not.
// If true, the exchange will be marked as durable. Durable exchanges remain active when a server restarts.
// Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
func WithExchangeDurable(durable bool) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.IsDurable = durable
	}
}

// WithExchangeAutoDelete determines whether the exchange is deleted when
// all queues have finished using it or not.
func WithExchangeAutoDelete(autoDelete bool) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.IsAutoDelete = autoDelete
	}
}

// WithExchangeInternal determines whether the exchange may not be used directly by publishers,
// but only when bound to other exchanges or not. Internal exchanges are used to construct
// wiring that is not visible to applications.
func WithExchangeInternal(internal bool) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.IsInternal = internal
	}
}

// WithExchangeNoWait determines whether the server will respond to the declare exchange
// method or not.
// If true, the client should not wait for a reply method. If the server could not complete
// the method it will raise a channel or connection exception.
func WithExchangeNoWait(noWait bool) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.IsNoWait = noWait
	}
}

// WithExchangeArguments sets the optional arguments for the exchange declaration.
func WithExchangeArguments(arguments amqp.Table) ExchangeOption {
	return func(ec *ExchangeConfig) {
		ec.Arguments = arguments
	}
}

// WithExchangeArgument sets an optional argument of the exchange declaration.
func WithExchangeArgument(key, value string) ExchangeOption {
	return func(ec *ExchangeConfig) {
		if ec.Arguments == nil {
			ec.Arguments = make(amqp.Table)
		}
		ec.Arguments[key] = value
	}
}

// WithDelayMessageExchangeType configures the exchange as a delayed message exchange
// and sets its exchange type.
//
// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
func WithDelayedMessageExchangeType(exchangeType ExchangeType) ExchangeOption {
	return func(ec *ExchangeConfig) {
		WithExchangeType(DelayedMessageExchangeType)(ec)
		WithExchangeArgument(DelayedTypeArgument, exchangeType.String())(ec)
	}
}
