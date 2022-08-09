package rabbitmq

import (
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// ConsumerOption configures a Consume call.
type ConsumerOption func(*ConsumerConfig)

// WithConsumedExchangeName specifies the name of the exchange bound to the queue
// the consumer will consume from.
// The exchange name consists of a non-empty sequence of these characters:
// letters, digits, hyphen, underscore, period, or colon.
// Exchange names starting with "amq." are reserved for pre-declared and standardised exchanges.
func WithConsumedExchangeName(name string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.Name = name
	}
}

// WithConsumedExchangeType specifies the type of the exchange bound to the queue
// the consumer will consume from.
// The exchange types define the functionality of the exchange - i.e. how messages are routed through it.
// It is not valid nor meaningful to attempt to change the type of an existing exchange.
//
// For further information: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
func WithConsumedExchangeType(exchangeType ExchangeType) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.Type = exchangeType
	}
}

// WithConsumedExchangeDurable determines whether the exchange bound to the queue
// the consumer will consume from is durable or not.
// If true, the exchange will be marked as durable. Durable exchanges remain active when a server restarts.
// Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
func WithConsumedExchangeDurable(durable bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.IsDurable = durable
	}
}

// WithConsumedExchangeAutoDelete determines whether the exchange bound to the queue
// the consumer will consume from is deleted when all queues have finished using it or not.
func WithConsumedExchangeAutoDelete(autoDelete bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.IsAutoDelete = autoDelete
	}
}

// WithConsumedExchangeInternal determines whether the exchange bound to the queue
// the consumer will consume from may not be used directly by publishers or not, but only when bound to other exchanges.
// Internal exchanges are used to construct wiring that is not visible to applications.
func WithConsumedExchangeInternal(internal bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.IsInternal = internal
	}
}

// WithConsumedExchangeNoWait determines whether the server will respond to the exchange
// declare method or not.
// If true, the client should not wait for a reply method. If the server could not complete
// the method it will raise a channel or connection exception.
func WithConsumedExchangeNoWait(noWait bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.IsNoWait = noWait
	}
}

// WithConsumedExchangeArguments sets the optional arguments for the exchange declaration.
func WithConsumedExchangeArguments(arguments amqp091.Table) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.ExchangeConfig.Arguments = arguments
	}
}

// WithPublishExchangeArgument sets an optional argument of the exchange declaration.
func WithConsumedExchangeArgument(key, value string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		if cc.ExchangeConfig.Arguments == nil {
			cc.ExchangeConfig.Arguments = make(amqp091.Table)
		}
		cc.ExchangeConfig.Arguments[key] = value
	}
}

// WithConsumedExchangeDelayMessageExchangeType configures the exchange as a delayed message exchange
// and sets its exchange type.
//
// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
func WithConsumedExchangeDelayedMessageExchangeType(exchangeType ExchangeType) ConsumerOption {
	return func(cc *ConsumerConfig) {
		WithConsumedExchangeType(DelayedMessageExchangeType)(cc)
		WithConsumedExchangeArgument(DelayedTypeArgument, exchangeType.String())(cc)
	}
}

func WithConsumedQueueBindingRoutingKey(key string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueConfig.QueueBindRoutingKey = key
	}
}

// WithConsumedQueueName specifies the name of the queue the consumer will consume from.
// The queue name MAY be empty, in which case the server will create a new queue with a unique generated name.
// Queue names starting with "amq." are reserved for pre-declared and standardised queues.
// The queue name can be empty, or a sequence of these characters: letters, digits, hyphen,
// underscore, period, or colon.
func WithConsumedQueueName(name string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueConfig.Name = name
	}
}

// WithConsumedQueue determines whether the consumed queue is durable or not.
// If true, the queue will be marked as durable. Durable queues remain active when a server restarts.
// Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
// Note that durable queues do not necessarily hold persistent messages,
// although it does not make sense to send persistent messages to a transient queue.
func WithConsumedQueueDurable(durable bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueConfig.IsDurable = durable
	}
}

// WithConsumedQueueExclusive determines whether the consumed queue is exclusive or not.
// Exclusive queues may only be accessed by the current connection, and are deleted when that connection closes.
func WithConsumedQueueExclusive(exclusive bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueConfig.IsExclusive = exclusive
	}
}

// WithConsumedQueueAutoDelete determines whether the consumed queue is deleted
// when all consumers have finished using it.
// The last consumer can be cancelled either explicitly or because its channel is closed.
// If there was no consumer ever on the queue, it won't be deleted.
func WithConsumedQueueAutoDelete(autoDelete bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueConfig.AutoDelete = autoDelete
	}
}

// WithConsumedQueueArguments configures the optional arguments used by plugins and broker-specific features
// such as message TTL, queue length limit, etc.
//
// For further information: https://www.rabbitmq.com/queues.html#optional-arguments
func WithConsumedQueueArguments(args amqp091.Table) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueConfig.Arguments = args
	}
}

// WithConsumedQueueArgument sets an optional argument of the consumed queue declaration.
func WithConsumedQueueArgument(key, value string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		if cc.Arguments == nil {
			cc.QueueConfig.Arguments = make(amqp091.Table)
		}
		cc.QueueConfig.Arguments[key] = value
	}
}

// WithConsumedQueueDeadLetterExchange specifies the name of the dead letter exchange used by the
// consumed queue.
func WithConsumedQueueDeadLetterExchange(name string) ConsumerOption {
	return WithConsumedQueueArgument(DeadLetterExchangeNameArgument, name)
}

// WithConsumerQueueDeadLetterRoutingKey specifies the routing key used to publish the message
// to the dead letter exchange.
func WithConsumedQueueDeadLetterRoutingKey(key string) ConsumerOption {
	return WithConsumedQueueArgument(DeadLetterRoutingKeyArgument, key)
}

// WithConsumedQueueQuorum sets the consumed queue type as "Q"uorum queue".
func WithConsumedQueueQuorum() ConsumerOption {
	return WithConsumedQueueArgument(QueueTypeArgument, QuorumQueueType)
}

// WithConsumerName specifies the name of the consumer.
// The consumer tag is local to a channel, so two clients can use the same consumer tags.
// If this field is empty the server will generate a unique tag.
func WithConsumerName(name string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.Name = name
	}
}

// WithConsumerAutoAck determines whether the server expect acknowledgments for messages
// from the consumer or not.
// That is, when a message is delivered to the client the server assumes the delivery will succeed and immediately dequeues it.
// This functionality may increase performance but at the cost of reliability.
// Messages can get lost if a client dies before they are delivered to the application.
func WithConsumerAutoAck(autoAck bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.AutoAck = autoAck
	}
}

// WithConsumerExclusive determines whether only this consumer can access the queue or not.
func WithConsumerExclusive(exclusive bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.IsExclusive = exclusive
	}
}

// WithConsumerNoWait determines whether the server will respond to the consume method or not.
// If true, the client should not wait for a reply method. If the server could not complete
// the method it will raise a channel or connection exception.
func WithConsumerNoWait(noWait bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.IsNoWait = noWait
	}
}

// WithConsumerNoLocal determines whether the server will send messages to the connection
// that published them or not.
func WithConsumerNoLocal(noLocal bool) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.IsNoLocal = noLocal
	}
}

// WithConsumerInitializationRetryDelay sets the delay the client will wait between each
// consumer initialization attempt.
func WithConsumerInitializationRetryDelay(delay time.Duration) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.InitializationRetryDelay = delay
	}
}

// WithConsumeArguments sets the arguments that pass to Consume
func WithConsumeArguments(args amqp091.Table) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.Arguments = args
	}
}

// WithConsumeArgument sets an optionnal argument for the consume call.
func WithConsumeArgument(key, value string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		if cc.Arguments == nil {
			cc.Arguments = make(amqp091.Table)
		}
		cc.Arguments[key] = value
	}
}
