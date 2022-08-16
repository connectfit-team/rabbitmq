package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerOption configures a Consume call.
type ConsumerOption func(*ConsumerConfig)

// WithConsumedQueueName specifies the name of the queue the consumer will consume from.
func WithConsumedQueueName(name string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.QueueName = name
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

// WithConsumeArguments sets the optionnal arguments for the consume call.
func WithConsumeArguments(args amqp.Table) ConsumerOption {
	return func(cc *ConsumerConfig) {
		cc.Arguments = args
	}
}

// WithConsumeArgument sets an optionnal argument for the consume call.
func WithConsumeArgument(key, value string) ConsumerOption {
	return func(cc *ConsumerConfig) {
		if cc.Arguments == nil {
			cc.Arguments = make(amqp.Table)
		}
		cc.Arguments[key] = value
	}
}
