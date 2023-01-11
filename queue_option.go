package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type QueueOption func(*QueueConfig)

func WithQueueBindingRoutingKey(key string) QueueOption {
	return func(qc *QueueConfig) {
		qc.QueueBindRoutingKey = key
	}
}

// WithQueueName specifies the name of the queue.
// The queue name MAY be empty, in which case the server will create a new queue with a unique generated name.
// Queue names starting with "amq." are reserved for pre-declared and standardised queues.
// The queue name can be empty, or a sequence of these characters: letters, digits, hyphen,
// underscore, period, or colon.
func WithQueueName(name string) QueueOption {
	return func(qc *QueueConfig) {
		qc.Name = name
	}
}

// WithQueueDurable determines whether queue is durable or not.
// If true, the queue will be marked as durable. Durable queues remain active when a server restarts.
// Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
// Note that durable queues do not necessarily hold persistent messages,
// although it does not make sense to send persistent messages to a transient queue.
func WithQueueDurable(durable bool) QueueOption {
	return func(qc *QueueConfig) {
		qc.IsDurable = durable
	}
}

// WithQueueExclusive determines whether the queue is exclusive or not.
// Exclusive queues may only be accessed by the current connection, and are deleted when that connection closes.
func WithQueueExclusive(exclusive bool) QueueOption {
	return func(qc *QueueConfig) {
		qc.IsExclusive = exclusive
	}
}

// WithQueueAutoDelete determines whether the queue is deleted when all consumers have finished using it.
// The last consumer can be cancelled either explicitly or because its channel is closed.
// If there was no consumer ever on the queue, it won't be deleted.
func WithQueueAutoDelete(autoDelete bool) QueueOption {
	return func(qc *QueueConfig) {
		qc.AutoDelete = autoDelete
	}
}

// WithQueueArguments configures the optional arguments used by plugins and broker-specific features
// such as message TTL, queue length limit, etc.
//
// For further information: https://www.rabbitmq.com/queues.html#optional-arguments
func WithQueueArguments(args amqp.Table) QueueOption {
	return func(qc *QueueConfig) {
		qc.Arguments = args
	}
}

// WithQueueArgument sets an optional argument of the queue declaration.
func WithQueueArgument(key string, value interface{}) QueueOption {
	return func(qc *QueueConfig) {
		if qc.Arguments == nil {
			qc.Arguments = make(amqp.Table)
		}
		qc.Arguments[key] = value
	}
}

// WithQueueDeadLetterExchange specifies the name of the dead letter exchange used by the queue.
func WithQueueDeadLetterExchange(name string) QueueOption {
	return WithQueueArgument(DeadLetterExchangeNameArgument, name)
}

// WithQueueDeadLetterRoutingKey specifies the routing key used to publish the message
// to the dead letter exchange.
func WithQueueDeadLetterRoutingKey(key string) QueueOption {
	return WithQueueArgument(DeadLetterRoutingKeyArgument, key)
}

// WithQueueQuorum sets the queue type as "Quorum queue".
func WithQueueQuorum() QueueOption {
	return WithQueueArgument(QueueTypeArgument, QuorumQueueType)
}

// WithRedeliveryLimit sets the redelivery-count's limit
// msg's actual consume count is always 1 greater than redelivery limit
// because the first consumption is not counted as redelivery-count
func WithRedeliveryLimit(limit int64) QueueOption {
	return WithQueueArgument(QueueRedeliveryLimit, limit)
}
