package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/exp/slog"
)

// ClientConfig represents the configuration of a client instance.
type ClientConfig struct {
	ConnectionConfig ConnectionConfig
	ChannelConfig    ChannelConfig
	Logger           *slog.Logger
}

// ConnectionConfig contains all the configurable parameters used by a client instance
// to connect to a RabbitMQ server.
type ConnectionConfig struct {
	URL         string
	Username    string
	Password    string
	Host        string
	Port        string
	VirtualHost string
	RetryDelay  time.Duration
}

const (
	// DefaultConnectionRetryDelay is the default delay between each connection attempt.
	DefaultConnectionRetryDelay = time.Second * 5
)

// DefaultConnectionConfig is the default configuration used by the client to connect
// to the server.
var DefaultConnectionConfig = ConnectionConfig{
	Username:    "guest",
	Password:    "guest",
	Host:        "localhost",
	Port:        "5672",
	VirtualHost: "",
	RetryDelay:  DefaultConnectionRetryDelay,
}

// ChannelConfig contains all the configurable parameters used by a client instance
// to initialize a RabbitMQ channel.
type ChannelConfig struct {
	PublishConfirmNoWait     bool
	NotifyPublishCapacity    int
	PrefetchCount            int
	PrefetchSize             int
	IsGlobal                 bool
	InitializationRetryDelay time.Duration
}

const (
	// DefaultNotifyPublishCapacity is the default number of confirmation channel buffer size.
	DefaultNotifyPublishCapacity = 1
	// DefaultPrefetchCount is the default number of messages the server will be able to send to your consumer
	// before receiving acknowledgment.
	DefaultPrefetchCount = 1
	// DefaultChannelInitializationRetryDelay is the default delay between each channel initialization attempt.
	DefaultChannelInitializationRetryDelay = time.Second * 5
)

// DefaultChannelConfig is the default configuration used by a client to initialize
// a RabbitMQ channel.
var DefaultChannelConfig = ChannelConfig{
	// PublishConfirmNoWait decides whether the client should wait for a confirmation from the rabbitmq server
	// If PublishConfirmNoWait is set to false, the client will wait for a confirmation from the server
	// and need to handle the confirmation in the notifyPublish channel.
	PublishConfirmNoWait:     false,
	NotifyPublishCapacity:    DefaultNotifyPublishCapacity,
	PrefetchCount:            DefaultPrefetchCount,
	PrefetchSize:             0,
	IsGlobal:                 false,
	InitializationRetryDelay: DefaultChannelInitializationRetryDelay,
}

// PublishConfig contains all the configurable parameters used by a client instance
// to publish messages.
type PublishConfig struct {
	ExchangeName   string
	RoutingKey     string
	IsMandatory    bool
	IsImmediate    bool
	TTL            string // TTL is applied on a message-by-message basis and is represented in millisecond.
	MessageHeaders amqp.Table
	RetryDelay     time.Duration
	Timeout        time.Duration
}

const (
	// DefaultPublishRetryDelay is the default delay between each attempt to publish
	// a RabbitMQ message.
	DefaultPublishRetryDelay = time.Second * 5
	// DefaultPublishTimeout is the default duration the client will wait until the server
	// confirms to the client the message a successfully been published.
	DefaultPublishTimeout = time.Second * 20
)

// DefaultPublishConfig is the default configuration used by a client instance to publish messages.
var DefaultPublishConfig = PublishConfig{
	ExchangeName:   "", // Default exchange
	RoutingKey:     "",
	IsMandatory:    false,
	IsImmediate:    false,
	TTL:            "",
	MessageHeaders: nil,
	RetryDelay:     DefaultPublishRetryDelay,
	Timeout:        DefaultPublishTimeout,
}

// ConsumerConfig contains all the configurable parameters used by a client instance
// to consume messages.
type ConsumerConfig struct {
	QueueName                string
	Name                     string
	AutoAck                  bool
	IsExclusive              bool
	IsNoWait                 bool
	IsNoLocal                bool
	InitializationRetryDelay time.Duration
	Arguments                amqp.Table
}

const (
	// DefaultConsumerInitializationRetryDelay is the delay between each consumer initialization attempt.
	DefaultConsumerInitializationRetryDelay = time.Second * 5
)

// DefaultConsumerConfig is the default configuration used by a client instance to consume messages.
var DefaultConsumerConfig = ConsumerConfig{
	QueueName:                "",
	AutoAck:                  false,
	IsExclusive:              false,
	IsNoWait:                 false,
	IsNoLocal:                false,
	InitializationRetryDelay: DefaultConsumerInitializationRetryDelay,
	Arguments:                nil,
}

// ExchangeType defines the functionality of the exchange i.e. how messages are routed through it.
type ExchangeType string

func (t ExchangeType) String() string {
	return string(t)
}

const (
	// https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-direct
	DirectExchangeType ExchangeType = "direct"
	// https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-fanout
	FanoutExchangeType ExchangeType = "fanout"
	// https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-topic
	TopicExchangeType ExchangeType = "topic"
	// https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-headers
	HeadersExchangeType ExchangeType = "headers"
	// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
	DelayedMessageExchangeType ExchangeType = "x-delayed-message"
)

const (
	// DelayedTypeArgument is the key used to specify the exchange type of a
	// delayed message exchange.
	//
	// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
	DelayedTypeArgument = "x-delayed-type"
)

const (
	// DelayMessageHeader is the key used to specify the delay before publishing
	// the message to the queue.
	//
	// https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
	DelayMessageHeader = "x-delay"
)

// ExchangeConfig contains all the configurable parameters used by a client instance
// to declare an exchange.
type ExchangeConfig struct {
	Name         string
	Type         ExchangeType
	IsDurable    bool
	IsAutoDelete bool
	IsInternal   bool
	IsNoWait     bool
	Arguments    amqp.Table
}

// DefaultExchangeConfig is the default configuration used by a client instance to declare exchanges.
var DefaultExchangeConfig = ExchangeConfig{
	Name:         "", // Default exchange
	Type:         DirectExchangeType,
	IsDurable:    true,
	IsAutoDelete: false,
	IsInternal:   false,
	IsNoWait:     false,
}

// QueueConfig contains all the configurable parameters used by a client instance to declare a
// queue.
type QueueConfig struct {
	Name                string
	QueueBindRoutingKey string
	IsDurable           bool
	IsExclusive         bool
	AutoDelete          bool
	NoWait              bool
	Arguments           amqp.Table
}

// DefaultQueueConfig is the default configuration used by a client instance to declare queues.
var DefaultQueueConfig = QueueConfig{
	Name:                "", // Automatically generated by RabbitMQ
	QueueBindRoutingKey: "",
	IsDurable:           true,
	IsExclusive:         false,
	AutoDelete:          false,
	NoWait:              false,
	Arguments:           nil,
}

const (
	// DeadLetterExchangeNameArgument is the key used in the queue optional arguments
	// to specify the name of the dead letter exchange of a queue through its optional arguments.
	DeadLetterExchangeNameArgument = "x-dead-letter-exchange"
	// DeadLetterRoutingKeyArgument is the key used in the queue optional arguments
	// to specify the routing key of the message sent to the dead letter exchange.
	DeadLetterRoutingKeyArgument = "x-dead-letter-routing-key"
	// QueueTypeArgument is the key used in the queue optional arguments to specify
	// the queue type.
	QueueTypeArgument = "x-queue-type"
	// QueueDeliveryLimit is queue optional arguments to limit
	// redelivery count('x-delivery-count') of unacked message
	// https://www.rabbitmq.com/quorum-queues.html#configuration
	QueueDeliveryLimit = "x-delivery-limit"
)

const (
	// QuorumQueueType is the value used with the key "x-queue-type" in the optional
	// arguments when declaring a queue to declare a Quorum queue.
	QuorumQueueType = "quorum"
)

const (
	// MessageDeliveryCount in message's header represent how many times
	// the message was redelivered.
	MessageDeliveryCount = "x-delivery-count"
)
