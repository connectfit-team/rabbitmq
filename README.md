![RabbitMQ Client](https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/RabbitMQ_logo.svg/2560px-RabbitMQ_logo.svg.png)

RabbitMQ Client provide a simple yet robust abstraction around [the most widely used Go AMQP 0.9.1 client](https://github.com/rabbitmq/amqp091-go). This package has been designed to ease the interactions with the RabbitMQ server and let the developer focus on what really matter.

</br >

# ⚙️ Installation

`go get bitbucket.org/connectfit/rabbitmq`

</br >

# ⚡️ Quickstart

```Go
package main

import (
    "log"
    
    "bitbucket.org/connectfit/rabbitmq"
)

func main() {    
    ctx := context.Background()

	logger := log.New(os.Stderr, "RabbitMQ Client: ",log.LstdFlags)

    client := rabbitmq.NewClient(logger)
	err := rabbitClient.Connect(connectCtx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

    msgs, err := client.Consume(ctx)
    if err != nil {
        panic(err)
    }
    for msg := range msgs {
        // Handle your delivery
    }
}
```

</br>

# 📖 Features

* Automatic connection recovery(including channel and consumers recovery)
* Context handling(gracefully shutdown on context cancellation)

</br>

# 📚 Options

The client originally use a default configuration to connect to a RabbitMQ instance locally but it is actually highly configurable through functional options:

```Go
rabbitClient := rabbitmq.NewClient(
		logger,
		rabbitmq.WithUsername("username"),
		rabbitmq.WithPassword("password"),
		rabbitmq.WithHost("host"),
		rabbitmq.WithPort("port"),
		rabbitmq.WithQueueName("queue-name"),
		rabbitmq.WithConsumerName("consumer-name"),
		rabbitmq.WithChannelInitializationRetryDelay(time.Second * 5),
		rabbitmq.WithConnectionRetryDelay(time.Second * 5),
		rabbitmq.WithConnectionTimeout(time.Minute * 1),
	)
```