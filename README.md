![RabbitMQ Client](https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/RabbitMQ_logo.svg/2560px-RabbitMQ_logo.svg.png)

</br>

### RabbitMQ Client provide a simple yet robust abstraction around [the most widely used Go AMQP 0.9.1 client](https://github.com/rabbitmq/amqp091-go). This package has been designed to ease the interactions with the RabbitMQ server and let the developer focus on what really matter.

# ⚙️ Installation

</br>

`go get bitbucket.org/connectfit/rabbitmq`

# ⚡️ Quickstart

</br>

### 📖  Publisher

```Go
package main

import (
	"context"
	"log"
	"os"

	"bitbucket.org/connectfit/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx := context.Background()

	logger := log.New(os.Stdout, "RabbitMQ Client :", log.LstdFlags)

	c := rabbitmq.NewClient(
		logger,
	)
	err := c.Connect(ctx)
	if err != nil {
		panic(err)
	}

	msg := amqp091.Publishing{
		Body: []byte("Created user foo"),
	}
	err = c.Publish(ctx, msg, "user.created")
	if err != nil {
		panic(err)
	}
}
```


### 📖  Consumer

```Go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"bitbucket.org/connectfit/rabbitmq"
)

func main() {
	ctx := context.Background()

	logger := log.New(os.Stdout, "RabbitMQ client: ", 0)

	c := rabbitmq.NewClient(logger)
	err := c.Connect(ctx)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	queue, err := c.QueueDeclare("user.created")
	if err != nil {
		panic(err)
	}

	msgs, err := c.Consume(ctx, queue.Name)
	if err != nil {
		panic(err)
	}
	for msg := range msgs {
		// Handle the messages
		fmt.Printf("Event: %s\n", string(msg.Body))

		// Acknowledge the message to the server
		msg.Ack(false)
	}
}
```

# 🪄 Features

</br>

* Automatic connection recovery(including channel and consumers recovery)
* Context handling(gracefully shutdown on context cancellation)

# 📚 Documentation

</br>

For further information you can generates documentation for the project through the [`godoc`](https://pkg.go.dev/golang.org/x/tools/cmd/godoc?utm_source=godoc) command:

```godoc -http=:[port]```

And then browse the documentation at [`http://localhost:[port]/pkg/bitbucket.org/connectfit/rabbitmq/`](http://localhost:6060/pkg/bitbucket.org/connectfit/rabbitmq/)

# 👀 Examples

</br>

### 📖 Publish a delayed message (using the RabbitMQ delayed message exchange plugin)

```Go
package main

import (
	"context"
	"log"
	"os"
	"time"

	"bitbucket.org/connectfit/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx := context.Background()

	logger := log.New(os.Stdout, "RabbitMQ Client :", log.LstdFlags)

	c := rabbitmq.NewClient(
		logger,
	)
	err := c.Connect(ctx)
	if err != nil {
		panic(err)
	}

	err = c.ExchangeDeclare(
		"user",
		rabbitmq.WithDelayedMessageExchangeType(rabbitmq.DirectExchangeType),
	)
	if err != nil {
		panic(err)
	}

	msg := amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte("Created user foo"),
	}
	err = c.Publish(
		ctx,
		msg,
		"user.created",
		rabbitmq.WithPublishExchangeName("user"),
		rabbitmq.WithMessageDelay(time.Second*5),
	)
	if err != nil {
		panic(err)
	}
}
```

# 📝 To Do List