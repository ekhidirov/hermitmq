package main

import (
	"fmt"

	"github.com/ekhidirov/hermitmq/pkg/client"
)

func main() {
	consumer, err := client.NewConsumer("127.0.0.1:9092", "terminal-group", "chat_messages", 0)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	fmt.Println("Listening for messages...")
	for {
		msg, err := consumer.Receive()
		if err != nil {
			fmt.Println("Consumer closed:", err)
			break
		}
		fmt.Printf("[Offset: %d] %s\n", msg.Offset, string(msg.Payload))

		// Optional: Commit the offset after processing the message
		// consumer.Commit(msg.Offset)
	}
}
