package client

// struct message is a convenient way to represent what we received from the broker
type Message struct {
	Offset  uint64
	Payload []byte
}
