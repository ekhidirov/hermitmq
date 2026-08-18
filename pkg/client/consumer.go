package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Consumer struct {
	conn      net.Conn
	addr      string
	group     string
	topic     string
	partition uint32
}

// function newconsumer connects to the broker and performs a handshake as a consumer
func NewConsumer(addr, group, topic string, partition uint32) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	conn.Write([]byte{0x02})

	conn.Write([]byte{byte(len(group))})
	conn.Write([]byte(group))

	conn.Write([]byte{byte(len(topic))})
	conn.Write([]byte(topic))

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, partition)
	conn.Write(partBuf)

	return &Consumer{
		conn:      conn,
		addr:      addr,
		group:     group,
		topic:     topic,
		partition: partition,
	}, nil
}

// function receive blocks execution until it gets a new message from the broker
func (c *Consumer) Receive() (*Message, error) {
	header := make([]byte, 29)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, fmt.Errorf("header read error: %w", err)
	}

	offset := binary.BigEndian.Uint64(header[9:17])
	keySize := binary.BigEndian.Uint32(header[17:21])
	payloadSize := binary.BigEndian.Uint32(header[21:25])

	if keySize > 0 {
		io.CopyN(io.Discard, c.conn, int64(keySize))
	}

	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, fmt.Errorf("payload read error: %w", err)
	}

	return &Message{
		Offset:  offset,
		Payload: payload,
	}, nil
}

// function commit tells the broker to save the offset for the current consumer group
func (c *Consumer) Commit(offset uint64) error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("commit dial error: %w", err)
	}
	defer conn.Close()

	conn.Write([]byte{0x03})

	conn.Write([]byte{byte(len(c.group))})
	conn.Write([]byte(c.group))

	conn.Write([]byte{byte(len(c.topic))})
	conn.Write([]byte(c.topic))

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, c.partition)
	conn.Write(partBuf)

	offsetBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBuf, offset)
	conn.Write(offsetBuf)

	ack := make([]byte, 1)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return fmt.Errorf("commit ack read error: %w", err)
	}

	return nil
}

// function close shuts down the current network connection
func (c *Consumer) Close() error {
	return c.conn.Close()
}
