package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

type Producer struct {
	conn      net.Conn
	topic     string
	partition uint32
}

// function newproducer connects to the broker and performs a handshake as a producer
func NewProducer(addr, topic string, partition uint32) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	conn.Write([]byte{0x01})

	conn.Write([]byte{byte(len(topic))})
	conn.Write([]byte(topic))

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, partition)
	conn.Write(partBuf)

	return &Producer{
		conn:      conn,
		topic:     topic,
		partition: partition,
	}, nil
}

// function send packs the payload into our custom twenty nine byte protocol and waits for broker acknowledgment
func (p *Producer) Send(payload []byte) (uint64, error) {
	buf := make([]byte, 29+len(payload))
	buf[0] = 0x01

	binary.BigEndian.PutUint64(buf[1:9], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(buf[9:17], 0) // offset that the broker will fill in later
	binary.BigEndian.PutUint32(buf[17:21], 0)
	binary.BigEndian.PutUint32(buf[21:25], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[25:29], 1)

	copy(buf[29:], payload)

	if _, err := p.conn.Write(buf); err != nil {
		return 0, fmt.Errorf("write error: %w", err)
	}

	ack := make([]byte, 9)
	if _, err := io.ReadFull(p.conn, ack); err != nil {
		return 0, fmt.Errorf("ack read error: %w", err)
	}

	if ack[0] != 0x00 {
		return 0, fmt.Errorf("broker rejected the message, status code: %d", ack[0])
	}

	offset := binary.BigEndian.Uint64(ack[1:9])
	return offset, nil
}

// function close shuts down the current network connection
func (p *Producer) Close() error {
	return p.conn.Close()
}
