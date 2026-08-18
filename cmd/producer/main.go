package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// main function connects to the broker as a producer and sends chat messages from standard input
func main() {
	fmt.Println("Connecting to Broker (Producer)...")

	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	conn.Write([]byte{0x01})
	conn.Write([]byte{13})
	conn.Write([]byte("chat_messages"))
	conn.Write([]byte{0, 0, 0, 0})

	fmt.Println("Connected! Type your messages and press Enter:")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Bytes()
		if len(text) == 0 {
			continue
		}

		buf := make([]byte, 29+len(text))
		buf[0] = 0x01
		binary.BigEndian.PutUint64(buf[1:9], uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint64(buf[9:17], 0)
		binary.BigEndian.PutUint32(buf[17:21], 0)
		binary.BigEndian.PutUint32(buf[21:25], uint32(len(text)))
		binary.BigEndian.PutUint32(buf[25:29], 1)

		copy(buf[29:], text)

		if _, err := conn.Write(buf); err != nil {
			fmt.Println("Failed to send:", err)
			break
		}

		ack := make([]byte, 9)
		if _, err := io.ReadFull(conn, ack); err != nil {
			fmt.Println("Failed to read ACK:", err)
			break
		}

		if ack[0] != 0x00 {
			fmt.Println("Broker rejected the message (partition full or internal error).")
			continue
		}

		offset := binary.BigEndian.Uint64(ack[1:9])
		fmt.Printf("✔ Saved to disk with Offset: %d\n", offset)
	}
}
