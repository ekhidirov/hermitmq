package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// main function connects to the broker to load test the delivery rate of the pipeline[cite: 10]
func main() {
	fmt.Println("Connecting to Broker (Spam Consumer)...")

	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	conn.Write([]byte{0x02})

	groupName := "stress-group"
	conn.Write([]byte{byte(len(groupName))})
	conn.Write([]byte(groupName))

	topicName := "load_test"
	conn.Write([]byte{byte(len(topicName))})
	conn.Write([]byte(topicName))

	conn.Write([]byte{0, 0, 0, 0})

	fmt.Println("Connected! Waiting for 1,000,000 messages (via Virtual Offsets)...")

	var start time.Time
	logicalCount := 0
	physicalCount := 0

	header := make([]byte, 29)

	payloadBuf := make([]byte, 1024)

	for logicalCount < 1000000 {
		if _, err := io.ReadFull(conn, header); err != nil {
			panic(fmt.Errorf("Network error at physical msg %d: %v", physicalCount, err))
		}

		if physicalCount == 0 {
			start = time.Now()
		}

		keySize := binary.BigEndian.Uint32(header[17:21])
		payloadSize := binary.BigEndian.Uint32(header[21:25])

		recordCount := binary.BigEndian.Uint32(header[25:29])
		if recordCount == 0 {
			recordCount = 1
		}

		if keySize > 0 {
			io.CopyN(io.Discard, conn, int64(keySize))
		}

		if payloadSize > uint32(len(payloadBuf)) {
			payloadBuf = make([]byte, payloadSize)
		}
		io.ReadFull(conn, payloadBuf[:payloadSize])

		physicalCount++

		logicalCount += int(recordCount)

		if logicalCount%100000 == 0 || logicalCount >= 1000000 {
			fmt.Printf("Received %d JSON messages... (in %d physical batches)\n", logicalCount, physicalCount)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\n✔ SUCCESS! Received %d messages in %v\n", logicalCount, elapsed)

	totalMB := float64(logicalCount*1812) / 1024 / 1024
	fmt.Printf("✔ Throughput: %.0f JSONs/sec (Approx %.0f MB/sec)\n", float64(logicalCount)/elapsed.Seconds(), totalMB/elapsed.Seconds())
}
