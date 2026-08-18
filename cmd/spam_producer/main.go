package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	ProducersCount    = 10
	MessagesPerWorker = 100000
	BatchSize         = 100
	LingerTime        = 100 * time.Millisecond
)

const jsonPayload = `{"message": "Hello World!"}`

// spawnproducer manages a single worker lifecycle decoupling message generation from network transmission for maximum non blocking throughput
func spawnProducer(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		fmt.Printf("Producer %d failed to connect: %v\n", id, err)
		return
	}
	defer conn.Close()

	conn.Write([]byte{0x01})
	topic := "load_test"
	conn.Write([]byte{byte(len(topic))})
	conn.Write([]byte(topic))
	conn.Write([]byte{0, 0, 0, 0})

	msgCh := make(chan []byte, 10000)
	go func() {
		payloadBytes := []byte(jsonPayload)
		for i := 0; i < MessagesPerWorker; i++ {
			msgCh <- payloadBytes
		}
		close(msgCh)
	}()

	ticker := time.NewTicker(LingerTime)
	defer ticker.Stop()

	var batchBuffer bytes.Buffer
	var recordCount uint32 = 0
	var totalSent int = 0

	ack := make([]byte, 9)

	flush := func() {
		if recordCount == 0 {
			return
		}

		payloadBytes := batchBuffer.Bytes()
		payloadSize := uint32(len(payloadBytes))

		totalSize := 29 + int(payloadSize)

		buf := make([]byte, totalSize)
		buf[0] = 0x01
		binary.BigEndian.PutUint64(buf[1:9], uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint32(buf[17:21], 0)
		binary.BigEndian.PutUint32(buf[21:25], payloadSize)
		binary.BigEndian.PutUint32(buf[25:29], recordCount)
		copy(buf[29:], payloadBytes)

		for {
			if _, err := conn.Write(buf); err != nil {
				fmt.Printf("Producer %d failed to write (network down): %v\n", id, err)
				return
			}

			if _, err := io.ReadFull(conn, ack); err != nil {
				fmt.Printf("Producer %d failed to read ACK: %v\n", id, err)
				return
			}

			status := ack[0]
			if status == 0xFF {
				fmt.Printf("Producer %d: Broker capacity reached (Disk Full/Backpressure). Retrying in 3s...\n", id)
				time.Sleep(3 * time.Second)
				continue
			} else if status == 0x00 {
				break
			} else {
				fmt.Printf("Producer %d got unknown status: %d\n", id, status)
				return
			}
		}

		totalSent += int(recordCount)
		batchBuffer.Reset()
		recordCount = 0
		ticker.Reset(LingerTime)
	}

	for totalSent < MessagesPerWorker {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				flush()
				return
			}

			batchBuffer.Write(msg)
			batchBuffer.WriteString("\n")
			recordCount++

			if recordCount >= BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}

	fmt.Printf("✔ Producer %d finished sending %d msgs.\n", id, totalSent)
}

// main sets up and starts the smart batch stress test using channels and tickers
func main() {
	singleJsonLen := len([]byte(jsonPayload))
	batchPayloadLen := (singleJsonLen + 1) * BatchSize
	totalMessages := ProducersCount * MessagesPerWorker

	totalMB := float64(ProducersCount*(MessagesPerWorker/BatchSize)*(29+batchPayloadLen)) / 1024 / 1024

	fmt.Printf("Starting SMART BATCH Stress Test (Channels + Ticker):\n")
	fmt.Printf("Producers: %d\n", ProducersCount)
	fmt.Printf("Batch Limits: %d msgs OR %v\n", BatchSize, LingerTime)
	fmt.Printf("Messages Total: %d\n", totalMessages)
	fmt.Printf("Expected Data Volume: %.2f MB\n\n", totalMB)

	start := time.Now()
	var wg sync.WaitGroup

	for i := 1; i <= ProducersCount; i++ {
		wg.Add(1)
		go spawnProducer(i, &wg)
	}

	wg.Wait()
	fmt.Printf("\n✔ All Producers finished in %v\n", time.Since(start))
}
