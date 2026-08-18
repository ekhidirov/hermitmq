package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"syscall"
	"time"
)

const (
	ClientTypeProducer byte = 0x01
	ClientTypeConsumer byte = 0x02
	ClientTypeCommit   byte = 0x03
)

// handleconnection routes incoming tcp connections to the correct protocol handler
func HandleConnection(b *Broker, conn *net.TCPConn) {
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	clientType := make([]byte, 1)
	if _, err := io.ReadFull(conn, clientType); err != nil {
		return
	}

	conn.SetReadDeadline(time.Time{})

	switch clientType[0] {
	case ClientTypeProducer:
		handleProducer(b, conn)
	case ClientTypeConsumer:
		handleConsumer(b, conn)
	case ClientTypeCommit:
		handleCommit(b, conn)
	default:
		slog.Warn("unknown client type rejected", "client_type", clientType[0], "remote_addr", conn.RemoteAddr().String())
	}
}

// handleproducer manages the data stream for a specific partition
func handleProducer(b *Broker, conn *net.TCPConn) {
	topicName, err := readString(conn)
	if err != nil {
		return
	}

	b.mu.RLock()
	_, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		slog.Info("auto-creating new topic on demand", "role", "producer", "topic", topicName)
		b.CreateTopic(topicName, 1)
	}

	partIDBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, partIDBuf); err != nil {
		return
	}
	partitionID := int(binary.BigEndian.Uint32(partIDBuf))

	part, err := b.GetPartition(topicName, partitionID)
	if err != nil {
		slog.Error("producer routing error", "topic", topicName, "error", err)
		return
	}

	for {
		msg := &Message{}
		if err := msg.Decode(conn, b.cfg.Security.MaxPayloadMB); err != nil {

			if err == io.EOF || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
				slog.Debug("producer disconnected", "topic", topicName, "reason", err)
			} else {
				slog.Warn("producer connection closed or decode error", "topic", topicName, "error", err)
			}
			break
		}

		offset, err := part.Append(msg)
		if err != nil {
			slog.Error("disk append error", "topic", topicName, "error", err)

			errAck := make([]byte, 9)
			errAck[0] = 0xFF
			conn.Write(errAck)

			continue
		}

		ackBuf := make([]byte, 9)
		ackBuf[0] = 0x00
		binary.BigEndian.PutUint64(ackBuf[1:], offset)
		if _, err := conn.Write(ackBuf); err != nil {
			break
		}
	}
}

// handleconsumer gets messages for a specific consumer group
func handleConsumer(b *Broker, conn *net.TCPConn) {
	groupID, err := readString(conn)
	if err != nil {
		return
	}

	topicName, err := readString(conn)
	if err != nil {
		return
	}

	b.mu.RLock()
	_, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		slog.Info("auto-creating new topic on demand", "role", "consumer", "topic", topicName)
		b.CreateTopic(topicName, 1)
	}

	partIDBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, partIDBuf); err != nil {
		return
	}
	partitionID := int(binary.BigEndian.Uint32(partIDBuf))

	currentOffset := b.FetchOffset(groupID, topicName, partitionID)

	part, err := b.GetPartition(topicName, partitionID)
	if err != nil {
		slog.Error("consumer routing error", "topic", topicName, "error", err)
		return
	}

	for {
		part.WaitForOffset(currentOffset)

		nextOffset, err := part.SendZeroCopy(conn, currentOffset)
		if err != nil {
			oldest := part.GetOldestOffset()
			if currentOffset < oldest {
				slog.Warn("fast-forwarding consumer due to retention policy",
					"group", groupID,
					"dead_offset", currentOffset,
					"new_offset", oldest)
				currentOffset = oldest
				continue
			}

			if err == io.EOF || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
				slog.Debug("consumer disconnected", "group", groupID, "topic", topicName, "reason", err)
			} else {
				slog.Warn("consumer connection error", "group", groupID, "topic", topicName, "error", err)
			}
			break
		}

		currentOffset = nextOffset
	}
}

// handlecommit saves the consumer offset to the internal state
func handleCommit(b *Broker, conn *net.TCPConn) {
	groupID, err := readString(conn)
	if err != nil {
		return
	}

	topicName, err := readString(conn)
	if err != nil {
		return
	}

	b.mu.RLock()
	_, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		slog.Info("auto-creating new topic on demand", "role", "commit", "topic", topicName)
		b.CreateTopic(topicName, 1)
	}

	partIDBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, partIDBuf); err != nil {
		return
	}
	partitionID := int(binary.BigEndian.Uint32(partIDBuf))

	offsetBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, offsetBuf); err != nil {
		return
	}
	offset := binary.BigEndian.Uint64(offsetBuf)

	err = b.CommitOffset(groupID, topicName, partitionID, offset)
	if err != nil {
		slog.Error("failed to commit offset", "group", groupID, "topic", topicName, "error", err)
		return
	}

	conn.Write([]byte{1})
}

// sendzerocopy streams data directly from wal to the network socket
func (p *Partition) SendZeroCopy(conn *net.TCPConn, offset uint64) (uint64, error) {
	walFile, position, err := p.LookupPosition(offset)
	if err != nil {
		return 0, err
	}

	header := make([]byte, HeaderSize)
	_, err = walFile.ReadAt(header, position)
	if err != nil {
		return 0, err
	}

	keySize := binary.BigEndian.Uint32(header[17:21])
	payloadSize := binary.BigEndian.Uint32(header[21:25])

	recordCount := binary.BigEndian.Uint32(header[25:29])
	if recordCount == 0 {
		recordCount = 1
	}

	baseOffset := binary.BigEndian.Uint64(header[9:17])
	nextOffset := baseOffset + uint64(recordCount)

	bytesToSend := int64(HeaderSize) + int64(keySize) + int64(payloadSize)
	reader := io.NewSectionReader(walFile, position, bytesToSend)
	_, err = io.CopyN(conn, reader, bytesToSend)

	return nextOffset, err
}

// readstring decodes a string with length prefix from the buffer
func readString(conn net.Conn) (string, error) {
	lenBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return "", err
	}

	strLen := int(lenBuf[0])
	if strLen == 0 {
		return "", fmt.Errorf("empty string received")
	}

	strBuf := make([]byte, strLen)
	if _, err := io.ReadFull(conn, strBuf); err != nil {
		return "", err
	}

	return string(strBuf), nil
}
