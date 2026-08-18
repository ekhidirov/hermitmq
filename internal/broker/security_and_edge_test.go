package broker

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// tests if the broker correctly rejects invalid magic bytes and oversized payloads to protect against out of memory attacks
func TestProtocol_SecurityAndOOMProtection(t *testing.T) {
	b, tempDir := setupTestBroker(t)
	defer os.RemoveAll(tempDir)

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go HandleConnection(b, conn.(*net.TCPConn))
		}
	}()
	serverAddr := listener.Addr().String()

	t.Run("Reject Invalid Magic Byte", func(t *testing.T) {
		conn, _ := net.Dial("tcp", serverAddr)
		defer conn.Close()

		conn.Write([]byte{ClientTypeProducer})
		conn.Write([]byte{10})
		conn.Write([]byte("test_topic"))
		conn.Write([]byte{0, 0, 0, 0})

		badHeader := make([]byte, 29)
		badHeader[0] = 0x02
		conn.Write(badHeader)

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err := conn.Read(make([]byte, 1))
		if err == nil {
			t.Fatal("Broker should have disconnected the client for bad magic byte")
		}
	})

	t.Run("Reject Oversized Payload (OOM Vector)", func(t *testing.T) {
		conn, _ := net.Dial("tcp", serverAddr)
		defer conn.Close()

		conn.Write([]byte{ClientTypeProducer})
		conn.Write([]byte{10})
		conn.Write([]byte("test_topic"))
		conn.Write([]byte{0, 0, 0, 0})

		oomHeader := make([]byte, 29)
		oomHeader[0] = MagicV1
		binary.BigEndian.PutUint32(oomHeader[21:25], 1024*1024*1024)
		conn.Write(oomHeader)

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err := conn.Read(make([]byte, 1))
		if err == nil {
			t.Fatal("Broker should have disconnected the client for exceeding MaxPayloadMB")
		}
	})
}

// checks if the broker can handle many concurrent offset commits and fetches without race conditions
func TestBroker_LockStripingConcurrency(t *testing.T) {
	cfg := testConfig(t.TempDir())
	b := NewBroker(cfg)
	defer b.Close()

	var wg sync.WaitGroup
	goroutines := 1000

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			group := fmt.Sprintf("group_%d", id%10)
			topic := "chat_messages"
			partition := id % 5

			for j := 0; j < 100; j++ {
				offset := uint64(j)
				_ = b.CommitOffset(group, topic, partition, offset)
				_ = b.FetchOffset(group, topic, partition)
			}
		}(i)
	}

	wg.Wait()
}

// verifies that the storage engine can recover from incomplete writes by truncating corrupted data
func TestStorage_TornPageRecovery(t *testing.T) {
	tempDir := t.TempDir()
	cfg := testConfig(tempDir)

	walPath := filepath.Join(tempDir, "00000000000000000000.wal")
	idxPath := filepath.Join(tempDir, "00000000000000000000.idx")

	validMsg := []byte("perfect-message")
	msgTotalSize := 29 + len(validMsg)

	f, _ := os.Create(walPath)

	buf := make([]byte, msgTotalSize)
	buf[0] = MagicV1
	binary.BigEndian.PutUint64(buf[9:17], 0)
	binary.BigEndian.PutUint32(buf[21:25], uint32(len(validMsg)))
	binary.BigEndian.PutUint32(buf[25:29], 1)
	copy(buf[29:], validMsg)
	f.Write(buf)

	f.Write(buf[:15])
	f.Close()

	os.WriteFile(idxPath, []byte{}, 0644)

	part, err := NewPartition(tempDir, "torn_topic", cfg, nil)
	if err != nil {
		t.Fatalf("Failed to load partition with torn page: %v", err)
	}

	if part.active.nextOffset != 1 {
		t.Errorf("Expected next offset to be 1, got %d", part.active.nextOffset)
	}

	stat, _ := os.Stat(walPath)
	if stat.Size() != int64(msgTotalSize) {
		t.Errorf("Expected WAL file size to be truncated to %d, got %d", msgTotalSize, stat.Size())
	}
}
