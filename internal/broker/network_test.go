package broker

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// sets up isolated test broker with a temp dir and fixed config
func setupTestBroker(t *testing.T) (*Broker, string) {
	tempDir, err := os.MkdirTemp("", "hermitmq_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	os.Chdir(tempDir)

	cfg := testConfig(tempDir)
	b := NewBroker(cfg)

	b.CreateTopic("test_topic", 1)
	return b, tempDir
}

// tests full tcp flow between producer and consumer checking routing and zero copy
func TestTCP_ProducerConsumer(t *testing.T) {
	b, tempDir := setupTestBroker(t)
	defer os.RemoveAll(tempDir)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to bind: %v", err)
	}
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
	totalMessages := 50

	prodConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("Producer dial failed: %v", err)
	}
	defer prodConn.Close()

	prodConn.Write([]byte{ClientTypeProducer})
	prodConn.Write([]byte{10})
	prodConn.Write([]byte("test_topic"))
	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, 0)
	prodConn.Write(partBuf)

	for i := 0; i < totalMessages; i++ {

		payload := []byte("hello-world")
		buf := make([]byte, 29+len(payload))
		buf[0] = MagicV1
		binary.BigEndian.PutUint32(buf[21:25], uint32(len(payload)))
		binary.BigEndian.PutUint32(buf[25:29], 1)
		copy(buf[29:], payload)

		if _, err := prodConn.Write(buf); err != nil {
			t.Fatalf("Producer encode failed: %v", err)
		}

		ack := make([]byte, 8)

		if _, err := io.ReadFull(prodConn, ack); err != nil {
			t.Fatalf("Failed to read ACK: %v", err)
		}
	}

	consConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("Consumer dial failed: %v", err)
	}
	defer consConn.Close()

	consConn.Write([]byte{ClientTypeConsumer})
	consConn.Write([]byte{10})
	consConn.Write([]byte("test_group"))
	consConn.Write([]byte{10})
	consConn.Write([]byte("test_topic"))
	consConn.Write(partBuf)

	for i := 0; i < totalMessages; i++ {
		receivedMsg := &Message{}
		if err := receivedMsg.Decode(consConn, 50); err != nil {
			t.Fatalf("Consumer decode failed at msg %d: %v", i, err)
		}

		if string(receivedMsg.Payload) != "hello-world" {
			t.Errorf("Expected payload 'hello-world', got '%s'", string(receivedMsg.Payload))
		}
		if receivedMsg.Offset != uint64(i) {
			t.Errorf("Expected offset %d, got %d", i, receivedMsg.Offset)
		}
	}
}

// makes sure consumer offsets are saved correctly in memory for safe restarts
func TestTCP_ConsumerOffsets(t *testing.T) {
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

	commitConn, _ := net.Dial("tcp", serverAddr)
	commitConn.Write([]byte{ClientTypeCommit})
	commitConn.Write([]byte{10})
	commitConn.Write([]byte("test_group"))
	commitConn.Write([]byte{10})
	commitConn.Write([]byte("test_topic"))

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, 0)
	commitConn.Write(partBuf)

	offsetBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBuf, 42)
	commitConn.Write(offsetBuf)

	ack := make([]byte, 1)
	io.ReadFull(commitConn, ack)
	commitConn.Close()

	savedOffset := b.FetchOffset("test_group", "test_topic", 0)
	if savedOffset != 42 {
		t.Fatalf("Expected offset 42 in broker state, got %d", savedOffset)
	}
}

// floods partition with data to measure raw disk io speed
func Benchmark_StorageAppend(bench *testing.B) {
	b, tempDir := setupTestBroker(nil)
	defer os.RemoveAll(tempDir)

	part, _ := b.GetPartition("test_topic", 0)

	msg := &Message{
		Magic:       MagicV1,
		PayloadSize: 1024,
		Payload:     make([]byte, 1024),
	}

	bench.ResetTimer()
	bench.ReportAllocs()

	for i := 0; i < bench.N; i++ {
		_, err := part.Append(msg)
		if err != nil {
			bench.Fatalf("Append failed: %v", err)
		}
	}
}

// simulates many parallel producers to test network and lock overhead
func Benchmark_TCPProducer(bench *testing.B) {
	b, tempDir := setupTestBroker(nil)
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

	bench.ResetTimer()
	bench.ReportAllocs()

	bench.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			bench.Fatalf("Dial failed: %v", err)
		}
		defer conn.Close()

		conn.Write([]byte{ClientTypeProducer})
		conn.Write([]byte{10})
		conn.Write([]byte("test_topic"))
		conn.Write([]byte{0, 0, 0, 0})

		ack := make([]byte, 8)

		payload := make([]byte, 256)
		buf := make([]byte, 29+256)
		buf[0] = MagicV1

		binary.BigEndian.PutUint32(buf[17:21], 0)
		binary.BigEndian.PutUint32(buf[21:25], 256)
		binary.BigEndian.PutUint32(buf[25:29], 1)
		copy(buf[29:], payload)

		for pb.Next() {
			conn.Write(buf)
			io.ReadFull(conn, ack)
		}
	})
}

// checks lock contention and routing across many partitions under load
func Benchmark_MultiPartitionTCP(bench *testing.B) {
	b, tempDir := setupTestBroker(nil)
	defer os.RemoveAll(tempDir)

	partitionCount := 50
	b.topics = make(map[string]*Topic)
	b.CreateTopic("chat_messages", partitionCount)

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

	bench.ResetTimer()
	bench.ReportAllocs()

	bench.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			bench.Fatalf("Dial failed: %v", err)
		}
		defer conn.Close()

		localPort := conn.LocalAddr().(*net.TCPAddr).Port
		targetPartition := uint32(localPort % partitionCount)

		conn.Write([]byte{ClientTypeProducer})
		conn.Write([]byte{13})
		conn.Write([]byte("chat_messages"))

		partBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(partBuf, targetPartition)
		conn.Write(partBuf)

		msg := &Message{
			Magic:       MagicV1,
			PayloadSize: 256,
			Payload:     make([]byte, 256),
		}
		ack := make([]byte, 8)

		for pb.Next() {
			msg.Encode(conn, nil, 0)
			io.ReadFull(conn, ack)
		}
	})
}

// verifies that a blocked consumer wont freeze the whole broker pipeline
func TestSlowConsumer_NonBlocking(t *testing.T) {
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

	consConn, _ := net.Dial("tcp", serverAddr)
	defer consConn.Close()

	consConn.Write([]byte{ClientTypeConsumer})
	consConn.Write([]byte{10})
	consConn.Write([]byte("slow_group"))
	consConn.Write([]byte{10})
	consConn.Write([]byte("test_topic"))
	consConn.Write([]byte{0, 0, 0, 0})

	time.Sleep(50 * time.Millisecond)

	prodConn, _ := net.Dial("tcp", serverAddr)
	defer prodConn.Close()

	prodConn.Write([]byte{ClientTypeProducer})
	prodConn.Write([]byte{10})
	prodConn.Write([]byte("test_topic"))
	prodConn.Write([]byte{0, 0, 0, 0})

	for i := 0; i < 1000; i++ {
		payload := []byte("1234567890")
		buf := make([]byte, 29+len(payload))
		buf[0] = MagicV1
		binary.BigEndian.PutUint32(buf[21:25], uint32(len(payload)))
		binary.BigEndian.PutUint32(buf[25:29], 1)
		copy(buf[29:], payload)

		if _, err := prodConn.Write(buf); err != nil {
			t.Fatalf("Producer blocked or failed on msg %d: %v", i, err)
		}

		ack := make([]byte, 8)
		io.ReadFull(prodConn, ack)
	}
}

// forces segment rotation and checks if old logs get deleted from disk
func TestLogRetention_DiskCleanup(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "hermitmq_retention_*")
	defer os.RemoveAll(tempDir)

	cfg := testConfig(tempDir)

	part, err := NewPartition(tempDir, "retention_topic", cfg, nil)

	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	part.rotate(100)
	part.rotate(200)

	if len(part.segments) != 3 {
		t.Fatalf("Expected 3 segments, got %d", len(part.segments))
	}

	oldSegment := part.segments[0]
	eightDaysAgo := time.Now().Add(-8 * 24 * time.Hour)
	os.Chtimes(oldSegment.walFile.Name(), eightDaysAgo, eightDaysAgo)

	part.EnforceRetention()

	if len(part.segments) != 2 {
		t.Fatalf("Expected 2 segments to survive, got %d", len(part.segments))
	}

	if _, err := os.Stat(oldSegment.walFile.Name()); !os.IsNotExist(err) {
		t.Fatalf("Old WAL file was not physically deleted from disk")
	}
}

// returns baseline config to keep tests isolated from local environment
func testConfig(dataDir string) *Config {
	return &Config{
		Server:   ServerConfig{Host: "127.0.0.1", Port: 9092, HandshakeTimeoutSec: 5},
		Storage:  StorageConfig{DataDir: dataDir, MaxSegmentMB: 10, RetentionHours: 24, CleanupIntervalMin: 1},
		Security: SecurityConfig{MaxPayloadMB: 50},
	}
}

// tests if broker survives bad packets without crashing
func TestTCP_MalformedPackets(t *testing.T) {
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

	conn1, _ := net.Dial("tcp", serverAddr)
	conn1.Write([]byte{ClientTypeProducer})
	conn1.Write([]byte{255})
	conn1.Write([]byte("short"))
	conn1.Close()

	conn2, _ := net.Dial("tcp", serverAddr)
	conn2.Write([]byte{99})

	time.Sleep(100 * time.Millisecond)

	conn3, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("Broker crashed after malformed packets!")
	}
	conn3.Close()
}
