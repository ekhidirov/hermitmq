package broker

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// topic is a logical group of data stream partitions
// splitting topics helps broker ingest messages in parallel and allows consumer groups to scale
type Topic struct {
	Name       string
	Partitions map[int]*Partition
	mu         sync.RWMutex
}

// broker is the main coordinator for all topics
// it manages global state of the messaging cluster and gives thread safe access
type Broker struct {
	cfg          *Config
	topics       map[string]*Topic
	mu           sync.RWMutex
	bufferPool   *sync.Pool
	offsetShards [OffsetShardCount]*OffsetShard
	groupOffsets map[string]uint64
	offsetMu     sync.RWMutex
}

// newbroker sets up the core broker state and starts the startup sequence
// it discovers disk structures and restores consumer progress for seamless continuity after restart
func NewBroker(cfg *Config) *Broker {
	poolSize := cfg.Storage.BufferPoolSizeKB * 1024

	b := &Broker{
		cfg:    cfg,
		topics: make(map[string]*Topic),
		bufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, poolSize)
				return &b
			},
		},
	}

	for i := 0; i < OffsetShardCount; i++ {
		b.offsetShards[i] = &OffsetShard{
			offsets: make(map[string]uint64),
		}
	}

	b.loadTopicsFromDisk(cfg.Storage.DataDir)
	b.CreateTopic("__consumer_offsets", 1)
	b.loadOffsetsFromDisk()

	return b
}

// createtopic makes logical structure and physical disk folders for a new topic across partitions
func (b *Broker) CreateTopic(name string, partitionCount int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic '%s' already exists", name)
	}

	t := &Topic{
		Name:       name,
		Partitions: make(map[int]*Partition),
	}

	for id := 0; id < partitionCount; id++ {
		dirPath := fmt.Sprintf("%s/%s_%d", b.cfg.Storage.DataDir, name, id)

		p, err := NewPartition(dirPath, name, b.cfg, b.bufferPool)
		if err != nil {
			return fmt.Errorf("failed to create partition %d for topic '%s': %w", id, name, err)
		}

		t.Partitions[id] = p
	}

	b.topics[name] = t
	return nil
}

// getpartition maps a logical partition id to physical memory
// uses fine grained read locks for high throughput during concurrent client routing
func (b *Broker) GetPartition(topicName string, partitionID int) (*Partition, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic '%s' not found", topicName)
	}

	t.mu.RLock()
	p, exists := t.Partitions[partitionID]
	t.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("partition %d not found in topic '%s'", partitionID, topicName)
	}

	return p, nil
}

const OffsetShardCount = 256

type OffsetShard struct {
	mu      sync.RWMutex
	offsets map[string]uint64
}

func getShardIndex(key string) uint8 {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return uint8(hash & (OffsetShardCount - 1))
}

// makeoffsetkey builds a deterministic string key for fast memory lookups and durable wal indexing
func makeOffsetKey(group, topic string, partition int) string {
	return fmt.Sprintf("%s:%s:%d", group, topic, partition)
}

// commitoffset saves a consumer group read boundary to the internal write ahead log
// this gives at least once delivery by preventing replay after a crash
func (b *Broker) CommitOffset(group, topic string, partition int, offset uint64) error {
	keyStr := makeOffsetKey(group, topic, partition)
	keyBytes := []byte(keyStr)

	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, offset)

	msg := &Message{
		Magic:       MagicV1,
		KeySize:     uint32(len(keyBytes)),
		PayloadSize: 8,
		Key:         keyBytes,
		Payload:     payload,
	}

	internalPart, err := b.GetPartition("__consumer_offsets", 0)
	if err != nil {
		return err
	}

	_, err = internalPart.Append(msg)
	if err != nil {
		return fmt.Errorf("failed to write to __consumer_offsets: %v", err)
	}

	shardIdx := getShardIndex(keyStr)
	shard := b.offsetShards[shardIdx]

	shard.mu.Lock()
	shard.offsets[keyStr] = offset
	shard.mu.Unlock()

	return nil
}

// fetchoffset gets the latest read position for a consumer group
// defaults to zero if the group is new so it reads from the start
func (b *Broker) FetchOffset(group, topic string, partition int) uint64 {
	keyStr := makeOffsetKey(group, topic, partition)

	shardIdx := getShardIndex(keyStr)
	shard := b.offsetShards[shardIdx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return shard.offsets[keyStr]
}

// startbackgroundtasks kicks off async storage maintenance like log compaction and retention for disk stability
func (b *Broker) StartBackgroundTasks() {

	go func() {
		interval := time.Duration(b.cfg.Storage.FlushIntervalMs) * time.Millisecond
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {

			b.mu.RLock()
			var activeSegments []*os.File
			for _, t := range b.topics {
				t.mu.RLock()
				for _, p := range t.Partitions {
					p.mu.RLock()
					if p.active != nil && p.active.walFile != nil {
						activeSegments = append(activeSegments, p.active.walFile)
					}
					p.mu.RUnlock()
				}
				t.mu.RUnlock()
			}
			b.mu.RUnlock()

			for _, file := range activeSegments {
				file.Sync()
			}
		}
	}()

	go func() {
		interval := time.Duration(b.cfg.Storage.CleanupIntervalMin) * time.Minute
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			b.mu.RLock()
			topicsCopy := make([]*Topic, 0, len(b.topics))
			for _, t := range b.topics {
				topicsCopy = append(topicsCopy, t)
			}
			b.mu.RUnlock()

			for _, topic := range topicsCopy {
				topic.mu.RLock()
				partsCopy := make([]*Partition, 0, len(topic.Partitions))
				for _, p := range topic.Partitions {
					partsCopy = append(partsCopy, p)
				}
				topic.mu.RUnlock()

				for _, part := range partsCopy {
					if topic.Name == "__consumer_offsets" {
						part.CompactLogs()
					} else {
						part.EnforceRetention()
					}
				}
			}
		}
	}()
}

// loadoffsetsfromdisk replays the internal offset log at startup
// projects history into hot cache to rebuild the latest consumer group state
func (b *Broker) loadOffsetsFromDisk() {
	part, err := b.GetPartition("__consumer_offsets", 0)
	if err != nil {
		return
	}

	part.mu.RLock()
	defer part.mu.RUnlock()

	var count int

	for _, seg := range part.segments {
		fileInfo, _ := seg.walFile.Stat()
		fileSize := fileInfo.Size()

		var pos int64 = 0
		for pos < fileSize {
			header := make([]byte, HeaderSize)
			_, err := seg.walFile.ReadAt(header, pos)
			if err != nil {
				break
			}

			keySize := binary.BigEndian.Uint32(header[17:21])
			payloadSize := binary.BigEndian.Uint32(header[21:25])

			keyBuf := make([]byte, keySize)
			seg.walFile.ReadAt(keyBuf, pos+int64(HeaderSize))
			keyStr := string(keyBuf)

			payloadBuf := make([]byte, payloadSize)
			seg.walFile.ReadAt(payloadBuf, pos+int64(HeaderSize)+int64(keySize))
			savedOffset := binary.BigEndian.Uint64(payloadBuf)

			shardIdx := getShardIndex(keyStr)
			shard := b.offsetShards[shardIdx]

			shard.mu.Lock()
			shard.offsets[keyStr] = savedOffset
			shard.mu.Unlock()

			count++
			pos += int64(HeaderSize) + int64(keySize) + int64(payloadSize)
		}
	}
	slog.Info("successfully loaded consumer offsets from disk", "count", count)
}

// loadtopicsfromdisk scans the data dir and mounts existing partition files for zero config restore
func (b *Broker) loadTopicsFromDisk(dataDir string) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return
	}

	topicPartitions := make(map[string]int)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()

		lastUnderscore := strings.LastIndex(name, "_")
		if lastUnderscore == -1 {
			continue
		}

		topicName := name[:lastUnderscore]
		topicPartitions[topicName]++
	}

	for topicName, count := range topicPartitions {
		b.CreateTopic(topicName, count)
	}

	if len(topicPartitions) > 0 {
		slog.Info("auto-discovery complete", "topics_loaded", len(topicPartitions))
	}
}

// close stops all partitions in the topic safely
func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, p := range t.Partitions {
		p.Close()
	}
}

// close stops the broker and makes sure all data streams flush to disk
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, t := range b.topics {
		t.Close()
	}
	return nil
}
