package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

// segment maps a physical wal file and its index
type Segment struct {
	BaseOffset  uint64
	walFile     *os.File
	indexFile   *os.File
	indexReader *mmap.ReaderAt

	nextOffset  uint64
	walPosition int64
	isClosed    bool

	memOffsets   []uint64
	memPositions []int64
}

// partition manages a directory of log segments and handles concurrent access rotation and data retention
type Partition struct {
	cfg          *Config
	mu           sync.RWMutex
	dir          string
	name         string
	cond         *sync.Cond
	isCompacting bool

	segments []*Segment
	active   *Segment
	pool     *sync.Pool
}

// newpartition sets up a partition by scanning the folder and loading segments from disk for fast startup
func NewPartition(dir string, name string, cfg *Config, pool *sync.Pool) (*Partition, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	p := &Partition{
		cfg:  cfg,
		dir:  dir,
		name: name,
		pool: pool,
	}

	p.cond = sync.NewCond(p.mu.RLocker())

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var baseOffsets []uint64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".wal" {
			continue
		}
		baseName := strings.TrimSuffix(entry.Name(), ".wal")
		offset, err := strconv.ParseUint(baseName, 10, 64)
		if err == nil {
			baseOffsets = append(baseOffsets, offset)
		}
	}

	if len(baseOffsets) == 0 {
		if err := p.rotate(0); err != nil {
			return nil, err
		}
		return p, nil
	}

	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	for i, baseOffset := range baseOffsets {
		baseName := fmt.Sprintf("%020d", baseOffset)
		walPath := filepath.Join(dir, baseName+".wal")
		idxPath := filepath.Join(dir, baseName+".idx")

		var nextOffset uint64
		var validWalPosition int64
		var mOffsets []uint64
		var mPositions []int64
		var reader *mmap.ReaderAt
		isClosed := false

		if i < len(baseOffsets)-1 {
			isClosed = true

			nextOffset = baseOffsets[i+1]

			walStat, _ := os.Stat(walPath)
			validWalPosition = walStat.Size()

			reader, _ = mmap.Open(idxPath)
			mOffsets = nil
			mPositions = nil
		} else {

			var err error
			nextOffset, validWalPosition, mOffsets, mPositions, err = recoverSegment(walPath, idxPath, baseOffset)
			if err != nil {
				return nil, fmt.Errorf("failed to recover active segment %s: %v", walPath, err)
			}
		}

		wal, _ := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		idx, _ := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)

		seg := &Segment{
			BaseOffset:   baseOffset,
			walFile:      wal,
			indexFile:    idx,
			indexReader:  reader,
			nextOffset:   nextOffset,
			walPosition:  validWalPosition,
			isClosed:     isClosed,
			memOffsets:   mOffsets,
			memPositions: mPositions,
		}

		if !isClosed {
			p.active = seg
		}
		p.segments = append(p.segments, seg)
	}

	return p, nil
}

// rotate seals the active segment and creates a new wal and index pair to limit file sizes and help with garbage collection
func (p *Partition) rotate(baseOffset uint64) error {
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		return err
	}

	baseName := fmt.Sprintf("%020d", baseOffset)
	walPath := filepath.Join(p.dir, baseName+".wal")
	idxPath := filepath.Join(p.dir, baseName+".idx")

	wal, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	idx, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	seg := &Segment{
		BaseOffset:   baseOffset,
		walFile:      wal,
		indexFile:    idx,
		indexReader:  nil,
		nextOffset:   baseOffset,
		walPosition:  0,
		memOffsets:   make([]uint64, 0),
		memPositions: make([]int64, 0),
	}

	if p.active != nil {
		p.active.isClosed = true
		oldIdxPath := filepath.Join(p.dir, fmt.Sprintf("%020d.idx", p.active.BaseOffset))
		p.active.indexReader, _ = mmap.Open(oldIdxPath)
		p.active.memOffsets = nil
		p.active.memPositions = nil
	}

	p.segments = append(p.segments, seg)
	p.active = seg

	return nil
}

// append writes a message to the active wal syncs the index and wakes up consumers to keep latency low
func (p *Partition) Append(msg *Message) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	maxBytes := int64(p.cfg.Storage.MaxSegmentMB) * 1024 * 1024
	if p.active.walPosition >= maxBytes {
		if err := p.rotate(p.active.nextOffset); err != nil {
			return 0, err
		}
	}

	seg := p.active
	msg.Offset = seg.nextOffset

	maxReturnBytes := p.cfg.Storage.MaxBufferReturnKB * 1024

	if err := msg.Encode(seg.walFile, p.pool, maxReturnBytes); err != nil {
		return 0, err
	}

	idxBuf := make([]byte, IndexRecordSize)
	binary.BigEndian.PutUint64(idxBuf[0:8], msg.Offset)
	binary.BigEndian.PutUint64(idxBuf[8:16], uint64(seg.walPosition))

	if _, err := seg.indexFile.Write(idxBuf); err != nil {
		return 0, err
	}

	seg.memOffsets = append(seg.memOffsets, msg.Offset)
	seg.memPositions = append(seg.memPositions, seg.walPosition)

	msgSize := int64(HeaderSize + int(msg.KeySize) + int(msg.PayloadSize))
	seg.walPosition += msgSize

	rc := msg.RecordCount
	if rc == 0 {
		rc = 1
	}
	seg.nextOffset += uint64(rc)

	p.cond.Broadcast()

	return msg.Offset, nil
}

// lookupposition finds the physical file and byte position for a message offset checking the active segment first
func (p *Partition) LookupPosition(offset uint64) (*os.File, int64, error) {
	p.mu.RLock()

	idx := sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].BaseOffset > offset
	}) - 1

	if idx < 0 {
		p.mu.RUnlock()
		return nil, 0, errors.New("offset out of bounds (too old / dropped)")
	}

	seg := p.segments[idx]
	if offset >= seg.nextOffset {
		p.mu.RUnlock()
		return nil, 0, errors.New("offset out of bounds (future)")
	}

	if !seg.isClosed {
		localIdx := sort.Search(len(seg.memOffsets), func(i int) bool {
			return seg.memOffsets[i] > offset
		}) - 1

		if localIdx >= 0 {
			pos := seg.memPositions[localIdx]
			p.mu.RUnlock()
			return seg.walFile, pos, nil
		}
	}

	idxReader := seg.indexReader
	idxFile := seg.indexFile
	walFile := seg.walFile
	p.mu.RUnlock()

	stat, _ := idxFile.Stat()
	numRecords := int(stat.Size() / int64(IndexRecordSize))

	localIdx := sort.Search(numRecords, func(i int) bool {
		buf := make([]byte, 8)
		readOffset := int64(i) * int64(IndexRecordSize)
		if idxReader != nil {
			idxReader.ReadAt(buf, readOffset)
		} else {
			idxFile.ReadAt(buf, readOffset)
		}
		return binary.BigEndian.Uint64(buf) > offset
	}) - 1

	if localIdx < 0 {
		return nil, 0, errors.New("offset not found in index")
	}

	buf := make([]byte, 8)
	readOffset := int64(localIdx)*int64(IndexRecordSize) + 8
	if idxReader != nil {
		idxReader.ReadAt(buf, readOffset)
	} else {
		idxFile.ReadAt(buf, readOffset)
	}

	return walFile, int64(binary.BigEndian.Uint64(buf)), nil
}

// enforceretention deletes old immutable segments to free up disk space without affecting active reads
func (p *Partition) EnforceRetention() {
	p.mu.Lock()
	defer p.mu.Unlock()

	retention := time.Duration(p.cfg.Storage.RetentionHours) * time.Hour
	cutoffTime := time.Now().Add(-retention)

	survivingSegments := make([]*Segment, 0, len(p.segments))

	for _, seg := range p.segments {
		if !seg.isClosed {
			survivingSegments = append(survivingSegments, seg)
			continue
		}

		stat, err := seg.walFile.Stat()
		if err != nil {
			survivingSegments = append(survivingSegments, seg)
			continue
		}

		if stat.ModTime().Before(cutoffTime) {
			seg.walFile.Close()
			seg.indexFile.Close()
			if seg.indexReader != nil {
				seg.indexReader.Close()
			}
			os.Remove(seg.walFile.Name())
			os.Remove(seg.indexFile.Name())
		} else {
			survivingSegments = append(survivingSegments, seg)
		}
	}

	p.segments = survivingSegments
}

// compactlogs runs background compaction to deduplicate messages by key without blocking the broker
func (p *Partition) CompactLogs() error {
	p.mu.Lock()
	if p.isCompacting || len(p.segments) <= 2 {
		p.mu.Unlock()
		return nil
	}
	p.isCompacting = true

	numToCompact := len(p.segments) - 1
	segmentsToCompact := make([]*Segment, numToCompact)
	copy(segmentsToCompact, p.segments[:numToCompact])

	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.isCompacting = false
		p.mu.Unlock()
	}()

	latestOffsets := make(map[string]uint64)
	for _, seg := range segmentsToCompact {
		fileInfo, _ := seg.walFile.Stat()
		fileSize := fileInfo.Size()
		var pos int64 = 0
		for pos < fileSize {
			header := make([]byte, HeaderSize)
			_, err := seg.walFile.ReadAt(header, pos)
			if err != nil {
				break
			}

			offset := binary.BigEndian.Uint64(header[9:17])
			keySize := binary.BigEndian.Uint32(header[17:21])
			payloadSize := binary.BigEndian.Uint32(header[21:25])

			if keySize > 0 {
				keyBuf := make([]byte, keySize)
				seg.walFile.ReadAt(keyBuf, pos+int64(HeaderSize))
				latestOffsets[string(keyBuf)] = offset
			}
			pos += int64(HeaderSize) + int64(keySize) + int64(payloadSize)
		}
	}

	baseOffset := segmentsToCompact[0].BaseOffset
	baseName := fmt.Sprintf("%020d-compacted", baseOffset)
	walPath := filepath.Join(p.dir, baseName+".wal")
	idxPath := filepath.Join(p.dir, baseName+".idx")

	newWal, _ := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	newIdx, _ := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	newWalPos := int64(0)

	for _, seg := range segmentsToCompact {
		fileInfo, _ := seg.walFile.Stat()
		fileSize := fileInfo.Size()
		var pos int64 = 0
		for pos < fileSize {
			header := make([]byte, HeaderSize)
			seg.walFile.ReadAt(header, pos)

			offset := binary.BigEndian.Uint64(header[9:17])
			keySize := binary.BigEndian.Uint32(header[17:21])
			payloadSize := binary.BigEndian.Uint32(header[21:25])

			var keyStr string
			if keySize > 0 {
				keyBuf := make([]byte, keySize)
				seg.walFile.ReadAt(keyBuf, pos+int64(HeaderSize))
				keyStr = string(keyBuf)
			}

			msgSize := int64(HeaderSize) + int64(keySize) + int64(payloadSize)

			if latestOffsets[keyStr] == offset {
				msgBytes := make([]byte, msgSize)
				seg.walFile.ReadAt(msgBytes, pos)
				newWal.Write(msgBytes)

				idxBuf := make([]byte, IndexRecordSize)
				binary.BigEndian.PutUint64(idxBuf[0:8], offset)
				binary.BigEndian.PutUint64(idxBuf[8:16], uint64(newWalPos))
				newIdx.Write(idxBuf)
				newWalPos += msgSize
			}
			pos += msgSize
		}
	}

	newWal.Close()
	newIdx.Close()

	finalWalPath := filepath.Join(p.dir, fmt.Sprintf("%020d.wal", baseOffset))
	finalIdxPath := filepath.Join(p.dir, fmt.Sprintf("%020d.idx", baseOffset))
	os.Rename(walPath, finalWalPath)
	os.Rename(idxPath, finalIdxPath)

	newWalFinal, _ := os.OpenFile(finalWalPath, os.O_RDWR|os.O_APPEND, 0666)
	newIdxFinal, _ := os.OpenFile(finalIdxPath, os.O_RDWR|os.O_APPEND, 0666)
	newReader, _ := mmap.Open(finalIdxPath)

	compactedSeg := &Segment{
		BaseOffset:  baseOffset,
		walFile:     newWalFinal,
		indexFile:   newIdxFinal,
		indexReader: newReader,
		isClosed:    true,
	}

	p.mu.Lock()
	newSegments := []*Segment{compactedSeg}
	newSegments = append(newSegments, p.segments[numToCompact:]...)
	p.segments = newSegments
	p.mu.Unlock()

	for _, seg := range segmentsToCompact {
		seg.walFile.Close()
		seg.indexFile.Close()
		if seg.indexReader != nil {
			seg.indexReader.Close()
		}
		os.Remove(seg.walFile.Name())
		os.Remove(seg.indexFile.Name())
	}

	return nil
}

// waitforoffset blocks until the requested offset is appended so producers and consumers sync efficiently
func (p *Partition) WaitForOffset(offset uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for p.active == nil || offset >= p.active.nextOffset {
		p.cond.Wait()
	}
}

// getoldestoffset returns the lowest persisted base offset to help lagging consumers fast forward
func (p *Partition) GetOldestOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.segments) > 0 {
		return p.segments[0].BaseOffset
	}
	return 0
}

// recoversegment checks wal integrity drops bad tail data and rebuilds the index file for consistency
func recoverSegment(walPath, idxPath string, baseOffset uint64) (uint64, int64, []uint64, []int64, error) {
	wal, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		return baseOffset, 0, nil, nil, err
	}
	defer wal.Close()

	tempIdxPath := idxPath + ".tmp"
	tempIdx, err := os.OpenFile(tempIdxPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return baseOffset, 0, nil, nil, err
	}

	fileStat, err := wal.Stat()
	if err != nil {
		tempIdx.Close()
		return baseOffset, 0, nil, nil, err
	}
	totalFileSize := fileStat.Size()

	var currentPos int64 = 0
	expectedOffset := baseOffset
	var mOffsets []uint64
	var mPositions []int64

	for {
		header := make([]byte, HeaderSize)
		n, err := wal.ReadAt(header, currentPos)
		if err == io.EOF {
			break
		}
		if err != nil || n < HeaderSize {
			break
		}

		offset := binary.BigEndian.Uint64(header[9:17])
		keySize := binary.BigEndian.Uint32(header[17:21])
		payloadSize := binary.BigEndian.Uint32(header[21:25])

		recordCount := binary.BigEndian.Uint32(header[25:29])
		if recordCount == 0 {
			recordCount = 1
		}

		if offset != expectedOffset {
			log.Printf("CORRUPTION DETECTED in %s: expected offset %d, got %d. Truncating file.", walPath, expectedOffset, offset)
			break
		}

		msgSize := int64(HeaderSize) + int64(keySize) + int64(payloadSize)

		if currentPos+msgSize > totalFileSize {
			log.Printf("PARTIAL MESSAGE DETECTED. Truncating tail.")
			break
		}

		idxBuf := make([]byte, IndexRecordSize)
		binary.BigEndian.PutUint64(idxBuf[0:8], offset)
		binary.BigEndian.PutUint64(idxBuf[8:16], uint64(currentPos))
		tempIdx.Write(idxBuf)

		mOffsets = append(mOffsets, offset)
		mPositions = append(mPositions, currentPos)

		currentPos += msgSize

		expectedOffset += uint64(recordCount)
	}

	tempIdx.Close()

	if err := os.Truncate(walPath, currentPos); err != nil {
		return baseOffset, 0, nil, nil, err
	}

	os.Rename(tempIdxPath, idxPath)

	return expectedOffset, currentPos, mOffsets, mPositions, nil
}

// close flushes and closes all file descriptors to stop data corruption
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, seg := range p.segments {
		seg.walFile.Sync()
		seg.walFile.Close()

		seg.indexFile.Sync()
		seg.indexFile.Close()

		if seg.indexReader != nil {
			seg.indexReader.Close()
		}
	}
}
