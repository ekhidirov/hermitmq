package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	MagicV1         byte = 0x01
	HeaderSize      int  = 29
	IndexRecordSize int  = 16
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)
		return &b
	},
}

// message represents the memory layout of the custom binary protocol built for disk saves and fast network decoding
type Message struct {
	Magic       byte
	Timestamp   uint64
	Offset      uint64
	KeySize     uint32
	PayloadSize uint32
	RecordCount uint32
	Key         []byte
	Payload     []byte
}

// encode writes the message straight to the stream using pooled buffers for zero allocation on normal payload sizes
func (m *Message) Encode(w io.Writer, pool *sync.Pool, maxReturnBytes int) error {
	totalSize := int(HeaderSize) + int(m.KeySize) + int(m.PayloadSize)
	var buf []byte
	var bufPtr *[]byte

	if pool != nil {
		bufPtr = pool.Get().(*[]byte)
		buf = *bufPtr

		if totalSize > cap(buf) {
			buf = make([]byte, totalSize)
		} else {
			buf = buf[:totalSize]
		}

		if cap(buf) <= maxReturnBytes {
			*bufPtr = buf
			defer pool.Put(bufPtr)
		}
	} else {
		buf = make([]byte, totalSize)
	}

	buf[0] = m.Magic
	binary.BigEndian.PutUint64(buf[1:9], m.Timestamp)
	binary.BigEndian.PutUint64(buf[9:17], m.Offset)
	binary.BigEndian.PutUint32(buf[17:21], m.KeySize)
	binary.BigEndian.PutUint32(buf[21:25], m.PayloadSize)
	binary.BigEndian.PutUint32(buf[25:29], m.RecordCount)

	if m.KeySize > 0 {
		copy(buf[29:29+m.KeySize], m.Key)
	}
	if m.PayloadSize > 0 {
		copy(buf[29+m.KeySize:totalSize], m.Payload)
	}

	_, err := w.Write(buf[:totalSize])
	return err
}

// decode builds a message from a byte stream checking structure and size limits to stop memory attacks
func (m *Message) Decode(r io.Reader, maxPayloadMB int) error {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	m.Magic = header[0]
	if m.Magic != MagicV1 {
		return fmt.Errorf("unsupported protocol version: %d", m.Magic)
	}

	m.Timestamp = binary.BigEndian.Uint64(header[1:9])
	m.Offset = binary.BigEndian.Uint64(header[9:17])
	m.KeySize = binary.BigEndian.Uint32(header[17:21])
	m.PayloadSize = binary.BigEndian.Uint32(header[21:25])
	m.RecordCount = binary.BigEndian.Uint32(header[25:29])

	if m.RecordCount == 0 {
		m.RecordCount = 1
	}

	if m.KeySize > 0 {
		m.Key = make([]byte, m.KeySize)
		if _, err := io.ReadFull(r, m.Key); err != nil {
			return err
		}
	}

	const MaxPayloadSize = 50 * 1024 * 1024

	maxBytes := uint32(maxPayloadMB * 1024 * 1024)

	if m.PayloadSize > maxBytes {
		return fmt.Errorf("security policy violation: payload too large (%d bytes)", m.PayloadSize)
	}

	if m.PayloadSize > 0 {
		m.Payload = make([]byte, m.PayloadSize)
		if _, err := io.ReadFull(r, m.Payload); err != nil {
			return err
		}
	}

	return nil
}
