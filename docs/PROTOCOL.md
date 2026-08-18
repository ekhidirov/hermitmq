# HermitMQ Binary Protocol Specification

HermitMQ uses a custom, zero-serialization binary protocol over TCP. It is designed to be extremely lightweight, utilizing a fixed-size 29-byte header.

## 1. Connection Handshake
When a client connects, it must first send a 1-byte client type identifier, followed by connection metadata based on the role.

| Client Type | Byte | Description |
| :--- | :--- | :--- |
| **Producer** | `0x01` | Sends messages to the broker. |
| **Consumer** | `0x02` | Reads messages from the broker. |
| **Commit**   | `0x03` | Commits read offsets for a consumer group. |

## 2. Message Format
Every message exchanged over the network (after the handshake) follows this exact binary layout (Big-Endian).

### Header (29 Bytes)
| Field | Size (Bytes) | Type | Description |
| :--- | :--- | :--- | :--- |
| `Magic` | 1 | `uint8` | Protocol version (Always `0x01`). Rejects bad packets. |
| `Timestamp` | 8 | `uint64` | Unix timestamp in nanoseconds. |
| `Offset` | 8 | `uint64` | Message sequence number (0 when sending, populated by broker). |
| `KeySize` | 4 | `uint32` | Length of the optional routing key. |
| `PayloadSize` | 4 | `uint32` | Length of the message payload. Max 50MB by default. |
| `RecordCount` | 4 | `uint32` | Number of logical records (useful for batching). |

### Body (Variable Length)
| Field | Size (Bytes) | Description |
| :--- | :--- | :--- |
| `Key` | `KeySize` | The routing key bytes (if `KeySize > 0`). |
| `Payload` | `PayloadSize` | The actual message data. |