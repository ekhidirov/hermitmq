package broker

import (
	"encoding/json"
	"fmt"
	"os"
)

// holds all global broker settings
type Config struct {
	Server   ServerConfig   `json:"server"`
	Storage  StorageConfig  `json:"storage"`
	Security SecurityConfig `json:"security"`
}

// network listener and connection options
type ServerConfig struct {
	Host                string `json:"host"`
	Port                int    `json:"port"`
	HandshakeTimeoutSec int    `json:"handshake_timeout_sec"`
}

// disk io segment rotation and retention rules
type StorageConfig struct {
	DataDir            string `json:"data_dir"`
	MaxSegmentMB       int    `json:"max_segment_mb"`
	RetentionHours     int    `json:"retention_hours"`
	CleanupIntervalMin int    `json:"cleanup_interval_min"`
	BufferPoolSizeKB   int    `json:"buffer_pool_size_kb"`
	MaxBufferReturnKB  int    `json:"max_buffer_return_kb"`
	FlushIntervalMs    int    `json:"flush_interval_ms"`
}

// limits to stop resource exhaustion like oom
type SecurityConfig struct {
	MaxPayloadMB int `json:"max_payload_mb"`
}

// reads and checks settings from a json file
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config %s: %w", path, err)
	}
	defer file.Close()

	cfg := &Config{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// enforces safe limits and sets defaults if admin gives bad values
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d (must be 1-65535)", c.Server.Port)
	}
	if c.Server.HandshakeTimeoutSec <= 0 {
		c.Server.HandshakeTimeoutSec = 5
	}

	if c.Storage.FlushIntervalMs <= 0 {
		c.Storage.FlushIntervalMs = 500
	}

	if c.Storage.BufferPoolSizeKB <= 0 {
		c.Storage.BufferPoolSizeKB = 32
	}
	if c.Storage.MaxBufferReturnKB <= 0 {
		c.Storage.MaxBufferReturnKB = 64
	}
	if c.Storage.MaxBufferReturnKB < c.Storage.BufferPoolSizeKB {
		c.Storage.MaxBufferReturnKB = c.Storage.BufferPoolSizeKB * 2
	}

	if c.Storage.DataDir == "" {
		c.Storage.DataDir = "./hermitmq-data"
	}
	if c.Storage.MaxSegmentMB <= 0 {
		c.Storage.MaxSegmentMB = 10
	}
	if c.Storage.RetentionHours <= 0 {
		c.Storage.RetentionHours = 168
	}
	if c.Storage.CleanupIntervalMin <= 0 {
		c.Storage.CleanupIntervalMin = 5
	}

	if c.Security.MaxPayloadMB <= 0 {
		c.Security.MaxPayloadMB = 50
	}

	return nil
}
