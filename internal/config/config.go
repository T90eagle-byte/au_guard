package config

import (
	"errors"
	"fmt"
	"time"
)

var ErrInvalidConfig = errors.New("invalid config")

type Config struct {
	Workers      int
	Queue        int
	MaxLineBytes int
	MaxEvents    int
	Timeout      time.Duration
}

func Default() Config {
	return Config{
		Workers:      4,
		Queue:        8,
		MaxLineBytes: 65536,
		MaxEvents:    1000,
		Timeout:      5 * time.Second,
	}
}

func (c Config) Validate() error {
	if c.Workers < 1 || c.Workers > 16 {
		return fmt.Errorf("config workers: %w", ErrInvalidConfig)
	}

	if c.Queue < 1 || c.Queue > 64 {
		return fmt.Errorf("config queue: %w", ErrInvalidConfig)
	}

	if c.MaxLineBytes < 128 || c.MaxLineBytes > 65536 {
		return fmt.Errorf("config max_line_bytes: %w", ErrInvalidConfig)
	}

	if c.MaxEvents < 1 || c.MaxEvents > 1000 {
		return fmt.Errorf("config max_events: %w", ErrInvalidConfig)
	}

	if c.Timeout < 100*time.Millisecond || c.Timeout > 30*time.Second {
		return fmt.Errorf("config timeout: %w", ErrInvalidConfig)
	}

	return nil
}
