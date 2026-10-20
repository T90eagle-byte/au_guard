package config_test

import (
	"errors"
	"testing"
	"time"

	"example.org/auguard/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.Default()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
}

func TestConfigBounds(t *testing.T) {
	tests := []struct {
		name   string
		change func(*config.Config)
		valid  bool
	}{
		{"workers_min", func(c *config.Config) { c.Workers = 1 }, true},
		{"workers_max", func(c *config.Config) { c.Workers = 16 }, true},
		{"workers_below", func(c *config.Config) { c.Workers = 0 }, false},
		{"workers_above", func(c *config.Config) { c.Workers = 17 }, false},

		{"queue_min", func(c *config.Config) { c.Queue = 1 }, true},
		{"queue_max", func(c *config.Config) { c.Queue = 64 }, true},
		{"queue_below", func(c *config.Config) { c.Queue = 0 }, false},
		{"queue_above", func(c *config.Config) { c.Queue = 65 }, false},

		{"line_min", func(c *config.Config) { c.MaxLineBytes = 128 }, true},
		{"line_max", func(c *config.Config) { c.MaxLineBytes = 65536 }, true},
		{"line_below", func(c *config.Config) { c.MaxLineBytes = 127 }, false},
		{"line_above", func(c *config.Config) { c.MaxLineBytes = 65537 }, false},

		{"events_min", func(c *config.Config) { c.MaxEvents = 1 }, true},
		{"events_max", func(c *config.Config) { c.MaxEvents = 1000 }, true},
		{"events_below", func(c *config.Config) { c.MaxEvents = 0 }, false},
		{"events_above", func(c *config.Config) { c.MaxEvents = 1001 }, false},

		{"timeout_min", func(c *config.Config) { c.Timeout = 100 * time.Millisecond }, true},
		{"timeout_max", func(c *config.Config) { c.Timeout = 30 * time.Second }, true},
		{"timeout_below", func(c *config.Config) { c.Timeout = 99 * time.Millisecond }, false},
		{"timeout_above", func(c *config.Config) { c.Timeout = 30*time.Second + time.Nanosecond }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			tt.change(&cfg)

			err := cfg.Validate()

			if tt.valid {
				if err != nil {
					t.Fatalf("expected valid config, got %v", err)
				}
				return
			}

			if !errors.Is(err, config.ErrInvalidConfig) {
				t.Fatalf("want ErrInvalidConfig, got %v", err)
			}
		})
	}
}
