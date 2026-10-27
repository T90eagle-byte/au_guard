package tools

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.org/auguard/internal/config"
)

func TestHashArtifactConfiguredTimeout(t *testing.T) {
	cfg := config.Default()

	if cfg.HashTimeout != HashMaxTimeout {
		t.Fatalf(
			"default hash timeout=%s, want %s",
			cfg.HashTimeout,
			HashMaxTimeout,
		)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewArtifactReader(
		"../../testdata/artifacts",
		ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	hasher, err := NewHashArtifactFromConfig(reader, cfg)
	if err != nil {
		t.Fatal(err)
	}

	result, err := hasher.Run(
		context.Background(),
		"alpha.txt",
	)
	if err != nil {
		t.Fatal(err)
	}

	const expected = "a5063a52898ce0d67a3ebed2dc17398783a76f42143c68a0466c22ef1c3c258b"

	if result.SHA256 != expected || result.Bytes != 20 {
		t.Fatalf("unexpected result: %+v", result)
	}

	t.Log("configured_timeout=2s fixture_hash=correct")

	// Таймаут ingest по-прежнему может быть 30 секунд.
	cfg.Timeout = 30 * time.Second

	if err := cfg.Validate(); err != nil {
		t.Fatalf("ingest timeout must remain valid: %v", err)
	}

	// Но таймаут hash_artifact нельзя поднять до 3 секунд.
	cfg.HashTimeout = 3 * time.Second

	if !errors.Is(cfg.Validate(), config.ErrInvalidConfig) {
		t.Fatal("config accepted hash timeout > 2s")
	}

	if _, err := NewHashArtifactFromConfig(
		reader, cfg,
	); !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("expected invalid config, got %v", err)
	}

	if _, err := NewHashArtifact(
		reader,
		3*time.Second,
	); !errors.Is(err, ErrHashInvalidTimeout) {
		t.Fatalf("constructor accepted timeout > 2s: %v", err)
	}

	t.Log("ingest_timeout=30s remains_valid=true")
	t.Log("hash_timeout=3s rejected_by_config_and_constructor=true")

	// Уменьшение таймаута разрешается.
	cfg.HashTimeout = 100 * time.Millisecond

	hasher, err = NewHashArtifactFromConfig(reader, cfg)
	if err != nil {
		t.Fatal(err)
	}

	if hasher.timeout != 100*time.Millisecond {
		t.Fatal("configured timeout not applied")
	}

	t.Log("configured_timeout=100ms applied=true")
}
