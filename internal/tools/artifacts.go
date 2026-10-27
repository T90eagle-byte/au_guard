package tools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const ArtifactMaxBytes int64 = 64 * 1024

var (
	ErrInvalidArtifactName = errors.New("invalid artifact name")
	ErrArtifactTooLarge    = errors.New("artifact too large")
	ErrNotRegular          = errors.New("artifact is not a regular file")
	ErrArtifactRoot        = errors.New("artifact root unavailable")
	ErrArtifactUnavailable = errors.New("artifact unavailable")
	ErrInvalidArtifactSize = errors.New("invalid artifact size")
)

type ArtifactReader struct {
	root     *os.Root
	maxBytes int64
	reportMu sync.Mutex
}

func NewArtifactReader(rootPath string, maxBytes int64) (*ArtifactReader, error) {
	if maxBytes != ArtifactMaxBytes {
		return nil, fmt.Errorf(
			"artifact limit must be %d bytes",
			ArtifactMaxBytes,
		)
	}

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, ErrArtifactRoot
	}

	return &ArtifactReader{
		root:     root,
		maxBytes: maxBytes,
	}, nil
}

func (r *ArtifactReader) Close() error {
	return r.root.Close()
}

func validateArtifactName(name string) error {
	if name == "" || len(name) > 256 {
		return ErrInvalidArtifactName
	}

	if filepath.IsAbs(name) ||
		strings.ContainsAny(name, "\\\x00") {
		return ErrInvalidArtifactName
	}

	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return ErrInvalidArtifactName
		}
	}

	return nil
}

func (r *ArtifactReader) Read(
	ctx context.Context,
	name string,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := validateArtifactName(name); err != nil {
		return nil, err
	}

	f, err := r.root.OpenFile(
		name,
		os.O_RDONLY|syscall.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, ErrArtifactUnavailable
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat artifact: %w", err)
	}

	if !info.Mode().IsRegular() {
		return nil, ErrNotRegular
	}

	if info.Size() < 0 {
		return nil, ErrInvalidArtifactSize
	}

	if info.Size() > r.maxBytes {
		return nil, ErrArtifactTooLarge
	}

	data, err := io.ReadAll(
		io.LimitReader(f, r.maxBytes+1),
	)
	if err != nil {
		return nil, fmt.Errorf("read artifact: %w", err)
	}

	if int64(len(data)) > r.maxBytes {
		return nil, ErrArtifactTooLarge
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return data, nil
}
