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

func (r *ArtifactReader) OpenRegular(
	ctx context.Context,
	name string,
) (*os.File, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	if err := validateArtifactName(name); err != nil {
		return nil, 0, err
	}

	f, err := r.root.OpenFile(
		name,
		os.O_RDONLY|syscall.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, 0, ErrArtifactUnavailable
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("stat artifact: %w", err)
	}

	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, 0, ErrNotRegular
	}

	if info.Size() < 0 {
		_ = f.Close()
		return nil, 0, ErrInvalidArtifactSize
	}

	if info.Size() > r.maxBytes {
		_ = f.Close()
		return nil, 0, ErrArtifactTooLarge
	}

	if err := ctx.Err(); err != nil {
		_ = f.Close()
		return nil, 0, err
	}

	return f, info.Size(), nil
}

func (r *ArtifactReader) Read(
	ctx context.Context,
	name string,
) ([]byte, error) {
	f, _, err := r.OpenRegular(ctx, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

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
