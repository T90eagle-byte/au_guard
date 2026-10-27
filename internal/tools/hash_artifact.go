package tools

import (
	"bytes"
	"context"
	"errors"
	"example.org/auguard/internal/config"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

const HashOutputLimit = 4096

type cappedBuffer struct {
	limit    int
	buf      bytes.Buffer
	overflow bool
}

func newCappedBuffer(limit int) (*cappedBuffer, error) {
	if limit < 0 {
		return nil, fmt.Errorf("negative output limit: %d", limit)
	}
	return &cappedBuffer{limit: limit}, nil
}

func (w *cappedBuffer) Write(p []byte) (int, error) {
	room := w.limit - w.buf.Len()

	if room > len(p) {
		room = len(p)
	}

	if room > 0 {
		_, _ = w.buf.Write(p[:room])
	}

	if room < len(p) {
		w.overflow = true
	}

	// Сообщаем, что вся порция обработана:
	// лишние байты отброшены, но чтение pipe продолжается.
	return len(p), nil
}

type boundedReader struct {
	r   io.Reader
	max int64
	n   int64
}

func (r *boundedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if r.n >= r.max+1 {
		return 0, io.EOF
	}

	remaining := r.max + 1 - r.n

	if int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}

	n, err := r.r.Read(p)
	r.n += int64(n)

	return n, err
}

const HashMaxTimeout = 2 * time.Second

var (
	ErrHashInvalidTimeout = errors.New("invalid hash timeout")
	ErrHashInfrastructure = errors.New("hash infrastructure failure")
	ErrHashProcessExit    = errors.New("hash process failed")
	ErrHashOutputOverflow = errors.New("hash output limit exceeded")
	ErrHashInvalidOutput  = errors.New("invalid hash output")
	ErrHashSizeMismatch   = errors.New("artifact size changed during hashing")
)

type HashResult struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// Внутренняя точка тестирования.
// Вызывающий код не может задавать executable через JSON.
type hashRunner func(
	context.Context,
	io.Reader,
	io.Writer,
	io.Writer,
) error

type HashArtifact struct {
	reader  *ArtifactReader
	timeout time.Duration
	runner  hashRunner
}

func NewHashArtifact(
	reader *ArtifactReader,
	timeout time.Duration,
) (*HashArtifact, error) {
	if reader == nil {
		return nil, ErrArtifactRoot
	}

	if timeout <= 0 || timeout > HashMaxTimeout {
		return nil, ErrHashInvalidTimeout
	}

	return &HashArtifact{
		reader:  reader,
		timeout: timeout,
		runner:  runFixedSHA256,
	}, nil
}

func newHashCommand(
	ctx context.Context,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "/usr/bin/sha256sum", "-")

	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	// Никакие переменные родительского окружения
	// дочернему процессу не передаются.
	cmd.Env = []string{
		"LC_ALL=C",
		"LANG=C",
	}

	return cmd
}

func runFixedSHA256(
	ctx context.Context,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
) error {
	cmd := newHashCommand(ctx, stdin, stdout, stderr)
	return cmd.Run()
}

func parseHashOutput(output string) (string, error) {
	// Ожидаем строго:
	// 64 символа lowercase hex + "  -\n"
	if len(output) != 68 ||
		!strings.HasSuffix(output, "  -\n") {
		return "", ErrHashInvalidOutput
	}

	digest := output[:64]

	for _, ch := range digest {
		if (ch < '0' || ch > '9') &&
			(ch < 'a' || ch > 'f') {
			return "", ErrHashInvalidOutput
		}
	}

	return digest, nil
}

func (h *HashArtifact) Run(
	ctx context.Context,
	path string,
) (HashResult, error) {
	var empty HashResult

	if err := ctx.Err(); err != nil {
		return empty, err
	}

	toolCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	file, expectedSize, err := h.reader.OpenRegular(toolCtx, path)
	if err != nil {
		return empty, err
	}
	defer file.Close()

	bounded := &boundedReader{
		r:   file,
		max: ArtifactMaxBytes,
	}

	stdoutCap, err := newCappedBuffer(HashOutputLimit)
	if err != nil {
		return empty, err
	}

	stderrCap, err := newCappedBuffer(HashOutputLimit)
	if err != nil {
		return empty, err
	}

	err = h.runner(
		toolCtx,
		bounded,
		stdoutCap,
		stderrCap,
	)

	if ctxErr := toolCtx.Err(); ctxErr != nil {
		return empty, fmt.Errorf(
			"hash interrupted: %w",
			ctxErr,
		)
	}

	if err != nil {
		var exitErr *exec.ExitError

		if errors.As(err, &exitErr) {
			return empty, ErrHashProcessExit
		}

		return empty, ErrHashInfrastructure
	}

	if bounded.n > ArtifactMaxBytes {
		return empty, ErrArtifactTooLarge
	}

	if bounded.n != expectedSize {
		return empty, ErrHashSizeMismatch
	}

	if stdoutCap.overflow || stderrCap.overflow {
		return empty, ErrHashOutputOverflow
	}

	if stderrCap.buf.Len() != 0 {
		return empty, ErrHashInvalidOutput
	}

	digest, err := parseHashOutput(
		stdoutCap.buf.String(),
	)
	if err != nil {
		return empty, err
	}

	return HashResult{
		Path:   path,
		SHA256: digest,
		Bytes:  bounded.n,
	}, nil
}

func (h *HashArtifact) RunAndUpdateReport(
	ctx context.Context,
	path string,
) (HashResult, error) {
	result, err := h.Run(ctx, path)
	if err != nil {
		return HashResult{}, err
	}

	if err := h.reader.UpdateHashReport(
		ctx, result,
	); err != nil {
		return HashResult{}, err
	}

	return result, nil
}

func NewHashArtifactFromConfig(
	reader *ArtifactReader,
	cfg config.Config,
) (*HashArtifact, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return NewHashArtifact(reader, cfg.HashTimeout)
}
