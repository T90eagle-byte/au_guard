package tools

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCappedBuffer(t *testing.T) {
	t.Run("exact_4096", func(t *testing.T) {
		w, err := newCappedBuffer(4096)
		if err != nil {
			t.Fatal(err)
		}

		data := bytes.Repeat([]byte("A"), 4096)

		n, err := w.Write(data)
		if err != nil || n != len(data) {
			t.Fatalf("write: n=%d err=%v", n, err)
		}

		if w.buf.Len() != 4096 || w.overflow {
			t.Fatal("exact limit incorrectly handled")
		}

		t.Log("limit=4096 received=4096 stored=4096 overflow=false")
	})

	t.Run("overflow_4097", func(t *testing.T) {
		w, err := newCappedBuffer(4096)
		if err != nil {
			t.Fatal(err)
		}

		data := bytes.Repeat([]byte("B"), 4097)

		n, err := w.Write(data)
		if err != nil || n != len(data) {
			t.Fatalf("write: n=%d err=%v", n, err)
		}

		if w.buf.Len() != 4096 || !w.overflow {
			t.Fatal("overflow was not detected")
		}

		t.Log("limit=4096 received=4097 stored=4096 overflow=true")
	})

	t.Run("multipart_write", func(t *testing.T) {
		w, err := newCappedBuffer(5)
		if err != nil {
			t.Fatal(err)
		}

		for _, part := range []string{"ab", "cd", "efg"} {
			n, err := w.Write([]byte(part))
			if err != nil || n != len(part) {
				t.Fatalf("write %q: n=%d err=%v", part, n, err)
			}
		}

		if w.buf.String() != "abcde" || !w.overflow {
			t.Fatalf(
				"buffer=%q overflow=%v",
				w.buf.String(), w.overflow,
			)
		}

		t.Log("multipart: received=7 stored=5 overflow=true")
	})

	t.Run("separate_streams", func(t *testing.T) {
		stdout, err := newCappedBuffer(4)
		if err != nil {
			t.Fatal(err)
		}
		stderr, err := newCappedBuffer(4)
		if err != nil {
			t.Fatal(err)
		}

		_, _ = stdout.Write([]byte("12345"))
		_, _ = stderr.Write([]byte("ok"))

		if !stdout.overflow || stderr.overflow {
			t.Fatal("output streams are not independent")
		}

		t.Log("stdout and stderr limits are independent")
	})

	t.Run("negative_limit", func(t *testing.T) {
		if _, err := newCappedBuffer(-1); err == nil {
			t.Fatal("negative limit was accepted")
		}
	})
}

func TestBoundedReader(t *testing.T) {
	tests := []struct {
		name      string
		inputSize int
		wantRead  int
	}{
		{"exact_65536", 65536, 65536},
		{"one_extra_65537", 65537, 65537},
		{"many_extra_70000", 70000, 65537},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := strings.NewReader(
				strings.Repeat("A", tt.inputSize),
			)

			reader := &boundedReader{
				r:   source,
				max: ArtifactMaxBytes,
			}

			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}

			if len(data) != tt.wantRead ||
				reader.n != int64(tt.wantRead) {
				t.Fatalf(
					"read=%d counted=%d want=%d",
					len(data), reader.n, tt.wantRead,
				)
			}

			tooLarge := reader.n > ArtifactMaxBytes

			t.Logf(
				"input=%d transmitted=%d too_large=%v",
				tt.inputSize,
				reader.n,
				tooLarge,
			)
		})
	}
}

func TestHashArtifactKnownFixture(t *testing.T) {
	reader, err := NewArtifactReader(
		"../../testdata/artifacts",
		ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	hasher, err := NewHashArtifact(
		reader,
		2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := hasher.Run(
		context.Background(),
		"alpha.txt",
	)
	if err != nil {
		t.Fatalf("hash artifact: %v", err)
	}

	const expected = "a5063a52898ce0d67a3ebed2dc17398783a76f42143c68a0466c22ef1c3c258b"

	if result.SHA256 != expected ||
		result.Bytes != 20 ||
		result.Path != "alpha.txt" {
		t.Fatalf("unexpected hash result: %+v", result)
	}

	t.Logf(
		"binary=/usr/bin/sha256sum artifact=%s bytes=%d sha256=%s",
		result.Path,
		result.Bytes,
		result.SHA256,
	)
}

func TestHashArtifactCommandContract(t *testing.T) {
	cmd := newHashCommand(
		context.Background(),
		strings.NewReader("fixture"),
		&cappedBuffer{limit: HashOutputLimit},
		&cappedBuffer{limit: HashOutputLimit},
	)

	expectedArgs := []string{
		"/usr/bin/sha256sum",
		"-",
	}

	expectedEnv := []string{
		"LC_ALL=C",
		"LANG=C",
	}

	if cmd.Path != "/usr/bin/sha256sum" {
		t.Fatalf("unexpected executable: %q", cmd.Path)
	}

	if !reflect.DeepEqual(cmd.Args, expectedArgs) {
		t.Fatalf("unexpected argv: %q", cmd.Args)
	}

	if !reflect.DeepEqual(cmd.Env, expectedEnv) {
		t.Fatalf("unexpected environment: %q", cmd.Env)
	}

	t.Log("executable=/usr/bin/sha256sum argv=[-] shell=false")
	t.Log("environment=[LC_ALL=C LANG=C] inherited=false")
	t.Log("stdout_limit=4096 stderr_limit=4096")
}

func TestHashArtifactTimeoutLimit(t *testing.T) {
	reader, err := NewArtifactReader(
		"../../testdata/artifacts",
		ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, err := NewHashArtifact(
		reader,
		3*time.Second,
	); !errors.Is(err, ErrHashInvalidTimeout) {
		t.Fatalf("expected timeout rejection, got %v", err)
	}

	if _, err := NewHashArtifact(
		reader,
		0,
	); !errors.Is(err, ErrHashInvalidTimeout) {
		t.Fatalf("expected zero timeout rejection, got %v", err)
	}

	if _, err := NewHashArtifact(
		reader,
		2*time.Second,
	); err != nil {
		t.Fatalf("2s must be accepted: %v", err)
	}

	t.Log("timeout=2s allowed=true")
	t.Log("timeout=3s allowed=false")
}

func TestHashArtifactDeadlineCancel(t *testing.T) {
	reader, err := NewArtifactReader(
		"../../testdata/artifacts",
		ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	h, err := NewHashArtifact(reader, 60*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Имитируем runner, который ждёт отмены.
	h.runner = func(
		ctx context.Context,
		_ io.Reader,
		_, _ io.Writer,
	) error {
		<-ctx.Done()
		return ctx.Err()
	}

	result, err := h.Run(
		context.Background(),
		"alpha.txt",
	)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	if result != (HashResult{}) {
		t.Fatal("deadline returned a nonempty result")
	}

	t.Log("deadline=60ms error=context deadline exceeded result_empty=true")

	// Проверяем отмену родительского контекста.
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.runner = func(
		ctx context.Context,
		_ io.Reader,
		_, _ io.Writer,
	) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}

	result, err = h.Run(parent, "alpha.txt")

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if result != (HashResult{}) {
		t.Fatal("cancellation returned a nonempty result")
	}

	t.Log("parent_cancel=true error=context canceled result_empty=true")
}

func TestHashArtifactFailureModes(t *testing.T) {
	reader, err := NewArtifactReader(
		"../../testdata/artifacts",
		ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	const digest = "a5063a52898ce0d67a3ebed2dc17398783a76f42143c68a0466c22ef1c3c258b"
	validOutput := digest + "  -\n"

	consume := func(in io.Reader) error {
		_, err := io.Copy(io.Discard, in)
		return err
	}

	tests := []struct {
		name   string
		runner hashRunner
		want   error
	}{
		{
			name: "nonzero_exit",
			runner: func(
				_ context.Context,
				_ io.Reader,
				_, _ io.Writer,
			) error {
				return &exec.ExitError{}
			},
			want: ErrHashProcessExit,
		},
		{
			name: "infrastructure_failure",
			runner: func(
				_ context.Context,
				_ io.Reader,
				_, stderr io.Writer,
			) error {
				_, _ = io.WriteString(
					stderr, "synthetic_private_diagnostic",
				)
				return errors.New("synthetic_private_diagnostic")
			},
			want: ErrHashInfrastructure,
		},
		{
			name: "invalid_digest",
			runner: func(
				_ context.Context,
				in io.Reader,
				stdout, _ io.Writer,
			) error {
				if err := consume(in); err != nil {
					return err
				}
				_, err := io.WriteString(
					stdout,
					strings.Repeat("A", 64)+"  -\n",
				)
				return err
			},
			want: ErrHashInvalidOutput,
		},
		{
			name: "stdout_overflow",
			runner: func(
				_ context.Context,
				in io.Reader,
				stdout, _ io.Writer,
			) error {
				if err := consume(in); err != nil {
					return err
				}
				_, err := io.WriteString(
					stdout,
					strings.Repeat("X", HashOutputLimit+1),
				)
				return err
			},
			want: ErrHashOutputOverflow,
		},
		{
			name: "stderr_overflow",
			runner: func(
				_ context.Context,
				in io.Reader,
				stdout, stderr io.Writer,
			) error {
				if err := consume(in); err != nil {
					return err
				}
				_, _ = io.WriteString(stdout, validOutput)
				_, err := io.WriteString(
					stderr,
					strings.Repeat("X", HashOutputLimit+1),
				)
				return err
			},
			want: ErrHashOutputOverflow,
		},
		{
			name: "unexpected_stderr",
			runner: func(
				_ context.Context,
				in io.Reader,
				stdout, stderr io.Writer,
			) error {
				if err := consume(in); err != nil {
					return err
				}
				_, _ = io.WriteString(stdout, validOutput)
				_, err := io.WriteString(
					stderr, "synthetic_private_diagnostic",
				)
				return err
			},
			want: ErrHashInvalidOutput,
		},
		{
			name: "size_mismatch",
			runner: func(
				_ context.Context,
				in io.Reader,
				stdout, _ io.Writer,
			) error {
				if _, err := io.CopyN(io.Discard, in, 10); err != nil {
					return err
				}
				_, err := io.WriteString(stdout, validOutput)
				return err
			},
			want: ErrHashSizeMismatch,
		},
	}

	for _, tt := range tests {
		h, err := NewHashArtifact(
			reader, 2*time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}

		h.runner = tt.runner

		result, err := h.Run(
			context.Background(),
			"alpha.txt",
		)

		if !errors.Is(err, tt.want) {
			t.Fatalf(
				"%s: expected %v, got %v",
				tt.name, tt.want, err,
			)
		}

		if result != (HashResult{}) {
			t.Fatalf("%s: unexpected result", tt.name)
		}

		if strings.Contains(
			err.Error(), "synthetic_private_diagnostic",
		) {
			t.Fatalf("%s: private diagnostics leaked", tt.name)
		}

		t.Logf(
			"%s: correctly_rejected=true result_empty=true",
			tt.name,
		)
	}
}

func TestHashArtifactGrowthAfterOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "alpha.txt")

	if err := os.WriteFile(
		path,
		[]byte("lab06 fixture alpha\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	reader, err := NewArtifactReader(
		dir,
		ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	h, err := NewHashArtifact(
		reader,
		2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	h.runner = func(
		_ context.Context,
		stdin io.Reader,
		_, _ io.Writer,
	) error {
		// Файл уже открыт, но его содержимое выросло.
		if err := os.WriteFile(
			path,
			bytes.Repeat([]byte("X"), 65537),
			0600,
		); err != nil {
			return err
		}

		_, err := io.Copy(io.Discard, stdin)
		return err
	}

	result, err := h.Run(
		context.Background(),
		"alpha.txt",
	)

	if !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("expected size limit error, got %v", err)
	}

	if result != (HashResult{}) {
		t.Fatal("oversize artifact returned a digest")
	}

	t.Log(
		"size_at_open=20 actual_read=65537 rejected=true digest_absent=true",
	)
}

func TestHashArtifactStrictDigest(t *testing.T) {
	valid := strings.Repeat("a", 64) + "  -\n"

	digest, err := parseHashOutput(valid)
	if err != nil || digest != strings.Repeat("a", 64) {
		t.Fatalf("valid output rejected: %v", err)
	}

	invalid := []string{
		strings.Repeat("A", 64) + "  -\n",
		strings.Repeat("a", 64) + "  file.txt\n",
		valid + "unexpected\n",
		strings.TrimSuffix(valid, "\n"),
	}

	for _, output := range invalid {
		if _, err := parseHashOutput(output); !errors.Is(
			err, ErrHashInvalidOutput,
		) {
			t.Fatalf("invalid output accepted: %q", output)
		}
	}

	t.Log("valid_digest=accepted malformed_outputs=4 rejected=true")
}

func TestHashArtifactSecurityBoundary(t *testing.T) {
	base := t.TempDir()
	rootPath := filepath.Join(base, "artifacts")
	outsidePath := filepath.Join(base, "outside.txt")

	if err := os.Mkdir(rootPath, 0700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(rootPath, "alpha.txt"),
		[]byte("lab06 fixture alpha\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		outsidePath,
		[]byte("outside fixture only\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(
		"../outside.txt",
		filepath.Join(rootPath, "escape-link.txt"),
	); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(
		filepath.Join(rootPath, "directory"),
		0700,
	); err != nil {
		t.Fatal(err)
	}

	reader, err := NewArtifactReader(
		rootPath, ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	h, err := NewHashArtifact(
		reader, 2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Считаем фактические вызовы внешнего runner.
	// Недопустимые пути должны отклоняться до него.
	runCalls := 0

	h.runner = func(
		_ context.Context,
		_ io.Reader,
		_, _ io.Writer,
	) error {
		runCalls++
		return errors.New("unexpected process invocation")
	}

	tests := []struct {
		name string
		path string
		want error
	}{
		{
			"traversal",
			"../outside.txt",
			ErrInvalidArtifactName,
		},
		{
			"absolute_path",
			outsidePath,
			ErrInvalidArtifactName,
		},
		{
			"symlink_escape",
			"escape-link.txt",
			ErrArtifactUnavailable,
		},
		{
			"directory",
			"directory",
			ErrNotRegular,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := h.Run(
				context.Background(), tt.path,
			)

			if !errors.Is(err, tt.want) {
				t.Fatalf(
					"expected %v, got %v",
					tt.want, err,
				)
			}

			if result != (HashResult{}) {
				t.Fatal("rejected input returned hash")
			}

			if runCalls != 0 {
				t.Fatal("runner executed for forbidden path")
			}

			t.Logf(
				"%s: rejected=true process_not_started=true",
				tt.name,
			)
		})
	}
}

func TestHashArtifactInjectionStrings(t *testing.T) {
	rootPath := t.TempDir()
	marker := filepath.Join(
		t.TempDir(),
		"should-not-exist",
	)

	if err := os.WriteFile(
		filepath.Join(rootPath, "alpha.txt"),
		[]byte("lab06 fixture alpha\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	reader, err := NewArtifactReader(
		rootPath, ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	h, err := NewHashArtifact(
		reader, 2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Эти строки остаются обычными именами файлов.
	// Никакая командная оболочка не запускается.
	inputs := []struct {
		name string
		path string
	}{
		{
			"semicolon",
			"alpha.txt; touch " + marker,
		},
		{
			"command_substitution",
			"$(touch " + marker + ")",
		},
	}

	for _, tt := range inputs {
		t.Run(tt.name, func(t *testing.T) {
			result, err := h.Run(
				context.Background(),
				tt.path,
			)

			if err == nil {
				t.Fatal("unexpected success for invalid artifact")
			}

			if result != (HashResult{}) {
				t.Fatal("rejected artifact returned a digest")
			}

			if _, err := os.Stat(marker); !errors.Is(
				err, os.ErrNotExist,
			) {
				t.Fatalf(
					"marker unexpectedly exists: %v",
					err,
				)
			}

			t.Logf(
				"%s: injection_not_executed=true marker_absent=true",
				tt.name,
			)
		})
	}
}
