package tools_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"example.org/auguard/internal/tools"
)

func TestArtifactReaderFixtures(t *testing.T) {
	reader, err := tools.NewArtifactReader(
		"../../testdata/artifacts",
		tools.ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	tests := []struct {
		name string
		want string
	}{
		{"alpha.txt", "lab06 fixture alpha\n"},
		{"nested/beta.txt", "lab06 fixture beta\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := reader.Read(
				context.Background(),
				tt.name,
			)
			if err != nil {
				t.Fatalf("read %s: %v", tt.name, err)
			}

			if string(data) != tt.want {
				t.Fatalf("unexpected content: %q", data)
			}

			t.Logf(
				"artifact=%s bytes=%d content=%q",
				tt.name,
				len(data),
				string(data),
			)
		})
	}
}

func TestArtifactReaderInvalidNames(t *testing.T) {
	reader, err := tools.NewArtifactReader(
		"../../testdata/artifacts",
		tools.ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	names := []string{
		"",
		"../outside.txt",
		"nested/../../outside.txt",
		"/tmp/outside.txt",
		"./alpha.txt",
		"nested\\beta.txt",
		"nested//beta.txt",
	}

	for _, name := range names {
		_, err := reader.Read(context.Background(), name)

		if !errors.Is(err, tools.ErrInvalidArtifactName) {
			t.Fatalf(
				"name=%q: expected invalid name, got %v",
				name, err,
			)
		}
	}

	t.Logf("rejected %d invalid names", len(names))
}

func newLinkFixtureReader(t *testing.T) (
	*tools.ArtifactReader,
	string,
) {
	t.Helper()

	base := t.TempDir()
	rootPath := filepath.Join(base, "artifacts")

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
		filepath.Join(base, "outside.txt"),
		[]byte("outside fixture only\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(
		"alpha.txt",
		filepath.Join(rootPath, "inside-link.txt"),
	); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(
		"../outside.txt",
		filepath.Join(rootPath, "escape-link.txt"),
	); err != nil {
		t.Fatal(err)
	}

	reader, err := tools.NewArtifactReader(
		rootPath,
		tools.ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close root: %v", err)
		}
	})

	return reader, filepath.Join(base, "outside.txt")
}

func TestArtifactReaderInsideSymlink(t *testing.T) {
	reader, _ := newLinkFixtureReader(t)

	data, err := reader.Read(
		context.Background(),
		"inside-link.txt",
	)
	if err != nil {
		t.Fatalf("inside symlink rejected: %v", err)
	}

	if string(data) != "lab06 fixture alpha\n" {
		t.Fatalf("unexpected content: %q", data)
	}

	t.Logf(
		"inside-link.txt: allowed, bytes=%d",
		len(data),
	)
}

func TestArtifactReaderEscapeSymlink(t *testing.T) {
	reader, outsidePath := newLinkFixtureReader(t)

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "escape_symlink",
			path:    "escape-link.txt",
			wantErr: tools.ErrArtifactUnavailable,
		},
		{
			name:    "absolute_outside",
			path:    outsidePath,
			wantErr: tools.ErrInvalidArtifactName,
		},
		{
			name:    "missing_file",
			path:    "missing.txt",
			wantErr: tools.ErrArtifactUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := reader.Read(
				context.Background(),
				tt.path,
			)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf(
					"want %v, got %v",
					tt.wantErr, err,
				)
			}

			if data != nil {
				t.Fatal("rejected path returned data")
			}

			t.Log("access denied; no content returned")
		})
	}
}

func TestArtifactReaderTypesAndSize(t *testing.T) {
	rootPath := t.TempDir()

	if err := os.Mkdir(
		filepath.Join(rootPath, "directory"), 0700,
	); err != nil {
		t.Fatal(err)
	}

	if err := syscall.Mkfifo(
		filepath.Join(rootPath, "blocked.fifo"), 0600,
	); err != nil {
		t.Fatal(err)
	}

	exactData := bytes.Repeat([]byte("A"), 65536)
	oversizeData := bytes.Repeat([]byte("B"), 65537)

	if err := os.WriteFile(
		filepath.Join(rootPath, "exact.bin"),
		exactData, 0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(rootPath, "oversize.bin"),
		oversizeData, 0600,
	); err != nil {
		t.Fatal(err)
	}

	reader, err := tools.NewArtifactReader(
		rootPath,
		tools.ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	t.Run("directory_rejected", func(t *testing.T) {
		data, err := reader.Read(
			context.Background(), "directory",
		)

		if !errors.Is(err, tools.ErrNotRegular) {
			t.Fatalf("expected ErrNotRegular, got %v", err)
		}
		if data != nil {
			t.Fatal("directory returned data")
		}

		t.Log("directory rejected: ErrNotRegular")
	})

	t.Run("fifo_nonblocking", func(t *testing.T) {
		start := time.Now()

		data, err := reader.Read(
			context.Background(), "blocked.fifo",
		)

		elapsed := time.Since(start)

		if !errors.Is(err, tools.ErrNotRegular) {
			t.Fatalf("expected ErrNotRegular, got %v", err)
		}
		if data != nil {
			t.Fatal("FIFO returned data")
		}
		if elapsed > 2*time.Second {
			t.Fatalf("FIFO read took too long: %s", elapsed)
		}

		t.Logf(
			"FIFO rejected without writer; elapsed=%s",
			elapsed,
		)
	})

	t.Run("exact_65536_allowed", func(t *testing.T) {
		data, err := reader.Read(
			context.Background(), "exact.bin",
		)
		if err != nil {
			t.Fatalf("read exact file: %v", err)
		}

		if !bytes.Equal(data, exactData) {
			t.Fatal("exact file content mismatch")
		}

		t.Logf(
			"file size=%d bytes: allowed",
			len(data),
		)
	})

	t.Run("oversize_65537_rejected", func(t *testing.T) {
		data, err := reader.Read(
			context.Background(), "oversize.bin",
		)

		if !errors.Is(err, tools.ErrArtifactTooLarge) {
			t.Fatalf(
				"expected ErrArtifactTooLarge, got %v",
				err,
			)
		}
		if data != nil {
			t.Fatal("oversize file returned partial data")
		}

		t.Log("file size=65537 bytes: rejected; no data returned")
	})
}

func TestArtifactReaderNegativeExperiment(t *testing.T) {
	reader, outsidePath := newLinkFixtureReader(t)

	rootPath := filepath.Join(
		filepath.Dir(outsidePath),
		"artifacts",
	)

	// Ссылка выглядит как безопасное относительное имя.
	userPath := "escape-link.txt"
	cleanedPath := filepath.Clean(userPath)

	if cleanedPath != userPath {
		t.Fatalf("unexpected cleaned path: %q", cleanedPath)
	}

	// НЕБЕЗОПАСНЫЙ ВАРИАНТ:
	// filepath.Clean + filepath.Join + os.Open.
	// Только для контролируемого учебного эксперимента.
	f, err := os.Open(
		filepath.Join(rootPath, cleanedPath),
	)
	if err != nil {
		t.Fatalf("naive open: %v", err)
	}
	defer f.Close()

	openedInfo, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	outsideInfo, err := os.Stat(outsidePath)
	if err != nil {
		t.Fatal(err)
	}

	if !os.SameFile(openedInfo, outsideInfo) {
		t.Fatal("naive open did not reach outside file")
	}

	t.Log(
		"NAIVE Clean+Join+Open: escaped_root=true",
	)

	// БЕЗОПАСНЫЙ ВАРИАНТ:
	// разрешение имени через уже открытый os.Root.
	data, err := reader.Read(
		context.Background(),
		userPath,
	)

	if !errors.Is(err, tools.ErrArtifactUnavailable) {
		t.Fatalf(
			"expected root boundary rejection, got %v",
			err,
		)
	}

	if data != nil {
		t.Fatal("rejected path returned content")
	}

	t.Log(
		"SAFE os.Root: escaped_root=false access_denied=true",
	)
	t.Log(
		"outside_content_not_read=true",
	)
}

func TestArtifactOpenRegular(t *testing.T) {
	reader, outsidePath := newLinkFixtureReader(t)

	rootPath := filepath.Join(
		filepath.Dir(outsidePath),
		"artifacts",
	)

	if err := os.Mkdir(
		filepath.Join(rootPath, "nested"), 0700,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(rootPath, "nested", "beta.txt"),
		[]byte("lab06 fixture beta\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(rootPath, "exact.bin"),
		bytes.Repeat([]byte("A"), 65536),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(rootPath, "oversize.bin"),
		bytes.Repeat([]byte("B"), 65537),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(
		filepath.Join(rootPath, "directory"), 0700,
	); err != nil {
		t.Fatal(err)
	}

	if err := syscall.Mkfifo(
		filepath.Join(rootPath, "blocked.fifo"), 0600,
	); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	allowed := []struct {
		name string
		size int64
	}{
		{"alpha.txt", 20},
		{"nested/beta.txt", 19},
		{"inside-link.txt", 20},
		{"exact.bin", 65536},
	}

	for _, tt := range allowed {
		f, size, err := reader.OpenRegular(ctx, tt.name)
		if err != nil {
			t.Fatalf("open %s: %v", tt.name, err)
		}

		data, readErr := io.ReadAll(f)
		closeErr := f.Close()

		if readErr != nil || closeErr != nil {
			t.Fatalf(
				"read/close %s: %v / %v",
				tt.name, readErr, closeErr,
			)
		}

		if size != tt.size ||
			int64(len(data)) != tt.size {
			t.Fatalf(
				"%s: reported=%d actual=%d expected=%d",
				tt.name, size, len(data), tt.size,
			)
		}

		t.Logf(
			"ALLOW %s: descriptor_opened=true bytes=%d",
			tt.name, size,
		)
	}

	denied := []struct {
		name string
		want error
	}{
		{"../outside.txt", tools.ErrInvalidArtifactName},
		{outsidePath, tools.ErrInvalidArtifactName},
		{"escape-link.txt", tools.ErrArtifactUnavailable},
		{"directory", tools.ErrNotRegular},
		{"blocked.fifo", tools.ErrNotRegular},
		{"oversize.bin", tools.ErrArtifactTooLarge},
	}

	for _, tt := range denied {
		start := time.Now()

		f, _, err := reader.OpenRegular(ctx, tt.name)
		elapsed := time.Since(start)

		if f != nil {
			_ = f.Close()
			t.Fatalf("%s: unexpected open descriptor", tt.name)
		}

		if !errors.Is(err, tt.want) {
			t.Fatalf(
				"%s: expected %v, got %v",
				tt.name, tt.want, err,
			)
		}

		if elapsed > time.Second {
			t.Fatalf("%s: slow rejection: %s", tt.name, elapsed)
		}

		t.Logf(
			"DENY %s: error=%v descriptor=nil",
			tt.name, err,
		)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	f, _, err := reader.OpenRegular(canceled, "alpha.txt")
	if f != nil || !errors.Is(err, context.Canceled) {
		if f != nil {
			_ = f.Close()
		}
		t.Fatalf("canceled context: file=%v err=%v", f, err)
	}

	t.Log("DENY canceled_context: descriptor=nil")
}
