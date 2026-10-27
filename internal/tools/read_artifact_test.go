package tools_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"example.org/auguard/internal/tools"
)

func TestReadArtifactToolEntry(t *testing.T) {
	rootPath := t.TempDir()

	fixture, err := os.ReadFile(
		"../../testdata/artifacts/alpha.txt",
	)
	if err != nil {
		t.Fatal(err)
	}

	artifactPath := filepath.Join(rootPath, "alpha.txt")

	if err := os.WriteFile(artifactPath, fixture, 0600); err != nil {
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

	tool := tools.NewReadArtifactTool(reader)

	if tool.Name() != "read_artifact" {
		t.Fatalf("unexpected tool name: %q", tool.Name())
	}

	args := json.RawMessage(
		`{"event_id":"demo_1","artifact":"alpha.txt"}`,
	)

	result, err := tool.Run(context.Background(), args)
	if err != nil {
		t.Fatalf("run read_artifact: %v", err)
	}

	var report tools.ArtifactReport
	if err := json.Unmarshal(result, &report); err != nil {
		t.Fatal(err)
	}

	expectedDigest := sha256.Sum256(
		[]byte("lab06 fixture alpha\n"),
	)
	wantHash := fmt.Sprintf("%x", expectedDigest)

	if report.Bytes != 20 ||
		report.SHA256 != wantHash ||
		report.EventID != "demo_1" ||
		report.Artifact != "alpha.txt" ||
		report.Status != "ok" {
		t.Fatalf("unexpected result: %+v", report)
	}

	storedData, err := os.ReadFile(
		filepath.Join(rootPath, "report.json"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var stored tools.ArtifactReport
	if err := json.Unmarshal(storedData, &stored); err != nil {
		t.Fatal(err)
	}

	if stored != report {
		t.Fatalf("stored report differs: %+v", stored)
	}

	if bytes.Contains(storedData, fixture) {
		t.Fatal("report contains artifact body")
	}

	after, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(after, fixture) {
		t.Fatal("read_artifact modified the source file")
	}

	t.Logf(
		"read_artifact: bytes=%d sha256=%s status=%s",
		report.Bytes, report.SHA256, report.Status,
	)
	t.Log("artifact_unchanged=true report_valid=true body_absent=true")

	// Недопустимый путь не должен заменить успешный отчёт.
	_, err = tool.Run(
		context.Background(),
		json.RawMessage(
			`{"event_id":"demo_2","artifact":"../outside.txt"}`,
		),
	)
	if !errors.Is(err, tools.ErrInvalidArtifactName) {
		t.Fatalf("expected invalid path error, got %v", err)
	}

	storedAfter, err := os.ReadFile(
		filepath.Join(rootPath, "report.json"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(storedAfter, storedData) {
		t.Fatal("invalid path modified the previous report")
	}

	t.Log("invalid_path=rejected previous_report=preserved")
}

func TestReadArtifactFixedRootDemo(t *testing.T) {
	reader, err := tools.NewArtifactReader(
		"../../testdata/artifacts",
		tools.ArtifactMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	data, err := reader.Read(
		context.Background(),
		"nested/beta.txt",
	)
	if err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256(data)

	if string(data) != "lab06 fixture beta\n" {
		t.Fatal("fixture content mismatch")
	}

	t.Logf(
		"demo: root=testdata/artifacts artifact=nested/beta.txt bytes=%d sha256=%x",
		len(data), digest,
	)
}
