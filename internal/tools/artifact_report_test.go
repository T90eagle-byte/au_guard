package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func lab06Report() ArtifactReport {
	return ArtifactReport{
		EventID:  "demo_1",
		Artifact: "alpha.txt",
		Bytes:    20,
		SHA256:   "",
		Status:   "ok",
	}
}

func lab06ReportReader(t *testing.T, dir string) *ArtifactReader {
	t.Helper()

	reader, err := NewArtifactReader(dir, ArtifactMaxBytes)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close root: %v", err)
		}
	})

	return reader
}

func assertNoReportTemps(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".report-") &&
			strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("temporary file remains: %s", entry.Name())
		}
	}
}

func TestArtifactReportAtomicAndPrivate(t *testing.T) {
	dir := t.TempDir()
	reader := lab06ReportReader(t, dir)
	want := lab06Report()

	if err := reader.WriteReport(context.Background(), want); err != nil {
		t.Fatal(err)
	}

	reportPath := filepath.Join(dir, "report.json")
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	var got ArtifactReport
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("report is not valid JSON: %v", err)
	}

	if got != want {
		t.Fatalf("report mismatch: got %+v, want %+v", got, want)
	}

	if bytes.Contains(data, []byte("lab06 fixture alpha")) {
		t.Fatal("artifact body leaked into report")
	}

	info, err := os.Stat(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	mode := info.Mode().Perm()
	if mode&0077 != 0 {
		t.Fatalf("group/other permissions too broad: %04o", mode)
	}

	assertNoReportTemps(t, dir)

	t.Logf(
		"report.json: valid_json=true mode=%04o bytes=%d body_absent=true temp_files=0",
		mode, got.Bytes,
	)
}

func TestArtifactReportFailureKeepsOld(t *testing.T) {
	for _, failStage := range []string{
		"after_write",
		"before_rename",
	} {
		t.Run(failStage, func(t *testing.T) {
			dir := t.TempDir()
			reader := lab06ReportReader(t, dir)

			reportPath := filepath.Join(dir, "report.json")
			oldReport := []byte(
				"{\"event_id\":\"old\",\"status\":\"old\"}\n",
			)

			if err := os.WriteFile(
				reportPath, oldReport, 0600,
			); err != nil {
				t.Fatal(err)
			}

			injected := errors.New("injected report failure")

			err := reader.writeReport(
				context.Background(),
				lab06Report(),
				func(stage string) error {
					if stage == failStage {
						return injected
					}
					return nil
				},
			)

			if !errors.Is(err, injected) {
				t.Fatalf("expected injected failure, got %v", err)
			}

			data, err := os.ReadFile(reportPath)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data, oldReport) {
				t.Fatal("old report was modified")
			}

			if !json.Valid(data) {
				t.Fatal("old report became invalid JSON")
			}

			assertNoReportTemps(t, dir)

			t.Logf(
				"stage=%s old_report_preserved=true temp_files=0",
				failStage,
			)
		})
	}
}

func TestArtifactReportRealRenameFailure(t *testing.T) {
	dir := t.TempDir()
	reader := lab06ReportReader(t, dir)

	// Каталог с целевым именем препятствует замене файла.
	target := filepath.Join(dir, "report.json")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}

	err := reader.WriteReport(
		context.Background(),
		lab06Report(),
	)
	if err == nil {
		t.Fatal("expected rename failure")
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("existing target was modified")
	}

	assertNoReportTemps(t, dir)
	t.Log("rename rejected; existing target preserved; temp_files=0")
}
