package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHashArtifactReportIntegration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	source := []byte("lab06 fixture alpha\n")
	artifactPath := filepath.Join(dir, "alpha.txt")
	reportPath := filepath.Join(dir, "report.json")

	if err := os.WriteFile(artifactPath, source, 0600); err != nil {
		t.Fatal(err)
	}

	reader, err := NewArtifactReader(dir, ArtifactMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Формируем первоначальный отчёт средствами ЛР6.
	readTool := NewReadArtifactTool(reader)

	_, err = readTool.Run(
		ctx,
		json.RawMessage(
			`{"event_id":"lab07","artifact":"alpha.txt"}`,
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	hasher, err := NewHashArtifact(
		reader, 2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Дополняем отчёт результатом внешней программы.
	result, err := hasher.RunAndUpdateReport(
		ctx, "alpha.txt",
	)
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	var report ArtifactReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}

	if report.Hash == nil || *report.Hash != result {
		t.Fatalf("missing or incorrect hash result: %+v", report)
	}

	if report.EventID != "lab07" ||
		report.Artifact != "alpha.txt" ||
		report.Bytes != 20 ||
		report.SHA256 != result.SHA256 ||
		report.Status != "ok" {
		t.Fatalf("previous report fields changed: %+v", report)
	}

	info, err := os.Stat(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode().Perm()&0077 != 0 {
		t.Fatalf("unsafe report permissions: %04o", info.Mode().Perm())
	}

	after, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(after, source) {
		t.Fatal("source artifact was modified")
	}

	t.Logf(
		"report_updated=true hash_result=present bytes=%d sha256=%s",
		result.Bytes, result.SHA256,
	)
	t.Logf(
		"report_json_valid=true mode=%04o source_unchanged=true",
		info.Mode().Perm(),
	)

	// Создаём другой корректный артефакт.
	if err := os.WriteFile(
		filepath.Join(dir, "beta.txt"),
		[]byte("lab06 fixture beta\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	beforeFailure := append([]byte(nil), data...)

	// Нельзя записать результат beta.txt в отчёт alpha.txt.
	_, err = hasher.RunAndUpdateReport(ctx, "beta.txt")
	if !errors.Is(err, ErrHashReportMismatch) {
		t.Fatalf("expected report mismatch, got %v", err)
	}

	unchanged, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(unchanged, beforeFailure) {
		t.Fatal("failed update modified existing report")
	}

	t.Log("mismatched_artifact=rejected old_report_preserved=true")

	// Повреждённый JSON не должен заменяться новым.
	broken := []byte("{invalid json")
	if err := os.WriteFile(reportPath, broken, 0600); err != nil {
		t.Fatal(err)
	}

	_, err = hasher.RunAndUpdateReport(ctx, "alpha.txt")
	if !errors.Is(err, ErrHashReportMismatch) {
		t.Fatalf("expected invalid report rejection, got %v", err)
	}

	unchanged, err = os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(unchanged, broken) {
		t.Fatal("invalid report was overwritten")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if len(name) >= 8 &&
			name[:8] == ".report-" {
			t.Fatalf("temporary report remained: %s", name)
		}
	}

	t.Log("malformed_report=rejected old_content_preserved=true temp_files=0")
}

func TestHashArtifactParallelSameFixture(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	fixture := []byte("lab06 fixture alpha\n")

	if err := os.WriteFile(
		filepath.Join(dir, "alpha.txt"),
		fixture,
		0600,
	); err != nil {
		t.Fatal(err)
	}

	reader := lab06ReportReader(t, dir)

	// Создаём первоначальный отчёт средствами ЛР6.
	readTool := NewReadArtifactTool(reader)

	_, err := readTool.Run(
		ctx,
		json.RawMessage(
			`{"event_id":"parallel","artifact":"alpha.txt"}`,
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	hasher, err := NewHashArtifact(
		reader,
		HashMaxTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		result HashResult
		err    error
	}

	results := make(chan outcome, 2)
	start := make(chan struct{})

	// Два конкурентных вызова используют один ArtifactReader.
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			result, err := hasher.RunAndUpdateReport(
				ctx, "alpha.txt",
			)
			results <- outcome{result, err}
		}()
	}

	close(start)

	const expectedHash = "a5063a52898ce0d67a3ebed2dc17398783a76f42143c68a0466c22ef1c3c258b"

	for i := 0; i < 2; i++ {
		got := <-results

		if got.err != nil {
			t.Fatalf("parallel call %d: %v", i+1, got.err)
		}

		if got.result.Path != "alpha.txt" ||
			got.result.Bytes != 20 ||
			got.result.SHA256 != expectedHash {
			t.Fatalf(
				"parallel call %d: wrong result %+v",
				i+1, got.result,
			)
		}
	}

	data, err := os.ReadFile(
		filepath.Join(dir, "report.json"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var report ArtifactReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("final report invalid: %v", err)
	}

	if report.EventID != "parallel" ||
		report.Hash == nil ||
		report.Hash.Path != "alpha.txt" ||
		report.Hash.Bytes != 20 ||
		report.Hash.SHA256 != expectedHash {
		t.Fatalf("incorrect final report: %+v", report)
	}

	info, err := os.Stat(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0077 != 0 {
		t.Fatalf("unsafe report mode: %04o", info.Mode().Perm())
	}

	sourceAfter, err := os.ReadFile(
		filepath.Join(dir, "alpha.txt"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sourceAfter, fixture) {
		t.Fatal("original artifact changed")
	}

	assertNoReportTemps(t, dir)

	t.Log("parallel_calls=2 equal_sha256=true")
	t.Log("final_report_valid=true source_unchanged=true temp_files=0")
	t.Logf("report_mode=%04o", info.Mode().Perm())
}
