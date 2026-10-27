package tools

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

var ErrInvalidArtifactReport = errors.New("invalid artifact report")

type ArtifactReport struct {
	EventID  string      `json:"event_id"`
	Artifact string      `json:"artifact"`
	Bytes    int64       `json:"bytes"`
	SHA256   string      `json:"sha256"`
	Status   string      `json:"status"`
	Hash     *HashResult `json:"hash_result,omitempty"`
}

func (r *ArtifactReader) WriteReport(
	ctx context.Context,
	report ArtifactReport,
) error {
	return r.writeReport(ctx, report, nil)
}

// step используется только для контролируемой проверки ошибок.

func (r *ArtifactReader) writeReport(
	ctx context.Context,
	report ArtifactReport,
	step func(string) error,
) error {
	r.reportMu.Lock()
	defer r.reportMu.Unlock()

	return r.writeReportLocked(ctx, report, step)
}

// Вызывается только при уже захваченном reportMu.
func (r *ArtifactReader) writeReportLocked(
	ctx context.Context,
	report ArtifactReport,
	step func(string) error,
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	if report.EventID == "" ||
		report.Status == "" ||
		report.Bytes < 0 ||
		validateArtifactName(report.Artifact) != nil {
		return ErrInvalidArtifactReport
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	data = append(data, '\n')

	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return fmt.Errorf("generate temporary name: %w", err)
	}

	tempName := ".report-" +
		hex.EncodeToString(randomBytes[:]) +
		".tmp"

	f, err := r.root.OpenFile(
		tempName,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if err != nil {
		return fmt.Errorf("create temporary report: %w", err)
	}

	published := false
	defer func() {
		_ = f.Close()

		if !published {
			_ = r.root.Remove(tempName)
		}
	}()

	if step != nil {
		if err := step("before_write"); err != nil {
			return err
		}
	}

	n, err := f.Write(data)
	if err != nil {
		return fmt.Errorf("write temporary report: %w", err)
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	if step != nil {
		if err := step("after_write"); err != nil {
			return err
		}
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync temporary report: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close temporary report: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if step != nil {
		if err := step("before_rename"); err != nil {
			return err
		}
	}

	if err := r.root.Rename(tempName, "report.json"); err != nil {
		return fmt.Errorf("publish report: %w", err)
	}

	published = true
	return nil
}

var ErrHashReportMismatch = errors.New("hash report mismatch")

func (r *ArtifactReader) UpdateHashReport(
	ctx context.Context,
	result HashResult,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if validateArtifactName(result.Path) != nil ||
		result.Bytes < 0 ||
		result.Bytes > r.maxBytes {
		return ErrHashReportMismatch
	}

	if _, err := parseHashOutput(result.SHA256 + "  -\n"); err != nil {
		return ErrHashReportMismatch
	}

	r.reportMu.Lock()
	defer r.reportMu.Unlock()

	// Читаем существующий отчёт исключительно
	// через уже открытый доверенный os.Root.
	f, err := r.root.OpenFile(
		"report.json",
		os.O_RDONLY|syscall.O_NONBLOCK,
		0,
	)
	if err != nil {
		return ErrHashReportMismatch
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat report: %w", err)
	}

	const maxReportBytes = 16 * 1024

	if !info.Mode().IsRegular() ||
		info.Size() < 0 ||
		info.Size() > maxReportBytes {
		return ErrHashReportMismatch
	}

	data, err := io.ReadAll(
		io.LimitReader(f, maxReportBytes+1),
	)
	if err != nil {
		return fmt.Errorf("read report: %w", err)
	}

	if len(data) > maxReportBytes {
		return ErrHashReportMismatch
	}

	var report ArtifactReport
	if err := json.Unmarshal(data, &report); err != nil {
		return ErrHashReportMismatch
	}

	// Хеш должен относиться именно к артефакту,
	// указанному в существующем отчёте.
	if report.EventID == "" ||
		report.Status != "ok" ||
		report.Artifact != result.Path ||
		report.Bytes != result.Bytes {
		return ErrHashReportMismatch
	}

	if report.SHA256 != "" &&
		report.SHA256 != result.SHA256 {
		return ErrHashReportMismatch
	}

	// Сохраняем все прежние поля, добавляя hash_result.
	report.Hash = &result

	return r.writeReportLocked(ctx, report, nil)
}
