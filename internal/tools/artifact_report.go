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
)

var ErrInvalidArtifactReport = errors.New("invalid artifact report")

type ArtifactReport struct {
	EventID  string `json:"event_id"`
	Artifact string `json:"artifact"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	Status   string `json:"status"`
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

	r.reportMu.Lock()
	defer r.reportMu.Unlock()

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
