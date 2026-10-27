package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type ReadArtifactTool struct {
	reader *ArtifactReader
}

type readArtifactRequest struct {
	EventID  string `json:"event_id"`
	Artifact string `json:"artifact"`
}

func NewReadArtifactTool(reader *ArtifactReader) *ReadArtifactTool {
	return &ReadArtifactTool{reader: reader}
}

func (t *ReadArtifactTool) Name() string {
	return "read_artifact"
}

func (t *ReadArtifactTool) Run(
	ctx context.Context,
	args json.RawMessage,
) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if t.reader == nil {
		return nil, ErrArtifactRoot
	}

	var request readArtifactRequest
	if err := json.Unmarshal(args, &request); err != nil {
		return nil, fmt.Errorf("decode read_artifact arguments: %w", err)
	}

	if request.EventID == "" || len(request.EventID) > 128 {
		return nil, ErrInvalidArtifactReport
	}

	data, err := t.reader.Read(ctx, request.Artifact)
	if err != nil {
		return nil, err
	}

	digest := sha256.Sum256(data)

	report := ArtifactReport{
		EventID:  request.EventID,
		Artifact: request.Artifact,
		Bytes:    int64(len(data)),
		SHA256:   hex.EncodeToString(digest[:]),
		Status:   "ok",
	}

	result, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("encode read_artifact result: %w", err)
	}

	if err := t.reader.WriteReport(ctx, report); err != nil {
		return nil, err
	}

	return result, nil
}

var _ Tool = (*ReadArtifactTool)(nil)
