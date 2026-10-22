package tools

import (
	"context"
	"encoding/json"
)

type StubTool struct {
	name   string
	result json.RawMessage
}

func NewStubTool(name string, result json.RawMessage) *StubTool {
	owned := append(json.RawMessage(nil), result...)

	return &StubTool{
		name:   name,
		result: owned,
	}
}

func (s *StubTool) Name() string {
	return s.name
}

func (s *StubTool) Run(
	ctx context.Context,
	_ json.RawMessage,
) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result := append(json.RawMessage(nil), s.result...)
	return result, nil
}

var _ Tool = (*StubTool)(nil)
