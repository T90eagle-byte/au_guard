package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

type testHandler struct{}

func (testHandler) Handle(
	_ context.Context,
	e events.Event,
) (events.Decision, error) {
	return events.Decision{
		EventID: e.ID,
		Action:  "observe",
		Reason:  "test_rule",
	}, nil
}

var _ pipeline.EventHandler = testHandler{}

func TestProcessReplaceableHandler(t *testing.T) {
	source := pipeline.NewSliceSource([]events.Event{
		{
			ID:       "demo_2",
			Kind:     "file",
			Severity: 7,
			Message:  "fixture",
		},
	})

	var out bytes.Buffer

	err := process(
		context.Background(),
		source,
		testHandler{},
		&out,
	)

	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	var got events.Decision
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}

	want := events.Decision{
		EventID: "demo_2",
		Action:  "observe",
		Reason:  "test_rule",
	}

	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestProcessEmptySource(t *testing.T) {
	source := pipeline.NewSliceSource(nil)

	var out bytes.Buffer

	err := process(
		context.Background(),
		source,
		testHandler{},
		&out,
	)

	if err != nil {
		t.Fatalf("empty source must succeed: %v", err)
	}

	if out.Len() != 0 {
		t.Fatalf("expected empty output, got %q", out.String())
	}
}
