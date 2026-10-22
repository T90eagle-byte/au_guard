package tools_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"example.org/auguard/internal/tools"
)

func TestStubToolReturnsCopy(t *testing.T) {
	prepared := json.RawMessage(`{"status":"ok"}`)
	tool := tools.NewStubTool("demo_tool", prepared)

	if tool.Name() != "demo_tool" {
		t.Fatalf("unexpected tool name: %q", tool.Name())
	}

	first, err := tool.Run(
		context.Background(),
		json.RawMessage(`{"input":1}`),
	)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	want := []byte(`{"status":"ok"}`)

	if !bytes.Equal(first, want) {
		t.Fatalf("first result = %s, want %s", first, want)
	}

	// Изменяем результат первого вызова.
	first[0] = 'X'

	second, err := tool.Run(
		context.Background(),
		json.RawMessage(`{"input":2}`),
	)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	if !bytes.Equal(second, want) {
		t.Fatalf("second result = %s, want %s", second, want)
	}
}

func TestStubToolCancellation(t *testing.T) {
	tool := tools.NewStubTool(
		"demo_tool",
		json.RawMessage(`{"status":"ok"}`),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := tool.Run(ctx, nil)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	if got != nil {
		t.Fatalf("want nil result, got %s", got)
	}
}
