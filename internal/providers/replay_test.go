package providers_test

import (
	"context"
	"errors"
	"testing"

	"example.org/auguard/internal/providers"
)

func TestReplayModelResponse(t *testing.T) {
	model := providers.ReplayModel{
		Text: "prepared response",
	}

	got, err := model.Complete(
		context.Background(),
		providers.Request{Prompt: "hello"},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.Text != "prepared response" {
		t.Fatalf("got %q, want %q", got.Text, "prepared response")
	}
}

func TestReplayModelCancellation(t *testing.T) {
	model := providers.ReplayModel{
		Text: "prepared response",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := model.Complete(
		ctx,
		providers.Request{Prompt: "hello"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	if got != (providers.Response{}) {
		t.Fatalf("want zero Response, got %+v", got)
	}
}
