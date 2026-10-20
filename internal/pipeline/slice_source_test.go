package pipeline_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

func TestSliceSourceOrder(t *testing.T) {
	source := pipeline.NewSliceSource([]events.Event{
		{ID: "demo_1"},
		{ID: "demo_2"},
	})

	for _, want := range []string{"demo_1", "demo_2"} {
		got, err := source.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got.ID != want {
			t.Fatalf("got ID %q, want %q", got.ID, want)
		}
	}
}

func TestSliceSourceEOF(t *testing.T) {
	source := pipeline.NewSliceSource([]events.Event{
		{ID: "demo_1"},
		{ID: "demo_2"},
	})

	for i := 0; i < 2; i++ {
		_, err := source.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	for i := 0; i < 2; i++ {
		_, err := source.Next(context.Background())
		if !errors.Is(err, io.EOF) {
			t.Fatalf("want io.EOF, got %v", err)
		}
	}
}

func TestSliceSourceCopiesInput(t *testing.T) {
	input := []events.Event{
		{ID: "demo_1"},
	}

	source := pipeline.NewSliceSource(input)

	input[0].ID = "changed"

	got, err := source.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.ID != "demo_1" {
		t.Fatalf("got ID %q, want demo_1", got.ID)
	}
}

func TestSliceSourceCancellation(t *testing.T) {
	source := pipeline.NewSliceSource([]events.Event{
		{ID: "demo_1"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := source.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	got, err := source.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.ID != "demo_1" {
		t.Fatalf("got ID %q, want demo_1", got.ID)
	}
}
