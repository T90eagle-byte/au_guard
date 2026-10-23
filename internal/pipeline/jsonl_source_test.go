package pipeline_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

const validJSON = `{"id":"demo_1","kind":"auth","severity":2,"message":"fixture"}`
const secondJSON = `{"id":"demo_2","kind":"file","severity":7,"message":"fixture"}`

func newJSONLSource(input string, maxLine, maxEvents int) *pipeline.JSONLSource {
	return pipeline.NewJSONLSource(
		strings.NewReader(input),
		maxLine,
		maxEvents,
	)
}

func TestJSONLSourceOrderAndEOF(t *testing.T) {
	source := newJSONLSource(
		validJSON+"\n"+secondJSON+"\n",
		65536, 1000,
	)

	for _, want := range []string{"demo_1", "demo_2"} {
		got, err := source.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.ID != want {
			t.Fatalf("got ID %q, want %q", got.ID, want)
		}
	}

	for i := 0; i < 2; i++ {
		_, err := source.Next(context.Background())
		if !errors.Is(err, io.EOF) {
			t.Fatalf("want io.EOF, got %v", err)
		}
	}
}

func TestJSONLSourceStrictJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{
			"unknown_field",
			`{"id":"demo_1","kind":"auth","severity":2,"message":"fixture","severty":2}` + "\n",
			pipeline.ErrInvalidJSONL,
		},
		{
			"malformed_json",
			`{"id":` + "\n",
			pipeline.ErrInvalidJSONL,
		},
		{
			"two_objects",
			validJSON + validJSON + "\n",
			pipeline.ErrTrailingJSON,
		},
		{
			"trailing_garbage",
			validJSON + " garbage\n",
			pipeline.ErrTrailingJSON,
		},
		{
			"invalid_event",
			`{"id":"demo_1","kind":"auth","severity":11,"message":"fixture"}` + "\n",
			events.ErrInvalidEvent,
		},
		{
			"empty_line",
			"\n",
			pipeline.ErrInvalidJSONL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := newJSONLSource(tt.input, 65536, 1000)

			got, err := source.Next(context.Background())
			if !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}

			if got != (events.Event{}) {
				t.Fatalf("want zero Event, got %+v", got)
			}
		})
	}
}

func TestJSONLSourceLineLimit(t *testing.T) {
	const limit = 65536

	line := validJSON + strings.Repeat(" ", limit-len(validJSON))

	for _, tt := range []struct {
		name   string
		ending string
	}{
		{"exact_limit_LF", "\n"},
		{"exact_limit_CRLF", "\r\n"},
		{"exact_limit_no_LF", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := newJSONLSource(line+tt.ending, limit, 1000)

			got, err := source.Next(context.Background())
			if err != nil {
				t.Fatalf("65536 bytes must be accepted: %v", err)
			}
			if got.ID != "demo_1" {
				t.Fatalf("unexpected ID: %q", got.ID)
			}

			_, err = source.Next(context.Background())
			if !errors.Is(err, io.EOF) {
				t.Fatalf("want io.EOF, got %v", err)
			}
		})
	}

	t.Run("over_limit_65537", func(t *testing.T) {
		source := newJSONLSource(line+" \n", limit, 1000)

		_, err := source.Next(context.Background())
		if !errors.Is(err, pipeline.ErrLineTooLong) {
			t.Fatalf("want ErrLineTooLong, got %v", err)
		}
	})
}

func TestJSONLSourceMaxEvents(t *testing.T) {
	source := newJSONLSource(
		validJSON+"\n"+secondJSON+"\n",
		65536, 1,
	)

	first, err := source.Next(context.Background())
	if err != nil || first.ID != "demo_1" {
		t.Fatalf("first event: got %+v, error %v", first, err)
	}

	_, err = source.Next(context.Background())
	if !errors.Is(err, pipeline.ErrTooManyEvents) {
		t.Fatalf("want ErrTooManyEvents, got %v", err)
	}
}

func TestJSONLSourceCancellation(t *testing.T) {
	source := newJSONLSource(validJSON+"\n", 65536, 1000)

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
