package events_test

import (
	"errors"
	"strings"
	"testing"

	"example.org/auguard/internal/events"
)

func TestMessageLimit(t *testing.T) {
	e := events.Event{ID: "demo_1", Kind: "auth", Severity: 2}

	e.Message = strings.Repeat("a", 4096)
	if err := e.Validate(); err != nil {
		t.Fatalf("4096 bytes must be valid: %v", err)
	}

	e.Message += "a"
	err := e.Validate()

	if !errors.Is(err, events.ErrInvalidEvent) {
		t.Fatalf("want invalid event, got %v", err)
	}

	var fieldErr *events.ValidationError
	if !errors.As(err, &fieldErr) || fieldErr.Field != "message" {
		t.Fatalf("want message field error, got %v", err)
	}
}

func TestEventValidationBounds(t *testing.T) {
	tests := []struct {
		name     string
		change   func(*events.Event)
		badField string
	}{
		// ID
		{"id_empty", func(e *events.Event) { e.ID = "" }, "id"},
		{"id_64", func(e *events.Event) { e.ID = strings.Repeat("a", 64) }, ""},
		{"id_65", func(e *events.Event) { e.ID = strings.Repeat("a", 65) }, "id"},
		{"id_space", func(e *events.Event) { e.ID = "demo 1" }, "id"},
		{"id_cyrillic", func(e *events.Event) { e.ID = "демо" }, "id"},

		// Kind
		{"kind_auth", func(e *events.Event) { e.Kind = "auth" }, ""},
		{"kind_file", func(e *events.Event) { e.Kind = "file" }, ""},
		{"kind_process", func(e *events.Event) { e.Kind = "process" }, ""},
		{"kind_unknown", func(e *events.Event) { e.Kind = "network" }, "kind"},

		// Severity
		{"severity_0", func(e *events.Event) { e.Severity = 0 }, ""},
		{"severity_10", func(e *events.Event) { e.Severity = 10 }, ""},
		{"severity_negative", func(e *events.Event) { e.Severity = -1 }, "severity"},
		{"severity_11", func(e *events.Event) { e.Severity = 11 }, "severity"},

		// Message
		{"message_empty", func(e *events.Event) { e.Message = "" }, ""},
		{"message_4096", func(e *events.Event) { e.Message = strings.Repeat("a", 4096) }, ""},
		{"message_4097", func(e *events.Event) { e.Message = strings.Repeat("a", 4097) }, "message"},
		{"message_2048_cyrillic", func(e *events.Event) { e.Message = strings.Repeat("я", 2048) }, ""},
		{"message_2049_cyrillic", func(e *events.Event) { e.Message = strings.Repeat("я", 2049) }, "message"},
		{"message_bad_utf8", func(e *events.Event) { e.Message = string([]byte{0xff}) }, "message"},

		// Artifact
		{"artifact_empty", func(e *events.Event) { e.Artifact = "" }, ""},
		{"artifact_valid", func(e *events.Event) { e.Artifact = "reports/a.json" }, ""},
		{"artifact_256", func(e *events.Event) { e.Artifact = strings.Repeat("a", 256) }, ""},
		{"artifact_257", func(e *events.Event) { e.Artifact = strings.Repeat("a", 257) }, "artifact"},
		{"artifact_absolute", func(e *events.Event) { e.Artifact = "/tmp/a" }, "artifact"},
		{"artifact_parent", func(e *events.Event) { e.Artifact = ".." }, "artifact"},
		{"artifact_parent_prefix", func(e *events.Event) { e.Artifact = "../a" }, "artifact"},
		{"artifact_parent_middle", func(e *events.Event) { e.Artifact = "a/../b" }, "artifact"},
		{"artifact_dots_in_name", func(e *events.Event) { e.Artifact = "a..b" }, ""},
		{"artifact_backslash", func(e *events.Event) { e.Artifact = "a\\b" }, "artifact"},
		{"artifact_nul", func(e *events.Event) { e.Artifact = "a\x00b" }, "artifact"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := events.Event{
				ID:       "demo_1",
				Kind:     "auth",
				Severity: 2,
				Message:  "fixture",
			}

			tt.change(&e)
			err := e.Validate()

			if tt.badField == "" {
				if err != nil {
					t.Fatalf("expected valid event, got %v", err)
				}
				return
			}

			if !errors.Is(err, events.ErrInvalidEvent) {
				t.Fatalf("want ErrInvalidEvent, got %v", err)
			}

			var fieldErr *events.ValidationError
			if !errors.As(err, &fieldErr) {
				t.Fatalf("want ValidationError, got %v", err)
			}

			if fieldErr.Field != tt.badField {
				t.Fatalf("got field %q, want %q",
					fieldErr.Field, tt.badField)
			}
		})
	}
}

func TestValidationErrorDoesNotLeakMessage(t *testing.T) {
	e := events.Event{
		ID:       "demo_1",
		Kind:     "auth",
		Severity: 99,
		Message:  "PRIVATE_TEST_MARKER",
	}

	err := e.Validate()

	if !errors.Is(err, events.ErrInvalidEvent) {
		t.Fatalf("want ErrInvalidEvent, got %v", err)
	}

	if strings.Contains(err.Error(), "PRIVATE_TEST_MARKER") {
		t.Fatal("validation error contains user message")
	}
}
