package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

func TestRuleHandlerSeverity(t *testing.T) {
	tests := []struct {
		name     string
		severity int
		action   string
		reason   string
	}{
		{"severity_2", 2, "observe", "low_severity"},
		{"severity_6", 6, "observe", "low_severity"},
		{"severity_7", 7, "review", "high_severity"},
		{"severity_9", 9, "review", "high_severity"},
	}

	handler := pipeline.RuleHandler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := events.Event{
				ID:       "demo_1",
				Kind:     "auth",
				Severity: tt.severity,
			}

			got, err := handler.Handle(context.Background(), event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.EventID != event.ID {
				t.Fatalf("EventID = %q, want %q", got.EventID, event.ID)
			}

			if got.Action != tt.action || got.Reason != tt.reason {
				t.Fatalf("got action=%q reason=%q, want action=%q reason=%q",
					got.Action, got.Reason, tt.action, tt.reason)
			}
		})
	}
}

func TestRuleHandlerInvalidEvent(t *testing.T) {
	handler := pipeline.RuleHandler{}

	event := events.Event{
		ID:       "demo_1",
		Kind:     "auth",
		Severity: 99,
	}

	got, err := handler.Handle(context.Background(), event)

	if got != (events.Decision{}) {
		t.Fatalf("want zero Decision, got %+v", got)
	}

	if !errors.Is(err, events.ErrInvalidEvent) {
		t.Fatalf("want ErrInvalidEvent, got %v", err)
	}

	var fieldErr *events.ValidationError
	if !errors.As(err, &fieldErr) || fieldErr.Field != "severity" {
		t.Fatalf("want severity field error, got %v", err)
	}
}

func TestRuleHandlerCancellation(t *testing.T) {
	handler := pipeline.RuleHandler{}

	event := events.Event{
		ID:       "demo_1",
		Kind:     "auth",
		Severity: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := handler.Handle(ctx, event)

	if got != (events.Decision{}) {
		t.Fatalf("want zero Decision, got %+v", got)
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
