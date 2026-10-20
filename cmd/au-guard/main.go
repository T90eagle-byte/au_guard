package main

import (
	"context"
	"fmt"
	"os"

	"example.org/auguard/internal/config"
	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

func run() int {
	cfg := config.Default()

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid config")
		return 2
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		cfg.Timeout,
	)
	defer cancel()

	source := pipeline.NewSliceSource([]events.Event{
		{ID: "demo_1", Kind: "auth", Severity: 2, Message: "fixture"},
		{ID: "demo_2", Kind: "file", Severity: 7, Message: "fixture"},
	})

	if err := process(ctx, source, pipeline.RuleHandler{}, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "processing failed")
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
