package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"example.org/auguard/internal/config"
	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

var errWriteDecision = errors.New("write decision")

func reportError(err error, errOut io.Writer) int {
	switch {
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(errOut, "canceled")
		return 130

	case errors.Is(err, context.DeadlineExceeded):
		fmt.Fprintln(errOut, "deadline_exceeded")
		return 5

	case errors.Is(err, pipeline.ErrInvalidJSONL),
		errors.Is(err, pipeline.ErrTrailingJSON),
		errors.Is(err, pipeline.ErrLineTooLong),
		errors.Is(err, pipeline.ErrTooManyEvents),
		errors.Is(err, events.ErrInvalidEvent):
		fmt.Fprintf(errOut, "data_error: %v\n", err)
		return 3
	case errors.Is(err, errWriteDecision):
		fmt.Fprintln(errOut, "infrastructure_error: write_decision")
		return 4

	default:
		fmt.Fprintln(errOut, "infrastructure_error: read_event")
		return 4
	}
}

func Run(
	ctx context.Context,
	args []string,
	in io.Reader,
	out, errOut io.Writer,
) int {
	if len(args) == 0 || args[0] != "ingest" {
		fmt.Fprintln(errOut, "argument_error: expected ingest")
		return 2
	}

	cfg := config.Default()

	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	inputPath := fs.String("input", "-", "input file or stdin")
	workers := fs.Int("workers", cfg.Workers, "workers")
	queue := fs.Int("queue", cfg.Queue, "queue size")
	maxEvents := fs.Int("max-events", cfg.MaxEvents, "event limit")
	timeout := fs.Duration("timeout", cfg.Timeout, "command timeout")

	if err := fs.Parse(args[1:]); err != nil {
		fmt.Fprintln(errOut, "argument_error: invalid flag")
		return 2
	}

	if fs.NArg() != 0 || *inputPath == "" {
		fmt.Fprintln(errOut, "argument_error: unexpected argument")
		return 2
	}

	cfg.Workers = *workers
	cfg.Queue = *queue
	cfg.MaxEvents = *maxEvents
	cfg.Timeout = *timeout

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(errOut, "argument_error: invalid configuration")
		return 2
	}

	commandCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	var reader io.Reader = in

	if *inputPath != "-" {
		file, err := os.Open(*inputPath)
		if err != nil {
			fmt.Fprintln(errOut, "infrastructure_error: open_input")
			return 4
		}
		defer file.Close()
		reader = file
	}

	source := pipeline.NewJSONLSource(
		reader,
		cfg.MaxLineBytes,
		cfg.MaxEvents,
	)

	handler := pipeline.RuleHandler{}
	encoder := json.NewEncoder(out)

	stats, err := pipeline.Process(
		commandCtx,
		source,
		handler,
		cfg.Workers,
		cfg.Queue,
		func(decision events.Decision) error {
			if err := encoder.Encode(decision); err != nil {
				return fmt.Errorf("%w: %w", errWriteDecision, err)
			}
			return nil
		},
	)

	fmt.Fprintf(
		errOut,
		"read=%d succeeded=%d rejected=%d failed=%d\n",
		stats.Read,
		stats.Succeeded,
		stats.Rejected,
		stats.Failed,
	)

	if err != nil {
		return reportError(err, errOut)
	}

	return 0
}
