package cli_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.org/auguard/internal/cli"
)

const (
	eventOne = `{"id":"demo_1","kind":"auth","severity":2,"message":"fixture"}`
	eventTwo = `{"id":"demo_2","kind":"file","severity":7,"message":"fixture"}`

	decisionOne = `{"event_id":"demo_1","action":"observe","reason":"low_severity"}` + "\n"
	decisionTwo = `{"event_id":"demo_2","action":"review","reason":"high_severity"}` + "\n"
)

func runCLI(
	ctx context.Context,
	args []string,
	input string,
) (int, string, string) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := cli.Run(
		ctx,
		args,
		strings.NewReader(input),
		&stdout,
		&stderr,
	)

	return code, stdout.String(), stderr.String()
}

func TestCLIValidJSONL(t *testing.T) {
	input := eventOne + "\n" + eventTwo + "\n"

	tests := []struct {
		name string
		args []string
	}{
		{"defaults", []string{"ingest"}},
		{"explicit_flags", []string{
			"ingest", "--input", "-",
			"--workers", "4",
			"--queue", "8",
			"--max-events", "1000",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runCLI(
				context.Background(), tt.args, input,
			)

			if code != 0 {
				t.Fatalf("exit code = %d, want 0", code)
			}
			if out != decisionOne+decisionTwo {
				t.Fatalf("unexpected stdout: %q", out)
			}
			if errOut != "" {
				t.Fatalf("stderr must be empty: %q", errOut)
			}
		})
	}
}

func TestCLIEmptyInput(t *testing.T) {
	code, out, errOut := runCLI(
		context.Background(),
		[]string{"ingest"},
		"",
	)

	if code != 0 || out != "" || errOut != "" {
		t.Fatalf(
			"code=%d stdout=%q stderr=%q",
			code, out, errOut,
		)
	}
}

func TestCLIRejectsBadData(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			"unknown_field",
			`{"id":"demo_1","kind":"auth","severity":2,"severty":3}` + "\n",
		},
		{
			"malformed_json",
			`{"id":` + "\n",
		},
		{
			"two_objects",
			eventOne + eventTwo + "\n",
		},
		{
			"invalid_severity",
			`{"id":"demo_1","kind":"auth","severity":11,"message":"PRIVATE_TEST_MARKER"}` + "\n",
		},
		{
			"empty_line",
			"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runCLI(
				context.Background(),
				[]string{"ingest"},
				tt.input,
			)

			if code != 3 {
				t.Fatalf("exit code = %d, want 3", code)
			}
			if out != "" {
				t.Fatalf("unexpected stdout: %q", out)
			}
			if !strings.Contains(errOut, "data_error") {
				t.Fatalf("missing data_error: %q", errOut)
			}
			if strings.Contains(errOut, "PRIVATE_TEST_MARKER") {
				t.Fatal("private message leaked into stderr")
			}
		})
	}
}

func TestCLIPartialOutput(t *testing.T) {
	input := eventOne + "\n" + `{"id":` + "\n"

	code, out, errOut := runCLI(
		context.Background(),
		[]string{"ingest"},
		input,
	)

	if code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
	if out != decisionOne {
		t.Fatalf("stdout = %q, want first decision", out)
	}
	if !strings.Contains(errOut, "line 2") {
		t.Fatalf("stderr must identify line 2: %q", errOut)
	}
	if !strings.Contains(errOut, "data_error") {
		t.Fatalf("missing data_error: %q", errOut)
	}
}

func TestCLILineLimit(t *testing.T) {
	const limit = 65536
	line := eventOne + strings.Repeat(" ", limit-len(eventOne))

	t.Run("exact_limit_65536", func(t *testing.T) {
		code, out, errOut := runCLI(
			context.Background(),
			[]string{"ingest"},
			line+"\n",
		)

		if code != 0 || out != decisionOne || errOut != "" {
			t.Fatalf(
				"code=%d stdout=%q stderr=%q",
				code, out, errOut,
			)
		}
	})

	t.Run("over_limit_65537", func(t *testing.T) {
		code, out, errOut := runCLI(
			context.Background(),
			[]string{"ingest"},
			line+" \n",
		)

		if code != 3 || out != "" {
			t.Fatalf(
				"code=%d stdout=%q, want 3 and empty stdout",
				code, out,
			)
		}
		if !strings.Contains(errOut, "JSONL line too long") {
			t.Fatalf("missing length error: %q", errOut)
		}
	})
}

func TestCLIMaxEvents(t *testing.T) {
	input := eventOne + "\n" + eventTwo + "\n"

	code, out, errOut := runCLI(
		context.Background(),
		[]string{"ingest", "--max-events", "1"},
		input,
	)

	if code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
	if out != decisionOne {
		t.Fatalf("stdout = %q, want first decision", out)
	}
	if !strings.Contains(errOut, "too many events") {
		t.Fatalf("missing event limit error: %q", errOut)
	}
}

func TestCLIArgumentErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no_command", nil},
		{"unknown_command", []string{"serve"}},
		{"unknown_flag", []string{"ingest", "--unknown"}},
		{"missing_value", []string{"ingest", "--workers"}},
		{"invalid_number", []string{"ingest", "--workers", "abc"}},
		{"workers_zero", []string{"ingest", "--workers", "0"}},
		{"workers_17", []string{"ingest", "--workers", "17"}},
		{"queue_65", []string{"ingest", "--queue", "65"}},
		{"max_events_zero", []string{"ingest", "--max-events", "0"}},
		{"max_events_1001", []string{"ingest", "--max-events", "1001"}},
		{"empty_input_path", []string{"ingest", "--input", ""}},
		{"extra_argument", []string{"ingest", "unexpected"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runCLI(
				context.Background(), tt.args, "",
			)

			if code != 2 {
				t.Fatalf("exit code = %d, want 2", code)
			}
			if out != "" {
				t.Fatalf("stdout must be empty: %q", out)
			}
			if !strings.Contains(errOut, "argument_error") {
				t.Fatalf("missing argument_error: %q", errOut)
			}
		})
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated read failure")
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("simulated write failure")
}

func TestCLIInfrastructureFailures(t *testing.T) {
	t.Run("missing_file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.jsonl")

		code, out, errOut := runCLI(
			context.Background(),
			[]string{"ingest", "--input", path},
			"",
		)

		if code != 4 || out != "" {
			t.Fatalf("code=%d stdout=%q", code, out)
		}
		if !strings.Contains(errOut, "infrastructure_error") {
			t.Fatalf("unexpected stderr: %q", errOut)
		}
	})

	t.Run("read_failure", func(t *testing.T) {
		var out, errOut bytes.Buffer

		code := cli.Run(
			context.Background(),
			[]string{"ingest"},
			failingReader{},
			&out,
			&errOut,
		)

		if code != 4 || out.Len() != 0 {
			t.Fatalf("code=%d stdout=%q", code, out.String())
		}
		if !strings.Contains(errOut.String(), "infrastructure_error") {
			t.Fatalf("unexpected stderr: %q", errOut.String())
		}
	})

	t.Run("write_failure", func(t *testing.T) {
		var errOut bytes.Buffer

		code := cli.Run(
			context.Background(),
			[]string{"ingest"},
			strings.NewReader(eventOne+"\n"),
			failingWriter{},
			&errOut,
		)

		if code != 4 {
			t.Fatalf("exit code = %d, want 4", code)
		}
		if !strings.Contains(errOut.String(), "write_decision") {
			t.Fatalf("unexpected stderr: %q", errOut.String())
		}
	})
}

func TestCLIContextExitCodes(t *testing.T) {
	t.Run("canceled_130", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		code, out, errOut := runCLI(
			ctx, []string{"ingest"}, eventOne+"\n",
		)

		if code != 130 || out != "" {
			t.Fatalf("code=%d stdout=%q", code, out)
		}
		if !strings.Contains(errOut, "canceled") {
			t.Fatalf("unexpected stderr: %q", errOut)
		}
	})

	t.Run("deadline_5", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(
			context.Background(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		code, out, errOut := runCLI(
			ctx, []string{"ingest"}, eventOne+"\n",
		)

		if code != 5 || out != "" {
			t.Fatalf("code=%d stdout=%q", code, out)
		}
		if !strings.Contains(errOut, "deadline_exceeded") {
			t.Fatalf("unexpected stderr: %q", errOut)
		}
	})
}
