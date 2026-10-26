package cli_test

import (
	"context"
	"strings"
	"testing"

	"example.org/auguard/internal/cli"
)

type borrowedReader struct {
	source     *strings.Reader
	closeCalls int
}

func (r *borrowedReader) Read(p []byte) (int, error) {
	return r.source.Read(p)
}

func (r *borrowedReader) Close() error {
	r.closeCalls++
	return nil
}

type borrowedWriter struct {
	data       strings.Builder
	closeCalls int
}

func (w *borrowedWriter) Write(p []byte) (int, error) {
	return w.data.Write(p)
}

func (w *borrowedWriter) Close() error {
	w.closeCalls++
	return nil
}

func TestRunPreservesBorrowedStreams(t *testing.T) {
	valid := `{"id":"demo_1","kind":"auth","severity":2,"message":"fixture"}` + "\n"

	tests := []struct {
		name     string
		input    string
		canceled bool
		wantCode int
	}{
		{"success", valid, false, 0},
		{"invalid_json", "{\"id\":\n", false, 3},
		{"canceled", valid, true, 130},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			if tt.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			in := &borrowedReader{
				source: strings.NewReader(tt.input),
			}
			out := &borrowedWriter{}
			errOut := &borrowedWriter{}

			code := cli.Run(
				ctx,
				[]string{"ingest", "--input", "-"},
				in,
				out,
				errOut,
			)

			if code != tt.wantCode {
				t.Fatalf(
					"code=%d, want %d; stderr=%q",
					code, tt.wantCode, errOut.data.String(),
				)
			}

			if in.closeCalls != 0 ||
				out.closeCalls != 0 ||
				errOut.closeCalls != 0 {
				t.Fatalf(
					"borrowed streams closed: in=%d out=%d stderr=%d",
					in.closeCalls,
					out.closeCalls,
					errOut.closeCalls,
				)
			}

			t.Logf(
				"code=%d, borrowed stdin/stdout/stderr remain open",
				code,
			)
		})
	}
}
