package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"example.org/auguard/internal/cli"
)

func TestCLIRejectsHashTimeoutOverride(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := cli.Run(
		context.Background(),
		[]string{
			"ingest",
			"--hash-timeout", "3s",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)

	if code != 2 {
		t.Fatalf(
			"expected argument error, code=%d stderr=%q",
			code,
			stderr.String(),
		)
	}

	if stdout.Len() != 0 {
		t.Fatalf("unexpected stdout: %q", stdout.String())
	}

	if !strings.Contains(stderr.String(), "argument_error") {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}

	t.Log("cli_hash_timeout_override=3s rejected=true")
}
