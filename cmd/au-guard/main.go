package main

import (
	"context"
	"os"

	"example.org/auguard/internal/cli"
)

func main() {
	os.Exit(cli.Run(
		context.Background(),
		os.Args[1:],
		os.Stdin,
		os.Stdout,
		os.Stderr,
	))
}
