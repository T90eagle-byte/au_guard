package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"example.org/auguard/internal/cli"
)

func dupStream(fd int, name string) (*os.File, error) {
	dupFD, err := syscall.Dup(fd)
	if err != nil {
		return nil, err
	}

	if err := syscall.SetNonblock(dupFD, true); err != nil {
		_ = syscall.Close(dupFD)
		return nil, err
	}

	file := os.NewFile(uintptr(dupFD), name)
	if file == nil {
		_ = syscall.Close(dupFD)
		return nil, fmt.Errorf("os.NewFile returned nil for fd %d", fd)
	}

	return file, nil
}

func runWithReady(ready func()) int {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	in, err := dupStream(0, "stdin")
	if err != nil {
		stop()
		fmt.Fprintln(os.Stderr, "infrastructure_error: prepare_stdin")
		return 4
	}

	out, err := dupStream(1, "stdout")
	if err != nil {
		_ = in.Close()
		stop()
		fmt.Fprintln(os.Stderr, "infrastructure_error: prepare_stdout")
		return 4
	}

	finished := make(chan struct{})
	watcherDone := make(chan struct{})

	go func() {
		defer close(watcherDone)

		select {
		case <-ctx.Done():
			_ = in.Close()
			_ = out.Close()

		case <-finished:
		}
	}()

	// Сообщаем о готовности после регистрации сигналов
	// и запуска watcher.
	if ready != nil {
		ready()
	}

	code := cli.Run(
		ctx,
		os.Args[1:],
		in,
		out,
		os.Stderr,
	)

	close(finished)
	<-watcherDone

	_ = in.Close()
	_ = out.Close()

	stop()
	return code
}
func run() int {
	return runWithReady(nil)
}
func main() {
	os.Exit(run())
}
