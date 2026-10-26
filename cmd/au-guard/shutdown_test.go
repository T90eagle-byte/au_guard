package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// shutdownLog хранит stderr дочернего процесса
// и сообщает о появлении маркера готовности.
type shutdownLog struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	ready chan struct{}
}

func (l *shutdownLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, err := l.buf.Write(p)

	if strings.Contains(l.buf.String(), "CHILD_READY\n") {
		select {
		case <-l.ready:
		default:
			close(l.ready)
		}
	}

	return n, err
}

func (l *shutdownLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// Этот тест выполняет роль дочернего CLI-процесса.
// Без переменной окружения он ничего не запускает.
func TestShutdownChild(t *testing.T) {
	if os.Getenv("AUGUARD_CHILD_MODE") == "" {
		return
	}

	os.Args = []string{
		"au-guard",
		"ingest",
		"--input", "-",
		"--workers", "1",
		"--queue", "1",
		"--timeout", "30s",
	}

	os.Exit(runWithReady(func() {
		fmt.Fprintln(os.Stderr, "CHILD_READY")
	}))
}

func TestShutdownBlockedStdin(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(
		exe,
		"-test.run=^TestShutdownChild$",
	)
	cmd.Env = append(os.Environ(), "AUGUARD_CHILD_MODE=stdin")
	cmd.Stdout = io.Discard

	log := &shutdownLog{
		ready: make(chan struct{}),
	}
	cmd.Stderr = log

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	completed := false
	defer func() {
		if !completed {
			_ = cmd.Process.Kill()
			<-waitDone
		}
	}()

	select {
	case <-log.ready:
		t.Log("child ready; stdin pipe remains open")

	case <-time.After(3 * time.Second):
		t.Fatalf("child not ready: %s", log.String())
	}

	// stdin не закрываем и не записываем в него данные.
	// Посылаем SIGTERM, как требует методичка.
	start := time.Now()

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	var waitErr error

	select {
	case waitErr = <-waitDone:
		completed = true

	case <-time.After(3 * time.Second):
		t.Fatal("child did not exit after SIGTERM")
	}

	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf(
			"expected exit code 130, got %v; stderr=%s",
			waitErr, log.String(),
		)
	}

	if exitErr.ExitCode() != 130 {
		t.Fatalf(
			"exit code=%d, want 130; stderr=%s",
			exitErr.ExitCode(), log.String(),
		)
	}

	t.Logf(
		"stdin shutdown: signal=SIGTERM exit=130 wait=%s",
		time.Since(start),
	)
}

func setTestPipeCapacity(fd uintptr) (int, error) {
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		fd,
		uintptr(syscall.F_SETPIPE_SZ),
		4096,
	)
	if errno != 0 {
		return 0, errno
	}

	value, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		fd,
		uintptr(syscall.F_GETPIPE_SZ),
		0,
	)
	if errno != 0 {
		return 0, errno
	}

	return int(value), nil
}

func testPipeQueued(fd uintptr) (int, error) {
	var queued int32

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		fd,
		uintptr(syscall.TIOCINQ),
		uintptr(unsafe.Pointer(&queued)),
	)
	if errno != 0 {
		return 0, errno
	}

	return int(queued), nil
}

func TestShutdownBlockedStdout(t *testing.T) {
	const recordBytes = 64

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	pipeRead, pipeWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeRead.Close()
	defer pipeWrite.Close()

	capacity, err := setTestPipeCapacity(pipeWrite.Fd())
	if err != nil {
		t.Fatalf("set/get pipe capacity: %v", err)
	}
	if capacity != 4096 {
		t.Fatalf("pipe capacity=%d, want 4096", capacity)
	}

	cmd := exec.Command(
		exe,
		"-test.run=^TestShutdownChild$",
	)
	cmd.Env = append(os.Environ(), "AUGUARD_CHILD_MODE=stdout")
	cmd.Stdout = pipeWrite

	log := &shutdownLog{
		ready: make(chan struct{}),
	}
	cmd.Stderr = log

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Родитель не должен держать дополнительный
	// открытый пишущий конец stdout pipe.
	_ = pipeWrite.Close()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	completed := false
	var writeDone chan error

	defer func() {
		if !completed {
			_ = cmd.Process.Kill()
			<-waitDone
		}

		_ = stdin.Close()

		if writeDone != nil {
			select {
			case <-writeDone:
			case <-time.After(3 * time.Second):
				t.Error("stdin writer did not finish")
			}
		}
	}()

	// Сигналы зарегистрированы, watcher создан.
	select {
	case <-log.ready:
	case <-time.After(3 * time.Second):
		t.Fatalf("child not ready: %s", log.String())
	}

	// Проверяем длину одной записи stdout.
	sample := fmt.Sprintf(
		"{\"event_id\":\"e0001\",\"action\":\"observe\",\"reason\":\"low_severity\"}\n",
	)
	if len(sample) != recordBytes {
		t.Fatalf("decision size=%d, want 64", len(sample))
	}

	// Передаём 1000 корректных событий отдельно от
	// основной тестовой горутины.
	writeDone = make(chan error, 1)

	go func() {
		defer stdin.Close()

		for i := 1; i <= 1000; i++ {
			_, err := fmt.Fprintf(
				stdin,
				"{\"id\":\"e%04d\",\"kind\":\"auth\",\"severity\":1,\"message\":\"x\"}\n",
				i,
			)
			if err != nil {
				writeDone <- err
				return
			}
		}

		writeDone <- nil
	}()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()

	var queued int

	// Не читаем stdout. Наблюдаем заполнение pipe
	// через ioctl(TIOCINQ).
	for {
		queued, err = testPipeQueued(pipeRead.Fd())
		if err != nil {
			t.Fatalf("TIOCINQ: %v", err)
		}

		if queued > 0 && capacity-queued < recordBytes {
			break
		}

		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf(
				"stdout did not block: queued=%d capacity=%d; stderr=%s",
				queued, capacity, log.String(),
			)
		case waitErr := <-waitDone:
			completed = true
			t.Fatalf("child exited before signal: %v", waitErr)
		}
	}

	free := capacity - queued
	start := time.Now()

	// Следующая атомарная запись не помещается.
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}

	var waitErr error

	select {
	case waitErr = <-waitDone:
		completed = true
	case <-time.After(3 * time.Second):
		t.Fatal("child did not exit after SIGINT")
	}

	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf(
			"expected exit 130, got %v; stderr=%s",
			waitErr, log.String(),
		)
	}
	if exitErr.ExitCode() != 130 {
		t.Fatalf(
			"exit=%d, want 130; stderr=%s",
			exitErr.ExitCode(), log.String(),
		)
	}

	t.Logf(
		"stdout shutdown: capacity=%d queued=%d free=%d next_record=%d signal=SIGINT exit=130 wait=%s",
		capacity,
		queued,
		free,
		recordBytes,
		time.Since(start),
	)
}
