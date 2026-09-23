//go:build unix

package rabbitmq

import (
	"context"
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"
)

func selfCPU(t *testing.T) time.Duration {
	t.Helper()

	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		t.Skipf("rusage 를 읽지 못했다: %v", err)
	}

	return time.Duration(r.Utime.Nano() + r.Stime.Nano())
}

// 첫 연결을 기다리는 동안 CPU 를 태우면 안 된다.
//
// 예전에는 select 의 default 로 돌아 벽시계 500ms 에 CPU 487ms 를 썼다. 티커로
// 바꾼 뒤는 2ms 다. 문턱을 벽시계의 50%로 둔 것은 그 사이 어디에 걸려도 회귀를
// 잡되, 다른 시험이나 GC 가 끼어든 정도로는 깨지지 않게 하기 위해서다.
func TestConnectDoesNotBurnCPUWhileWaiting(t *testing.T) {
	c := NewClient(
		WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
		// 닿지 않는 포트라 끝내 준비되지 않는다.
		WithHost("127.0.0.1"),
		WithPort("1"),
		WithConnectionRetryDelay(time.Hour),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	before := selfCPU(t)
	start := time.Now()

	if err := c.Connect(ctx); err == nil {
		t.Fatal("연결이 될 리 없는데 성공했다")
	}

	wall := time.Since(start)
	used := selfCPU(t) - before

	if wall < 250*time.Millisecond {
		t.Fatalf("기다리지 않고 빠져나왔다: %v", wall)
	}

	if used > wall/2 {
		t.Fatalf("CPU 를 태우고 있다: 벽시계 %v 동안 CPU %v", wall, used)
	}

	t.Logf("벽시계 %v · CPU %v", wall, used)
}

// 기다리는 도중 취소하면 곧바로 빠져나와야 한다.
func TestConnectReturnsContextError(t *testing.T) {
	c := NewClient(
		WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
		WithHost("127.0.0.1"),
		WithPort("1"),
		WithConnectionRetryDelay(time.Hour),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := c.Connect(ctx)

	if err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	if d := time.Since(start); d > time.Second {
		t.Fatalf("취소 뒤 빠져나오는 데 %v 걸렸다", d)
	}
}
