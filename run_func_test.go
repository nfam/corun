package corun

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRun1Func(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3, 4, 5}
	var index int32
	var count int32

	fetch := func() (int, bool, error) {
		i := atomic.AddInt32(&index, 1) - 1
		if int(i) >= len(inputs) {
			return 0, false, nil
		}
		return inputs[i], true, nil
	}

	solve := func(x int) error {
		atomic.AddInt32(&count, 1)
		return nil
	}

	err := Run1Func(ctx, 0, fetch, solve)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != int32(len(inputs)) {
		t.Fatalf("expected count %d, got %d", len(inputs), count)
	}
}

func TestRunFunc_Success(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3, 4, 5}
	var index int32
	var sum int32

	fetch := func() (int, bool, error) {
		i := atomic.AddInt32(&index, 1) - 1
		if int(i) >= len(inputs) {
			return 0, false, nil
		}
		return inputs[i], true, nil
	}

	solve := func(x int) (int, error) {
		return x * 2, nil
	}

	collect := func(result int) error {
		atomic.AddInt32(&sum, int32(result))
		return nil
	}

	err := RunFunc(ctx, 3, fetch, solve, collect)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum != 30 {
		t.Fatalf("expected sum 30, got %d", sum)
	}
}

func TestRunFunc_EarlyExitOnError(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3, 4, 5}
	var index int32

	fetch := func() (int, bool, error) {
		i := atomic.AddInt32(&index, 1) - 1
		if int(i) >= len(inputs) {
			return 0, false, nil
		}
		return inputs[i], true, nil
	}

	solve := func(x int) (int, error) {
		if x == 3 {
			return 0, fmt.Errorf("error on %d", x)
		}
		return x * 2, nil
	}

	collect := func(result int) error {
		return nil
	}

	err := RunFunc(ctx, 3, fetch, solve, collect)
	if err == nil || err.Error() != "error on 3" {
		t.Fatalf("expected error containing 'error on 3', got %v", err)
	}
}

func TestRunFunc_FetchLessThanN(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2}
	var index int32
	var count int32

	fetch := func() (int, bool, error) {
		i := atomic.AddInt32(&index, 1) - 1
		if int(i) >= len(inputs) {
			return 0, false, nil
		}
		return inputs[i], true, nil
	}

	solve := func(x int) (int, error) {
		return x * 2, nil
	}

	collect := func(result int) error {
		atomic.AddInt32(&count, 1)
		return nil
	}

	err := RunFunc(ctx, 5, fetch, solve, collect)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != int32(len(inputs)) {
		t.Fatalf("expected count %d, got %d", len(inputs), count)
	}
}

func TestRunFunc_Ctx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inputs := []int{1, 2, 3, 4, 5}
	var index int32

	fetch := func() (int, bool, error) {
		i := atomic.AddInt32(&index, 1) - 1
		if int(i) >= len(inputs) {
			return 0, false, nil
		}
		return inputs[i], true, nil
	}

	solve := func(x int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return x * 2, nil
	}

	collect := func(result int) error {
		cancel()
		return nil
	}

	err := RunFunc(ctx, 3, fetch, solve, collect)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}
}

func TestRunFunc_FetchEarlyError(t *testing.T) {
	ctx := context.Background()

	fetch := func() (int, bool, error) {
		return 0, false, errors.New("fetch early error")
	}

	solve := func(x int) (int, error) {
		return x, nil
	}

	collect := func(result int) error {
		return nil
	}

	err := RunFunc(ctx, 3, fetch, solve, collect)
	if err == nil || err.Error() != "fetch early error" {
		t.Fatalf("expected fetch early error, got %v", err)
	}
}

func TestRunFunc_FetchLateError(t *testing.T) {
	ctx := context.Background()
	var index int32

	fetch := func() (int, bool, error) {
		atomic.AddInt32(&index, 1)
		if index > 3 {
			return 0, false, errors.New("fetch late error")
		}
		return int(index), true, nil
	}

	solve := func(x int) (int, error) {
		return x, nil
	}

	collect := func(result int) error {
		return nil
	}

	err := RunFunc(ctx, 3, fetch, solve, collect)
	if err == nil || err.Error() != "fetch late error" {
		t.Fatalf("expected fetch late error, got %v", err)
	}
}

func TestRunFunc_CollectError(t *testing.T) {
	ctx := context.Background()
	var index int32

	fetch := func() (int, bool, error) {
		i := atomic.AddInt32(&index, 1) - 1
		if i >= 5 {
			return 0, false, nil
		}
		return int(i), true, nil
	}

	solve := func(x int) (int, error) {
		return x * 2, nil
	}

	collect := func(result int) error {
		if result == 4 {
			return errors.New("collect error")
		}
		return nil
	}

	err := RunFunc(ctx, 3, fetch, solve, collect)
	if err == nil || err.Error() != "collect error" {
		t.Fatalf("expected collect error, got %v", err)
	}
}
