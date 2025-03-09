package corun

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRun1(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3, 4, 5}
	var mu sync.Mutex
	var processed []int

	err := Run1(ctx, 0, inputs, func(x int) error {
		mu.Lock()
		processed = append(processed, x)
		mu.Unlock()
		return nil
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(processed) != len(inputs) {
		t.Fatalf("expected all inputs to be processed, got %v", processed)
	}
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3, 4, 5}
	var mu sync.Mutex
	var collected []int

	err := Run(ctx, 2, inputs,
		func(x int) (int, error) {
			return x * 2, nil
		},
		func(x int) error {
			mu.Lock()
			collected = append(collected, x)
			mu.Unlock()
			return nil
		},
	)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(collected) != len(inputs) {
		t.Fatalf("expected all results to be collected, got %v", collected)
	}
}

func TestRunWithError(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3}
	err := Run(ctx, 2, inputs,
		func(x int) (int, error) {
			if x == 2 {
				return 0, errors.New("error at 2")
			}
			return x, nil
		},
		func(x int) error { return nil },
	)

	if err == nil || err.Error() != "error at 2" {
		t.Fatalf("expected error at 2, got %v", err)
	}
}

func TestRunWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	inputs := []int{1, 2, 3}
	err := Run(ctx, 2, inputs,
		func(x int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return x, nil
		},
		func(x int) error { return nil },
	)

	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded error, got %v", err)
	}
}

func TestRunCompletesWorkers(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3, 4, 5}
	var wg sync.WaitGroup
	wg.Add(len(inputs))

	err := Run(ctx, 2, inputs,
		func(x int) (int, error) {
			wg.Done()
			return x, nil
		},
		func(x int) error { return nil },
	)

	wg.Wait()

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestRunWithCollectError(t *testing.T) {
	ctx := context.Background()
	inputs := []int{1, 2, 3}

	err := Run(ctx, 2, inputs,
		func(x int) (int, error) {
			return x, nil
		},
		func(x int) error {
			if x == 2 {
				return errors.New("collect error at 2")
			}
			return nil
		},
	)

	if err == nil || err.Error() != "collect error at 2" {
		t.Fatalf("expected collect error at 2, got %v", err)
	}
}
