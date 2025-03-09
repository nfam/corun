package corun

import (
	"context"
	"runtime"
	"sync"
)

// Run1 runs `solve` concurrently on `queue` using `n` goroutines, discarding results.
// If `n` <= 0, it defaults to runtime.NumCPU().
func Run1[Input any](
	ctx context.Context,
	n int,
	queue []Input,
	solve func(x Input) error,
) error {
	return Run(ctx, n, queue,
		func(x Input) (struct{}, error) {
			return struct{}{}, solve(x)
		}, func(_ struct{}) error {
			return nil
		},
	)
}

// Run runs `solve` concurrently on `queue` using `n` goroutines, collects results using `collect`.
// If `n` <= 0, it defaults to runtime.NumCPU().
func Run[Input any, Result any](
	ctx context.Context,
	n int,
	queue []Input,
	solve func(Input) (Result, error),
	collect func(Result) error,
) error {
	if n <= 0 {
		n = runtime.NumCPU()
	}

	type output[R any] struct {
		val R
		err error
	}

	inputCh := make(chan Input, n)
	outputCh := make(chan output[Result], n)
	var wg sync.WaitGroup

	// Start workers
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for x := range inputCh {
				val, err := solve(x)
				outputCh <- output[Result]{val, err}
			}
		}()
	}

	// Send inputs to workers
	err := func() error {
		defer close(inputCh)

		var complete int
		var index int
		for ; index < n && index < len(queue); index++ {
			inputCh <- queue[index]
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case r := <-outputCh:
				if r.err != nil {
					return r.err
				}
				if err := collect(r.val); err != nil {
					return err
				}

				complete++
				if complete >= len(queue) {
					return nil
				}

				if index < len(queue) {
					inputCh <- queue[index]
					index++
				}
			}
		}
	}()

	wg.Wait() // Wait for all workers to finish
	return err
}
