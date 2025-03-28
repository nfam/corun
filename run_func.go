package corun

import (
	"context"
	"runtime"
	"sync"
)

// Run1Func runs `solve` concurrently with inputs from `fetch` using `n` goroutines,
// discarding results. If `n` <= 0, it defaults to runtime.NumCPU().
func Run1Func[Input any](
	ctx context.Context,
	n int,
	fetch func() (Input, bool, error),
	solve func(x Input) error,
) error {
	return RunFunc(ctx, n, fetch,
		func(x Input) (struct{}, error) {
			return struct{}{}, solve(x)
		}, func(_ struct{}) error {
			return nil
		},
	)
}

// RunFunc runs `solve` concurrently with inputs from `fetch` using `n` goroutines
// and processes results with `collect`. If `n` <= 0, it defaults to runtime.NumCPU().
func RunFunc[Input any, Result any](
	ctx context.Context,
	n int,
	fetch func() (Input, bool, error),
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
		var count int
		var fetched bool

		for ; count < n; count++ {
			x, ok, err := fetch()
			if err != nil {
				return err
			}
			if !ok {
				fetched = true
				break
			}
			inputCh <- x
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
				if fetched && complete >= count {
					return nil
				}

				x, ok, err := fetch()
				if err != nil {
					return err
				}
				if !ok {
					fetched = true
					if complete >= count {
						return nil
					}
				} else {
					count++
					inputCh <- x
				}
			}
		}
	}()

	wg.Wait() // Wait for all workers to finish
	return err
}
