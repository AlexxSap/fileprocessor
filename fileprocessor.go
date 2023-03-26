package fileprocessor

import (
	"bufio"
	"context"
	"os"
	"strings"
	"sync"
)

func processRow(row []string, processors []func([]string)) {
	for _, processor := range processors {
		processor(row)
	}
}

func processSequential(scanner *bufio.Scanner, processors []func([]string)) {
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ";")
		processRow(row, processors)
	}
}

func ProcessFileSequential(fileName string, processors []func([]string)) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)
	processSequential(scanner, processors)

	return nil
}

type Processor[P any] interface {
	ProcessString(string) P
}

type Accumulator[A any, P Processor[P]] interface {
	Accumulate(P) A
}

func reader(ctx context.Context, scanner *bufio.Scanner, bufferSize int) <-chan []string {
	out := make(chan []string)
	buffer := []string{}

	go func() {
		defer close(out)
		for {
			scanned := scanner.Scan()
			select {
			case <-ctx.Done():
				return
			default:
				row := scanner.Text()
				if len(buffer) == bufferSize || !scanned {
					out <- buffer
					buffer = []string{}
				}

				buffer = append(buffer, row)
			}

			if !scanned {
				return
			}
		}
	}()

	return out
}

func worker[P Processor[P]](buffer <-chan []string) <-chan []P {
	out := make(chan []P)

	go func() {
		defer close(out)

		var p P
		for rows := range buffer {
			res := make([]P, len(rows))
			for _, row := range rows {
				res = append(res, p.ProcessString(row))
			}
			out <- res
		}
	}()

	return out
}

func combiner[P Processor[P]](ctx context.Context, inputs ...<-chan []P) <-chan []P {
	out := make(chan []P)

	var wg sync.WaitGroup
	multiplexer := func(p <-chan []P) {
		defer wg.Done()

		for in := range p {
			select {
			case <-ctx.Done():
			case out <- in:
			}
		}
	} /// multiplexer

	wg.Add(len(inputs))
	for _, in := range inputs {
		go multiplexer(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func processConcurrent[P Processor[P], A Accumulator[A, P]](scanner *bufio.Scanner) A {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bufferSize := 3
	workersSize := 3
	rowCh := reader(ctx, scanner, bufferSize)

	workersCh := make([]<-chan []P, workersSize)
	for i := 0; i < workersSize; i++ {
		workersCh[i] = worker[P](rowCh)
	}

	var accumulator A
	for processed := range combiner(ctx, workersCh...) {
		for _, p := range processed {
			accumulator = accumulator.Accumulate(p)
		}
	}

	return accumulator
}

func ProcessFileConcurrent[P Processor[P], A Accumulator[A, P]](fileName string) (A, error) {
	file, err := os.Open(fileName)
	if err != nil {
		var a A
		return a, err
	}

	scanner := bufio.NewScanner(file)
	res := processConcurrent[P, A](scanner)

	return res, nil
}
