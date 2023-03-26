package fileprocessor

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
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

// func worker(ctx context.Context, buffer <-chan []string) <-chan temp {

// }

// func combiner(ctx context.Context, inputs ...<-chan temp) <-chan temp {

// }

func processConcurrent(scanner *bufio.Scanner) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bufferSize := 3
	rowChan := reader(ctx, scanner, bufferSize)

	for rowBatch := range rowChan {
		fmt.Println("-----")
		for _, row := range rowBatch {
			fmt.Println(row)
		}
	}

}

func ProcessFileConcurrent(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)
	processConcurrent(scanner)

	return nil
}
