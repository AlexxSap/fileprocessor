package fileprocessor

import (
	"bufio"
	"os"
	"strings"
)

func processRow(row []string, processors []func([]string)) {
	for _, processor := range processors {
		processor(row)
	}
}

func ProcessSequential(scanner *bufio.Scanner, processors []func([]string)) {
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
	ProcessSequential(scanner, processors)

	return nil
}
