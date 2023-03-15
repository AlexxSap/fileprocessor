package fileprocessor

import (
	"bufio"
	"strings"
)

func processRow(row []string, processors []func([]string)) {
	for _, processor := range processors {
		processor(row)
	}
}

func ProcessSequential(scaner *bufio.Scanner, processors []func([]string)) {
	for scaner.Scan() {
		row := strings.Split(scaner.Text(), ";")
		processRow(row, processors)
	}
}
