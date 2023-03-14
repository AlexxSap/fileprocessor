package fileprocessor

func processRow(row []string, processors []func([]string)) {

	for _, processor := range processors {
		processor(row)
	}

}
