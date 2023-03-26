package fileprocessor

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func compareSlices[T comparable](act, exp []T) error {
	if len(act) != len(exp) {
		return errors.New("different len")
	}

	for i := 0; i < len(act); i++ {
		if act[i] != exp[i] {
			return fmt.Errorf("act: %v exp: %v at index: %d", act[i], exp[i], i)
		}
	}

	return nil
}

func compareMaps[K, V comparable](act, exp map[K]V) error {
	if len(act) != len(exp) {
		return errors.New("different len")
	}

	for eKey, eValue := range exp {
		aValue, isIn := act[eKey]
		if !isIn {
			return fmt.Errorf("key %v not int act", eKey)
		}
		if eValue != aValue {
			return fmt.Errorf("act: %v exp: %v at key: %v", eValue, aValue, eKey)
		}
	}

	return nil
}

func Test_ProcessRowCounts(t *testing.T) {
	testCases := []struct {
		desc string
		str  []string
		proc []func([]string) int
		exp  []int
	}{
		{
			desc: "empty",
			str:  []string{},
			proc: []func([]string) int{},
			exp:  []int{},
		},
		{
			desc: "countEmpty",
			str:  []string{"a", "", "a", "", ""},
			proc: []func([]string) int{
				func(s []string) int {
					res := int(0)
					for _, v := range s {
						if len(v) == 0 {
							res++
						}
					}
					return res
				}},
			exp: []int{3},
		},
		{
			desc: "countAandB",
			str:  []string{"A", "B", "A", "B", "A"},
			proc: []func([]string) int{
				func(s []string) int {
					res := 0
					for _, v := range s {
						if v == "A" {
							res++
						}
					}
					return res
				},
				func(s []string) int {
					res := 0
					for _, v := range s {
						if v == "B" {
							res++
						}
					}
					return res
				}},
			exp: []int{3, 2},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			act := make([]int, 0, len(tC.exp))
			f := make([]func([]string), 0, len(tC.proc))
			for _, v := range tC.proc {
				local := v
				f = append(f, func(s []string) {
					act = append(act, local(s))
				})
			}

			processRow(tC.str, f)
			if err := compareSlices(act, tC.exp); err != nil {
				t.Error(err)
			}
		})
	}
}

func Test_ProcessRow(t *testing.T) {
	row := []string{"ab", "cd", "ab", "ab", "d"}

	shortestRes := ""
	shortest := func(s []string) {
		if len(s) == 0 {
			return
		}
		shortestRes = s[0]
		for _, v := range s {
			if len(v) < len(shortestRes) {
				shortestRes = v
			}
		}
	}

	frequentResStr := ""
	frequentResCount := 0
	frequent := func(s []string) {
		m := make(map[string]int)
		for _, v := range s {
			m[v]++
		}

		for k, v := range m {
			if v > frequentResCount {
				frequentResStr = k
				frequentResCount = v
			}
		}

	}

	processRow(row, []func(s []string){shortest, frequent})

	if shortestRes != "d" {
		t.Errorf("wrong shortest %s", shortestRes)
	}
	if frequentResStr != "ab" {
		t.Errorf("wrong frequent str %s", frequentResStr)
	}
	if frequentResCount != 3 {
		t.Errorf("wrong frequent count %d", frequentResCount)
	}
}

func Test_ProcessSequential(t *testing.T) {

	data := "a;1;z\nb;2;z\na;3;z"
	scan := bufio.NewScanner(strings.NewReader(data))

	m := make(map[string]int)
	frequentFirst := func(s []string) {
		m[s[0]]++
	}

	sum := 0
	summOfSecond := func(s []string) {
		v, _ := strconv.Atoi(s[1])
		sum += v
	}

	processSequential(scan, []func(s []string){frequentFirst, summOfSecond})

	if sum != 6 {
		t.Errorf("wrong summ of second %d", sum)
	}

	if err := compareMaps(m, map[string]int{"a": 2, "b": 1}); err != nil {
		t.Error(err)
	}

}

type FirstNumberProcessor struct {
	firstNumber int
}

func (acc FirstNumberProcessor) Init(a *FirstNumberProcessor) {
}

func (proc FirstNumberProcessor) ProcessString(p *FirstNumberProcessor, row string) {
	rows := strings.Split(row, ";")
	v, _ := strconv.Atoi(rows[1])
	p.firstNumber += v
}

type FirstNumberAccumulator struct {
	FirstNumberProcessor
}

func (acc FirstNumberAccumulator) Init(a *FirstNumberAccumulator) {
}

func (acc FirstNumberAccumulator) Accumulate(a *FirstNumberAccumulator, row FirstNumberProcessor) {
	a.firstNumber += row.firstNumber
}

func Test_ProcessFileConcurrentSimple(t *testing.T) {
	fileName := "testConcurrentSimpleFile.csv"
	defer os.Remove(fileName)

	expectedResult := 0
	{
		f, err := os.Create(fileName)
		if err != nil {
			t.Error(err)
		}
		defer f.Close()

		dataFormat := "asdasd;%d;7ieirt;%d;sdfhsdfs;%d\n"
		for i := 0; i < 10; i++ {
			expectedResult += i
			_, err = f.WriteString(fmt.Sprintf(dataFormat, i, i+1, i+2))
			if err != nil {
				t.Error(err)
			}
		}
	}

	res, err := ProcessFileConcurrent[FirstNumberProcessor, FirstNumberAccumulator](fileName)
	if err != nil {
		t.Error(err)
	}

	if res.firstNumber != expectedResult {
		t.Errorf("act: %d exp: %d", res.firstNumber, expectedResult)
	}
}

type ComplexProcessor struct {
	numberAt1   int
	numberAt3   int
	numberAt5   int
	counts      int
	numberOfRow int
	words       map[string]int
}

func (proc ComplexProcessor) Init(p *ComplexProcessor) {
	p.words = make(map[string]int)
}

func (proc ComplexProcessor) ProcessString(p *ComplexProcessor, row string) {
	p.numberOfRow++
	rows := strings.Split(row, ";")
	p.counts += len(rows)
	v, _ := strconv.Atoi(rows[1])
	p.numberAt1 += v
	v, _ = strconv.Atoi(rows[3])
	p.numberAt3 += v
	v, _ = strconv.Atoi(rows[5])
	p.numberAt5 += v

	p.words[rows[0]]++
	p.words[rows[2]]++
	p.words[rows[4]]++
	p.words[rows[6]]++
}

type ComplexAccumulator struct {
	ComplexProcessor
}

func (acc ComplexAccumulator) Init(a *ComplexAccumulator) {
	a.words = make(map[string]int)
}

func (acc ComplexAccumulator) Accumulate(a *ComplexAccumulator, row ComplexProcessor) {
	a.numberAt1 += row.numberAt1
	a.numberAt3 += row.numberAt3
	a.numberAt5 += row.numberAt5
	a.counts += row.counts
	a.numberOfRow += row.numberOfRow
	for k, v := range row.words {
		a.words[k] += v
	}
}

func Test_ProcessFileConcurrent(t *testing.T) {
	fileName := "testConcurrentFile.csv"
	defer os.Remove(fileName)

	expectedResultAt1 := 0
	expectedResultAt3 := 0
	expectedResultAt5 := 0
	expectedCounts := 0
	expectedNumberOfRow := 0
	expectedWords := make(map[string]int)

	{
		f, err := os.Create(fileName)
		if err != nil {
			t.Error(err)
		}
		defer f.Close()

		dataFormat := "aasadasd;%d;bwefwefwef;%d;cdvdfdfv;%d;458ug\n"

		for i := 0; i < 10; i++ {
			expectedResultAt1 += i
			expectedResultAt3 += i + 1
			expectedResultAt5 += i + 2
			expectedCounts += 7
			expectedNumberOfRow++
			expectedWords["aasadasd"]++
			expectedWords["bwefwefwef"]++
			expectedWords["cdvdfdfv"]++
			expectedWords["458ug"]++
			_, err = f.WriteString(fmt.Sprintf(dataFormat, i, i+1, i+2))
			if err != nil {
				t.Error(err)
			}
		}
	}

	res, err := ProcessFileConcurrent[ComplexProcessor, ComplexAccumulator](fileName)
	if err != nil {
		t.Error(err)
	}

	if res.numberAt1 != expectedResultAt1 {
		t.Errorf("numberAt1 act: %d exp: %d", res.numberAt1, expectedResultAt1)
	}
	if res.numberAt3 != expectedResultAt3 {
		t.Errorf("numberAt3 act: %d exp: %d", res.numberAt3, expectedResultAt3)
	}
	if res.numberAt5 != expectedResultAt5 {
		t.Errorf("numberAt5 act: %d exp: %d", res.numberAt5, expectedResultAt5)
	}
	if res.counts != expectedCounts {
		t.Errorf("counts act: %d exp: %d", res.counts, expectedCounts)
	}
	if res.numberOfRow != expectedNumberOfRow {
		t.Errorf("numberOfRow act: %d exp: %d", res.numberOfRow, expectedNumberOfRow)
	}

	if err := compareMaps(res.words, expectedWords); err != nil {
		t.Error(err)
	}
}

func Test_ProcessFileSequential(t *testing.T) {
	fileName := "testFile.csv"
	defer os.Remove(fileName)

	{
		f, err := os.Create(fileName)
		if err != nil {
			t.Error(err)
		}
		defer f.Close()

		data := "a;1;z\nb;2;z\na;3;z"
		_, err = f.WriteString(data)
		if err != nil {
			t.Error(err)
		}
	}

	sum := 0
	summOfSecond := func(s []string) {
		v, _ := strconv.Atoi(s[1])
		sum += v
	}

	err := ProcessFileSequential(fileName, []func(s []string){summOfSecond})
	if err != nil {
		t.Error(err)
	}

	if sum != 6 {
		t.Errorf("wrong summ of second %d", sum)
	}
}

func BenchmarkProcessFile(b *testing.B) {
	fileName := "benchFile.csv"
	defer os.Remove(fileName)

	{
		f, err := os.Create(fileName)
		if err != nil {
			b.Error(err)
		}
		defer f.Close()

		for i := 0; i < 100000; i++ {
			data := "ahfdjhgkfd;" + strconv.Itoa(i) + ";asdfdfgkdkfjgkfdjgz\n"
			_, err = f.WriteString(data)
			if err != nil {
				b.Error(err)
			}
		}
	}

	summOfSecond := func(s []string) {
		strconv.Atoi(s[1])
	}

	cases := []struct {
		name string
		fn   func()
	}{
		{
			name: "sequential",
			fn: func() {
				ProcessFileSequential(fileName, []func(s []string){summOfSecond})
			},
		},
		{
			name: "concurrent",
			fn: func() {
				ProcessFileConcurrent[FirstNumberProcessor, FirstNumberAccumulator](fileName)
			},
		},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.fn()
			}
		})
	}

}
