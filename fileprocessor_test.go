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

	ProcessSequential(scan, []func(s []string){frequentFirst, summOfSecond})

	if sum != 6 {
		t.Errorf("wrong summ of second %d", sum)
	}

	if err := compareMaps(m, map[string]int{"a": 2, "b": 1}); err != nil {
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

		for i := 0; i < 1000; i++ {
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
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c.fn()
			}
		})
	}

}
