package fileprocessor

import (
	"errors"
	"fmt"
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
