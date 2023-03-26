# fileprocessor
Processing files

Result of benchmarking

go test -benchmem -bench=.
```
goos: linux
goarch: amd64
pkg: fileprocessor
cpu: Intel(R) Core(TM) i5-2410M CPU @ 2.30GHz
BenchmarkProcessFile/sequential-4                     15          95692177 ns/op        17588222 B/op     200004 allocs/op
BenchmarkProcessFile/concurrent-4                     32          37348268 ns/op        22600830 B/op     201258 allocs/op
PASS
ok      fileprocessor   4.239s
```
