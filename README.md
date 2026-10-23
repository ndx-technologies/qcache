qcache: Go in-process TTL cache with Redis pubsub invalidation

[![codecov](https://codecov.io/gh/ndx-technologies/qcache/graph/badge.svg?token=9Z03NFK8YA)](https://codecov.io/gh/ndx-technologies/qcache)

- data does not leave the process
- redis does no store anything, only broadcast invlaidation control signal 
- invalidation updates are broadcasted to peers
- no JSON
- binary encoding keys on wire
- zero memory allocations
- no reflection, no `interface{}`, no `any`, generics

```
$ go test ./... -run '^$' -bench . -benchmem
goos: darwin
goarch: arm64
pkg: github.com/ndx-technologies/qcache
cpu: Apple M3 Max
BenchmarkCacheGetHit-16                 34779355                34.51 ns/op            0 B/op          0 allocs/op
BenchmarkCacheGetMiss-16                35679198                34.10 ns/op            0 B/op          0 allocs/op
BenchmarkCacheSet-16                    32181862                39.20 ns/op            0 B/op          0 allocs/op
BenchmarkCacheSetParallel-16             5686920               212.6 ns/op             0 B/op          0 allocs/op
BenchmarkCacheSweep10K-16                  17090             69939 ns/op               0 B/op          0 allocs/op
BenchmarkCacheGetParallel-16             9262346               134.9 ns/op             0 B/op          0 allocs/op
PASS
ok      github.com/ndx-technologies/qcache      7.847s
goos: darwin
goarch: arm64
pkg: github.com/ndx-technologies/qcache/redis
cpu: Apple M3 Max
BenchmarkMessageMarshal-16                      72030146                16.52 ns/op           64 B/op          1 allocs/op
BenchmarkMessageUnmarshal-16                    76804713                16.17 ns/op           24 B/op          1 allocs/op
BenchmarkRedisPublish-16                            7915            151204 ns/op             584 B/op         19 allocs/op
BenchmarkRedisInvalidationRoundTrip-16              7203            166698 ns/op            1156 B/op         34 allocs/op
PASS
ok      github.com/ndx-technologies/qcache/redis        5.398s
```
