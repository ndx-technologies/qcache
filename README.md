qcache: Go in-process TTL cache with Redis pubsub invalidation

[![codecov](https://codecov.io/gh/ndx-technologies/qcache/graph/badge.svg?token=9Z03NFK8YA)](https://codecov.io/gh/ndx-technologies/qcache)

- data does not leave the process
- redis does no store anything, only broadcast invalidation control signal 
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
BenchmarkCacheGetHit-16                 34774642                34.42 ns/op            0 B/op          0 allocs/op
BenchmarkCacheGetMiss-16                36137684                34.04 ns/op            0 B/op          0 allocs/op
BenchmarkCacheSet-16                    30921657                38.02 ns/op            0 B/op          0 allocs/op
BenchmarkCacheSetParallel-16             5760608               210.4 ns/op             0 B/op          0 allocs/op
BenchmarkCacheSweep10K-16               32402218                36.71 ns/op            0 B/op          0 allocs/op
BenchmarkCacheSweep10KAllExpired-16         3130            559621 ns/op               0 B/op          0 allocs/op
BenchmarkCacheSetWithTTL-16             26060978                45.90 ns/op           46 B/op          0 allocs/op
BenchmarkCacheGetParallel-16             8910888               137.9 ns/op             0 B/op          0 allocs/op
PASS
ok      github.com/ndx-technologies/qcache      18.400s
goos: darwin
goarch: arm64
pkg: github.com/ndx-technologies/qcache/redis
cpu: Apple M3 Max
BenchmarkMessageMarshal-16                      70904353                16.80 ns/op           64 B/op          1 allocs/op
BenchmarkMessageUnmarshal-16                    76451990                15.66 ns/op           24 B/op          1 allocs/op
BenchmarkRedisPublish-16                            7770            153447 ns/op             584 B/op         19 allocs/op
BenchmarkRedisInvalidationRoundTrip-16              6949            170703 ns/op            1157 B/op         34 allocs/op
PASS
ok      github.com/ndx-technologies/qcache/redis        5.219s
```
