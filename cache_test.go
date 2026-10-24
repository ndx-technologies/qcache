package qcache_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ndx-technologies/qcache"
)

func ExampleCache() {
	type K int

	type T struct {
		Value string
	}

	var config qcache.CacheConfig[K, T]
	s := qcache.NewCache(config, nil)

	id := K(1)
	s.Set(context.Background(), id, T{Value: "hot"})

	v, _ := s.Get(K(1))
	fmt.Println(v.Value)
	// Output: hot
}

type recorder[K any] struct {
	mu      sync.Mutex
	sent    [][]K
	err     error
	handler func(keys ...K)
}

func (r *recorder[K]) Publish(ctx context.Context, keys ...K) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, append([]K(nil), keys...))
	return r.err
}

func (r *recorder[K]) Subscribe(handler func(keys ...K)) {
	r.mu.Lock()
	r.handler = handler
	r.mu.Unlock()
}

func (r *recorder[K]) deliver(keys ...K) {
	r.mu.Lock()
	handler := r.handler
	r.mu.Unlock()
	handler(keys...)
}

func (r *recorder[K]) calls() [][]K {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]K(nil), r.sent...)
}

func TestSetGetDel(t *testing.T) {
	ctx := t.Context()
	const (
		a       = uint64(1)
		missing = uint64(2)
	)

	r := &recorder[uint64]{}
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{}, r)

	if _, ok := c.Get(a); ok {
		t.Fatal("empty cache returned a value")
	}

	if err := c.Set(ctx, a, 1); err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Get(a); !ok || got != 1 {
		t.Fatal(got, ok)
	}

	if err := c.Set(ctx, a, 2); err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Get(a); !ok || got != 2 {
		t.Fatal(got, ok)
	}

	if err := c.Del(ctx, a, missing); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get(a); ok {
		t.Fatal("deleted key is still cached")
	}

	want := [][]uint64{{a}, {a}, {a, missing}}
	got := r.calls()
	if len(got) != len(want) {
		t.Fatalf("published %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("message %d keys = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestSubscriptionInvalidates(t *testing.T) {
	ctx := t.Context()
	const a = uint64(1)

	r := &recorder[uint64]{}
	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, r)

	if err := c.Set(ctx, a, "one"); err != nil {
		t.Fatal(err)
	}
	calls := r.calls()
	if len(calls) != 1 || !slices.Equal(calls[0], []uint64{a}) {
		t.Fatal(calls)
	}

	r.deliver(a)
	if _, ok := c.Get(a); ok {
		t.Fatal("subscribed handler did not remove the cached entry")
	}
}

func TestTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := 1

		c := qcache.NewCache(qcache.CacheConfig[int, string]{
			TTL: 20 * time.Millisecond,
		}, nil)

		if err := c.Set(context.Background(), a, "one"); err != nil {
			t.Fatal(err)
		}
		if _, ok := c.Get(a); !ok {
			t.Fatal("value disappeared before the ttl")
		}

		time.Sleep(20*time.Millisecond + time.Nanosecond)
		if _, ok := c.Get(a); ok {
			t.Fatal("value outlived the ttl")
		}
		if n := c.Len(); n != 0 {
			t.Fatalf("expired value was not removed on read: %d", n)
		}
	})
}

func TestSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}, nil)

		if err := c.Set(context.Background(), a, "one"); err != nil {
			t.Fatal(err)
		}
		if n := c.Len(); n != 1 {
			t.Fatal(n)
		}

		time.Sleep(10*time.Millisecond + time.Nanosecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d expired entries", n)
		}
	})
}

func TestSweepKeepsLiveEntries(t *testing.T) {
	const a = uint64(1)

	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
		TTL:           time.Hour,
		SweepInterval: time.Minute,
	}, nil)
	if err := c.Set(context.Background(), a, "one"); err != nil {
		t.Fatal(err)
	}

	c.Sweep()

	if got, ok := c.Get(a); !ok || got != "one" {
		t.Fatal(got, ok)
	}
}

// Applying WithDefaults is the caller's job: without a SweepInterval there is no
// expiry index, so Sweep keeps nothing out of the way of Get.
func TestSweepWithoutIntervalKeepsExpiredEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL: 10 * time.Millisecond,
		}, nil)
		if err := c.Set(context.Background(), a, "one"); err != nil {
			t.Fatal(err)
		}

		time.Sleep(10*time.Millisecond + time.Nanosecond)
		c.Sweep()

		if n := c.Len(); n != 1 {
			t.Fatalf("len = %d, want the expired entry left in place", n)
		}
		if _, ok := c.Get(a); ok {
			t.Fatal("get returned an expired entry")
		}
	})
}

// A bucket wider than the TTL used to be clamped to the TTL. It does not need to be:
// everything lands in the bucket that is currently being swept, and the sweep of the
// current bucket deletes what is expired.
func TestSweepWithIntervalWiderThanTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const (
			oldKey = uint64(1)
			newKey = uint64(2)
		)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: time.Second,
		}.WithDefaults(), nil)

		if err := c.Set(ctx, oldKey, "old"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(11 * time.Millisecond)
		if err := c.Set(ctx, newKey, "new"); err != nil {
			t.Fatal(err)
		}

		c.Sweep()

		if _, ok := c.Get(oldKey); ok {
			t.Fatal("sweep kept the expired entry")
		}
		if got, ok := c.Get(newKey); !ok || got != "new" {
			t.Fatalf("sweep removed a live entry: %q %v", got, ok)
		}

		time.Sleep(11 * time.Millisecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d expired entries", n)
		}
	})
}

// A TTL below one microsecond used to fall back to a one nanosecond bucket, because
// TTL/64 truncated to zero. WithDefaults makes the interval a quarter of a TTL, so
// no derived width is needed.
func TestSweepWithNanosecondTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const a = uint64(1)

		config := qcache.CacheConfig[uint64, string]{TTL: 50 * time.Nanosecond}.WithDefaults()
		if config.SweepInterval != 50*time.Nanosecond {
			t.Fatalf("sweep interval = %v, want the ttl", config.SweepInterval)
		}

		c := qcache.NewCache(config, nil)
		if err := c.Set(context.Background(), a, "one"); err != nil {
			t.Fatal(err)
		}

		time.Sleep(51 * time.Nanosecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d expired entries", n)
		}
	})
}

// One sweep has to drain every bucket written since the previous one, however many
// sweep intervals were missed.
func TestSweepDrainsManyBuckets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const n = 100

		c := qcache.NewCache(qcache.CacheConfig[uint64, int]{
			TTL:           100 * time.Millisecond,
			SweepInterval: time.Millisecond,
		}.WithDefaults(), nil)

		// One key per tick, so every bucket in the queue holds something.
		for i := range n {
			if err := c.Set(ctx, uint64(i), i); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
		if got := c.Len(); got != n {
			t.Fatalf("len = %d, want %d", got, n)
		}

		time.Sleep(100 * time.Millisecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d expired entries", n)
		}
	})
}

// Steady state: one bucket written and one bucket dropped per sweep, so only the
// entries from the last TTL window survive.
func TestSweepSteadyStateKeepsLastTTLWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const n = 20

		c := qcache.NewCache(qcache.CacheConfig[uint64, int]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}.WithDefaults(), nil)

		// Key i is written at i*2ms, so it expires at i*2ms+10ms.
		for i := range n {
			if err := c.Set(ctx, uint64(i), i); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
			c.Sweep()
		}

		// At 40ms the keys written at 0..28ms are expired, and key 15 expires exactly
		// now, which does not count as expired.
		if got := c.Len(); got != 5 {
			t.Fatalf("len = %d, want the 5 entries of the last ttl window", got)
		}
		for i := range 15 {
			if _, ok := c.Get(uint64(i)); ok {
				t.Fatalf("expired entry %d survived its sweep", i)
			}
		}
		for i := 15; i < n; i++ {
			if got, ok := c.Get(uint64(i)); !ok || got != i {
				t.Fatalf("live entry %d was swept: %d %v", i, got, ok)
			}
		}
	})
}

func TestRunScheduledSweep(t *testing.T) {
	const a = uint64(1)

	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
		TTL:           10 * time.Millisecond,
		SweepInterval: 5 * time.Millisecond,
	}, nil)

	// A zero interval must return immediately instead of blocking the caller.
	zero := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, nil)
	zero.RunScheduledSweep()

	go c.RunScheduledSweep()
	if err := c.Set(t.Context(), a, "one"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if c.Len() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("scheduled sweep did not remove the expired entry")
}

func TestPublishErrorIsReturnedAndLocalWriteStillHappens(t *testing.T) {
	ctx := t.Context()
	const a = uint64(1)

	boom := errors.New("boom")
	r := &recorder[uint64]{err: boom}
	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, r)

	if err := c.Set(ctx, a, "one"); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if got, ok := c.Get(a); !ok || got != "one" {
		t.Fatal(got, ok)
	}
}

func TestNilBroadcaster(t *testing.T) {
	ctx := t.Context()
	const a = uint64(1)

	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, nil)
	if err := c.Set(ctx, a, "one"); err != nil {
		t.Fatal(err)
	}
	if err := c.Del(ctx, a); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	ctx := t.Context()
	r := &recorder[uint64]{}
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{
		TTL:           time.Second,
		SweepInterval: time.Millisecond,
	}, r)

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			for j := range 500 {
				k := uint64((i + j) % 8)
				_ = c.Set(ctx, k, j)
				c.Get(k)
				r.deliver(k)
				_ = c.Del(ctx, k)
			}
		})
	}
	wg.Wait()
}

func TestCacheConfigWithDefaults(t *testing.T) {
	tests := []struct {
		name string
		in   qcache.CacheConfig[uint64, string]
		want qcache.CacheConfig[uint64, string]
	}{
		{
			name: "zero",
			want: qcache.CacheConfig[uint64, string]{TTL: 5 * time.Minute, SweepInterval: time.Minute},
		},
		{
			name: "short ttl",
			in:   qcache.CacheConfig[uint64, string]{TTL: 30 * time.Second},
			want: qcache.CacheConfig[uint64, string]{TTL: 30 * time.Second, SweepInterval: 30 * time.Second},
		},
		{
			name: "long ttl",
			in:   qcache.CacheConfig[uint64, string]{TTL: 10 * time.Minute},
			want: qcache.CacheConfig[uint64, string]{TTL: 10 * time.Minute, SweepInterval: time.Minute},
		},
		{
			name: "explicit sweep",
			in:   qcache.CacheConfig[uint64, string]{TTL: 10 * time.Minute, SweepInterval: 2 * time.Minute},
			want: qcache.CacheConfig[uint64, string]{TTL: 10 * time.Minute, SweepInterval: 2 * time.Minute},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.WithDefaults(); got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNoTTLNeverExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, nil)
		if err := c.Set(context.Background(), a, "one"); err != nil {
			t.Fatal(err)
		}

		time.Sleep(24 * time.Hour)
		if got, ok := c.Get(a); !ok || got != "one" {
			t.Fatal(got, ok)
		}
		if n := c.Len(); n != 1 {
			t.Fatal(n)
		}
	})
}

func TestOverwriteResetsTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL: 10 * time.Millisecond,
		}, nil)

		if err := c.Set(context.Background(), a, "one"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Millisecond)

		if err := c.Set(context.Background(), a, "two"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Millisecond)

		if got, ok := c.Get(a); !ok || got != "two" {
			t.Fatalf("overwrite did not extend the ttl: %q %v", got, ok)
		}

		time.Sleep(5 * time.Millisecond)
		if _, ok := c.Get(a); ok {
			t.Fatal("value outlived the extended ttl")
		}
	})
}

func TestSweepMixedEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			oldKey = uint64(3)
			newKey = uint64(4)
		)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: time.Millisecond,
		}, nil)

		if err := c.Set(context.Background(), oldKey, "old"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Millisecond)
		if err := c.Set(context.Background(), newKey, "new"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Millisecond)

		c.Sweep()

		if _, ok := c.Get(oldKey); ok {
			t.Fatal("sweep kept the expired entry")
		}
		if got, ok := c.Get(newKey); !ok || got != "new" {
			t.Fatalf("sweep removed a live entry: %q %v", got, ok)
		}
	})
}

// Sweeps can be late or skipped entirely; a single sweep must still catch up.
func TestSweepRemovesExpiredAcrossSkippedIntervals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}, nil)

		for i := range 3 {
			if err := c.Set(ctx, uint64(i), "stale"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)

		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d expired entries", n)
		}
	})
}

// Overwriting a key queued for an earlier expiry must not let that stale slot
// remove the fresh value.
func TestSweepKeepsOverwrittenEntryWhenOlderSlotIsDue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}, nil)

		if err := c.Set(ctx, a, "one"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(8 * time.Millisecond)
		if err := c.Set(ctx, a, "two"); err != nil {
			t.Fatal(err)
		}

		// 12ms: the original expiry (10ms) is now in the past.
		time.Sleep(4 * time.Millisecond)
		c.Sweep()

		if got, ok := c.Get(a); !ok || got != "two" {
			t.Fatalf("sweep dropped the overwritten entry: %q %v", got, ok)
		}

		// 19ms: past the refreshed expiry (18ms).
		time.Sleep(7 * time.Millisecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d expired entries", n)
		}
	})
}

// Invalidation only deletes the entry. The key stays queued in the bucket that
// referenced it, and a sweep that reaches it skips it, because it is gone from the map.
func TestSweepSkipsInvalidatedKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const a = uint64(1)

		r := &recorder[uint64]{}
		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}.WithDefaults(), r)

		if err := c.Set(ctx, a, "one"); err != nil {
			t.Fatal(err)
		}

		r.deliver(a) // a peer invalidated the key; the bucket still references it

		if n := c.Len(); n != 0 {
			t.Fatalf("len = %d after invalidation", n)
		}

		// Sweep at the tick the key was queued for, then at the tick that drops the
		// bucket: neither may resurrect anything.
		time.Sleep(11 * time.Millisecond)
		c.Sweep()
		time.Sleep(2 * time.Millisecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("sweep resurrected %d entries", n)
		}
		if _, ok := c.Get(a); ok {
			t.Fatal("sweep resurrected an invalidated key")
		}
	})
}

// Writing the same key twice inside one tick leaves two references in one bucket. One
// sweep deletes the entry on the first reference and skips the second.
func TestSweepSameKeyTwiceInOneTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}.WithDefaults(), nil)

		if err := c.Set(ctx, a, "one"); err != nil {
			t.Fatal(err)
		}
		if err := c.Set(ctx, a, "two"); err != nil {
			t.Fatal(err)
		}
		if n := c.Len(); n != 1 {
			t.Fatalf("len = %d, want one entry", n)
		}

		time.Sleep(11 * time.Millisecond)
		c.Sweep()
		if n := c.Len(); n != 0 {
			t.Fatalf("sweep kept %d entries with two references", n)
		}

		// The bucket is dropped on the next tick; the leftover reference must not panic.
		time.Sleep(2 * time.Millisecond)
		c.Sweep()
		if n := c.Len(); n != 0 {
			t.Fatalf("len = %d after the bucket was dropped", n)
		}
	})
}

// Repeated sweeps must not evict a live entry early, nor lose track of it so
// that it never expires later.
func TestSweepDoesNotEvictLiveEntryAndKeepsTrackingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const a = uint64(1)

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: 2 * time.Millisecond,
		}, nil)

		if err := c.Set(ctx, a, "one"); err != nil {
			t.Fatal(err)
		}

		time.Sleep(9 * time.Millisecond)
		for range 5 {
			c.Sweep()
		}
		if got, ok := c.Get(a); !ok || got != "one" {
			t.Fatalf("sweep evicted a live entry: %q %v", got, ok)
		}

		time.Sleep(2 * time.Millisecond)
		c.Sweep()

		if n := c.Len(); n != 0 {
			t.Fatalf("entry stopped being tracked by sweep: %d entries left", n)
		}
	})
}

func TestSweepBulkMixedEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const n = 500

		c := qcache.NewCache(qcache.CacheConfig[uint64, string]{
			TTL:           10 * time.Millisecond,
			SweepInterval: time.Millisecond,
		}, nil)

		for i := range n {
			if err := c.Set(ctx, uint64(i), "old"); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(6 * time.Millisecond)
		for i := range n {
			if err := c.Set(ctx, uint64(n+i), "new"); err != nil {
				t.Fatal(err)
			}
		}

		// 11ms: "old" expired at 10ms, "new" is live until 16ms.
		time.Sleep(5 * time.Millisecond)
		c.Sweep()

		if got := c.Len(); got != n {
			t.Fatalf("len = %d, want %d live entries", got, n)
		}
		for i := range n {
			if _, ok := c.Get(uint64(i)); ok {
				t.Fatalf("expired entry %d survived the sweep", i)
			}
			if _, ok := c.Get(uint64(n + i)); !ok {
				t.Fatalf("live entry %d was swept", n+i)
			}
		}
	})
}

func TestDelPublishErrorStillDeletesLocally(t *testing.T) {
	ctx := context.Background()
	const a = uint64(1)

	boom := errors.New("boom")
	r := &recorder[uint64]{err: boom}
	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, r)

	if err := c.Set(ctx, a, "one"); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if err := c.Del(ctx, a); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if _, ok := c.Get(a); ok {
		t.Fatal("delete did not remove the local entry")
	}
}

func TestDelEmptyKeyListDoesNotPublish(t *testing.T) {
	r := &recorder[uint64]{}
	c := qcache.NewCache(qcache.CacheConfig[uint64, string]{}, r)

	if err := c.Del(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.calls()) != 0 {
		t.Fatal(r.calls())
	}
}

func BenchmarkCacheGetHit(b *testing.B) {
	const key = uint64(5)
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{}, nil)
	if err := c.Set(context.Background(), key, 1); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		c.Get(key)
	}
}

func BenchmarkCacheGetMiss(b *testing.B) {
	const key = uint64(5)
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{}, nil)

	b.ReportAllocs()
	for b.Loop() {
		c.Get(key)
	}
}

func BenchmarkCacheSet(b *testing.B) {
	const key = uint64(5)
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{}, nil)

	b.ReportAllocs()
	for b.Loop() {
		if err := c.Set(context.Background(), key, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheSetParallel(b *testing.B) {
	c := qcache.NewCache(qcache.CacheConfig[int, int]{}, nil)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if err := c.Set(context.Background(), i%64, i); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCacheSweep10K(b *testing.B) {
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{
		TTL:           time.Hour,
		SweepInterval: time.Minute,
	}, nil)
	ctx := context.Background()
	for i := range 10_000 {
		if err := c.Set(ctx, uint64(i), i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.Sweep()
	}
}

// Every entry is due for removal: the worst case for a sweep, since all buckets
// are due at once. Seeding runs with the timer stopped, so the reported
// allocations include it.
func BenchmarkCacheSweep10KAllExpired(b *testing.B) {
	const n = 10_000
	ctx := context.Background()

	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{
		TTL:           500 * time.Microsecond,
		SweepInterval: 10 * time.Microsecond,
	}, nil)
	seed := func() {
		for i := range n {
			if err := c.Set(ctx, uint64(i), i); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		seed()
		time.Sleep(time.Millisecond)
		b.StartTimer()

		c.Sweep()
	}
}

// Steady-state writes with a ttl: this is the path an expiry index has to pay for.
func BenchmarkCacheSetWithTTL(b *testing.B) {
	c := qcache.NewCache(qcache.CacheConfig[uint64, int]{
		TTL:           5 * time.Minute,
		SweepInterval: time.Minute,
	}, nil)

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := c.Set(context.Background(), uint64(i%1024), i); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheGetParallel(b *testing.B) {
	c := qcache.NewCache(qcache.CacheConfig[int, int]{}, nil)
	for i := range 64 {
		if err := c.Set(context.Background(), i, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			c.Get(i % 64)
		}
	})
}
