package qcache

import (
	"context"
	"sync"
	"time"
)

type Broadcaster[K any] interface {
	Publish(ctx context.Context, keys ...K) error
	Subscribe(handler func(keys ...K))
}

type CacheConfig[K comparable, T any] struct {
	TTL           time.Duration
	SweepInterval time.Duration
}

func (s CacheConfig[K, T]) WithDefaults() CacheConfig[K, T] {
	if s.TTL == 0 {
		s.TTL = time.Minute * 5
	}
	if s.SweepInterval == 0 {
		s.SweepInterval = min(time.Minute, s.TTL)
	}
	return s
}

type entry[T any] struct {
	value     T
	expiresAt time.Time // zero value means no expiry
}

func (e entry[T]) expired(now time.Time) bool { return !e.expiresAt.IsZero() && now.After(e.expiresAt) }

type expiryBucket[K comparable] struct {
	tick int64
	keys []K
}

// Cache stores data locally wiht TTL and broadcasts key changes to invalidate in peers.
type Cache[K comparable, T any] struct {
	config      CacheConfig[K, T]
	broadcaster Broadcaster[K]
	mu          sync.RWMutex
	entries     map[K]entry[T]
	buckets     []expiryBucket[K]
}

func NewCache[K comparable, T any](config CacheConfig[K, T], broadcaster Broadcaster[K]) *Cache[K, T] {
	c := &Cache[K, T]{
		config:  config,
		entries: make(map[K]entry[T]),
	}
	if broadcaster != nil {
		c.broadcaster = broadcaster
		c.broadcaster.Subscribe(c.invalidate)
	}
	return c
}

func (c *Cache[K, T]) Get(key K) (T, bool) {
	now := time.Now()

	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		var zero T
		return zero, false
	}
	if !e.expired(now) {
		return e.value, true
	}

	c.mu.Lock()
	if cur, ok := c.entries[key]; ok && cur.expiresAt.Equal(e.expiresAt) {
		delete(c.entries, key)
	}
	c.mu.Unlock()

	var zero T
	return zero, false
}

func (c *Cache[K, T]) Set(ctx context.Context, key K, value T) error {
	now := time.Now()
	var expiresAt time.Time
	if c.config.TTL > 0 {
		expiresAt = now.Add(c.config.TTL)
	}

	c.mu.Lock()
	c.entries[key] = entry[T]{value: value, expiresAt: expiresAt}
	if !expiresAt.IsZero() && c.config.SweepInterval > 0 {
		tick := expiresAt.UnixNano() / c.config.SweepInterval.Nanoseconds()
		c.enqueue(tick, key)
	}
	c.mu.Unlock()

	if c.broadcaster != nil {
		return c.broadcaster.Publish(ctx, key)
	}

	return nil
}

func (c *Cache[K, T]) enqueue(tick int64, key K) {
	if n := len(c.buckets); n > 0 && c.buckets[n-1].tick == tick {
		c.buckets[n-1].keys = append(c.buckets[n-1].keys, key)
		return
	}

	c.buckets = append(c.buckets, expiryBucket[K]{tick: tick, keys: []K{key}})
}

func (c *Cache[K, T]) Del(ctx context.Context, keys ...K) error {
	c.mu.Lock()
	for _, key := range keys {
		delete(c.entries, key)
	}
	c.mu.Unlock()

	if c.broadcaster != nil && len(keys) > 0 {
		return c.broadcaster.Publish(ctx, keys...)
	}

	return nil
}

func (c *Cache[K, T]) invalidate(keys ...K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, key := range keys {
		delete(c.entries, key)
	}
}

// Len returns the number of stored entries, including entries that have expired but have not been removed yet.
func (c *Cache[K, T]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Sweep removes expired entries.
func (c *Cache[K, T]) Sweep() {
	tick := c.config.SweepInterval.Nanoseconds()
	if tick <= 0 {
		return
	}

	now := time.Now()
	nowBucket := now.UnixNano() / tick

	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.buckets) > 0 && c.buckets[0].tick < nowBucket {
		c.sweepKeys(c.buckets[0].keys, now)
		c.buckets[0] = expiryBucket[K]{}
		c.buckets = c.buckets[1:]
	}

	if len(c.buckets) > 0 && c.buckets[0].tick == nowBucket {
		c.sweepKeys(c.buckets[0].keys, now)
	}
}

// sweepKeys deletes the keys that are expired. A key can sit in a bucket that no
// longer matches its expiry, because overwriting and invalidation do not rewrite the
// index, so only what is expired is ever deleted.
func (c *Cache[K, T]) sweepKeys(keys []K, now time.Time) {
	for _, key := range keys {
		if e, ok := c.entries[key]; ok && e.expired(now) {
			delete(c.entries, key)
		}
	}
}

func (c *Cache[K, T]) RunScheduledSweep() {
	if c.config.SweepInterval <= 0 {
		return
	}

	ticker := time.NewTicker(c.config.SweepInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.Sweep()
	}
}
