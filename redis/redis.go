package redis

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisInvalidator publishes invalidations and dispatches incoming ones to
// the attached caches.
//
// One RedisInvalidator owns one subscription. Caches wired to different Invalidators
// can live in the same process and still exercise the real Redis broadcast
// path, which is what the tests do.
type RedisInvalidator[K wireKey, P keyPtr[K]] struct {
	client  goredis.UniversalClient
	channel string
	origin  uint64

	ready chan struct{}

	mu      sync.RWMutex
	targets []func(keys ...K)
}

func NewRedisInvalidator[K wireKey, P keyPtr[K]](
	ctx context.Context,
	client goredis.UniversalClient,
	channel string,
) *RedisInvalidator[K, P] {
	n := &RedisInvalidator[K, P]{
		client:  client,
		channel: channel,
		origin:  newOrigin(),
		ready:   make(chan struct{}),
	}

	go n.run(ctx, client.Subscribe(ctx, channel))
	return n
}

// Ready blocks until Redis confirms the subscription or ctx is done.
// A subscription that never gets confirmed surfaces as ctx.Err().
func (n *RedisInvalidator[K, P]) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ready:
		return nil
	}
}

func newOrigin() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return binary.BigEndian.Uint64(b[:])
	}
	// crypto/rand is expected to always work; keep the process unique if it
	// does not instead of taking the service down.
	return uint64(os.Getpid())<<32 ^ uint64(time.Now().UnixNano())
}

func (n *RedisInvalidator[K, P]) Subscribe(handler func(keys ...K)) {
	n.mu.Lock()
	n.targets = append(n.targets, handler)
	n.mu.Unlock()
}

func (n *RedisInvalidator[K, P]) Publish(ctx context.Context, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}

	b, err := message[K, P]{Origin: n.origin, Keys: keys}.MarshalBinary()
	if err != nil {
		return err
	}
	return n.client.Publish(ctx, n.channel, b).Err()
}

func (n *RedisInvalidator[K, P]) run(ctx context.Context, ps *goredis.PubSub) {
	defer ps.Close()

	if _, err := ps.Receive(ctx); err != nil {
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "qcache: cannot confirm redis subscription", "channel", n.channel, "error", err)
		}
	} else {
		close(n.ready)
	}

	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "qcache: redis subscription closed", "channel", n.channel)
				}
				return
			}
			n.dispatch(ctx, msg.Payload)
		}
	}
}

func (n *RedisInvalidator[K, P]) dispatch(ctx context.Context, payload string) {
	var m message[K, P]
	if err := m.UnmarshalBinary([]byte(payload)); err != nil {
		slog.ErrorContext(ctx, "qcache: bad invalidation message", "channel", n.channel, "error", err)
		return
	}
	if m.Origin == n.origin || len(m.Keys) == 0 {
		return
	}

	n.mu.RLock()
	targets := slices.Clone(n.targets)
	n.mu.RUnlock()

	for _, target := range targets {
		target(m.Keys...)
	}
}
