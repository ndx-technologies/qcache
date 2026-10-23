package redis_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ndx-technologies/qcache"
	qredis "github.com/ndx-technologies/qcache/redis"
)

type uint64Key uint64

func (k uint64Key) AppendBinary(b []byte) ([]byte, error) {
	return binary.BigEndian.AppendUint64(b, uint64(k)), nil
}

func (k *uint64Key) UnmarshalBinary(b []byte) error {
	if len(b) != 8 {
		return errors.New("bad key length")
	}
	*k = uint64Key(binary.BigEndian.Uint64(b))
	return nil
}

func testRedisClient(t testing.TB) *goredis.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("redis")
	}

	u := os.Getenv("REDIS_URL")
	if u == "" {
		u = "redis://127.0.0.1:6379/0"
	}
	cfg, err := goredis.ParseURL(u)
	if err != nil {
		t.Fatal(err)
	}
	rdb := goredis.NewClient(cfg)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis unavailable: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func testChannel(t testing.TB) string {
	t.Helper()

	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "qcache:test:" + hex.EncodeToString(b[:])
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before the deadline")
}

// newInstance is one process: its own Redis connection and its own subscription.
// Two of them talk through real sockets and real Redis broadcasts.
func newInstanceWithConn(t *testing.T, ctx context.Context, channel string) (*qcache.Cache[uint64Key, string], *qredis.RedisInvalidator[uint64Key, *uint64Key], *goredis.Client) {
	t.Helper()

	client := testRedisClient(t)
	inv := qredis.NewRedisInvalidator[uint64Key](ctx, client, channel)
	if err := inv.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	var config qcache.CacheConfig[uint64Key, string]
	config = config.WithDefaults()

	c := qcache.NewCache(config, inv)
	return c, inv, client
}

func newInstance(t *testing.T, ctx context.Context, channel string) *qcache.Cache[uint64Key, string] {
	t.Helper()
	c, _, _ := newInstanceWithConn(t, ctx, channel)
	return c
}

// wireMessage builds the payload independently of the implementation, so tests
// can publish from outside the invalidator.
func wireMessage(origin uint64, keys ...uint64Key) []byte {
	b := binary.BigEndian.AppendUint64(nil, origin)
	b = binary.BigEndian.AppendUint32(b, uint32(len(keys)))
	for _, key := range keys {
		b = binary.BigEndian.AppendUint32(b, 8)
		b = binary.BigEndian.AppendUint64(b, uint64(key))
	}
	return b
}

func TestRedisInvalidationAcrossInstances(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1 := newInstance(t, ctx, channel)
	c2 := newInstance(t, ctx, channel)
	client := testRedisClient(t)

	t.Run("SetInvalidatesOtherInstance", func(t *testing.T) {
		const key = uint64Key(1)

		if err := c2.Set(ctx, key, "old-in-2"); err != nil {
			t.Fatal(err)
		}
		// Let the warm-up broadcast reach the other instance.
		time.Sleep(100 * time.Millisecond)

		if err := c1.Set(ctx, key, "new"); err != nil {
			t.Fatal(err)
		}
		if got, ok := c1.Get(key); !ok || got != "new" {
			t.Fatal(got, ok)
		}
		eventually(t, 2*time.Second, func() bool {
			_, ok := c2.Get(key)
			return !ok
		})
	})

	t.Run("SetDoesNotInvalidateItself", func(t *testing.T) {
		const key = uint64Key(2)

		if err := c1.Set(ctx, key, "one"); err != nil {
			t.Fatal(err)
		}
		// The instance receives its own broadcast and has to ignore it.
		time.Sleep(100 * time.Millisecond)
		if got, ok := c1.Get(key); !ok || got != "one" {
			t.Fatal(got, ok)
		}
	})

	t.Run("DelInvalidatesOtherInstance", func(t *testing.T) {
		const key = uint64Key(3)

		if err := c2.Set(ctx, key, "two"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		if err := c1.Del(ctx, key); err != nil {
			t.Fatal(err)
		}
		eventually(t, 2*time.Second, func() bool {
			_, ok := c2.Get(key)
			return !ok
		})
	})

	t.Run("ChannelsAreIsolated", func(t *testing.T) {
		const key = uint64Key(4)

		other := newInstance(t, ctx, testChannel(t))

		if err := c1.Set(ctx, key, "one"); err != nil {
			t.Fatal(err)
		}
		if err := other.Set(ctx, key, "other"); err != nil {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Millisecond)
		if got, ok := c1.Get(key); !ok || got != "one" {
			t.Fatal(got, ok)
		}
		if got, ok := other.Get(key); !ok || got != "other" {
			t.Fatal(got, ok)
		}
	})

	t.Run("BadMessageIsIgnored", func(t *testing.T) {
		const key = uint64Key(5)

		if err := c1.Set(ctx, key, "one"); err != nil {
			t.Fatal(err)
		}
		if err := client.Publish(ctx, channel, "not-json").Err(); err != nil {
			t.Fatal(err)
		}

		time.Sleep(100 * time.Millisecond)
		if got, ok := c1.Get(key); !ok || got != "one" {
			t.Fatal(got, ok)
		}
	})

	t.Run("RedisStoresNoValues", func(t *testing.T) {
		const key = uint64Key(6)

		if err := c1.Set(ctx, key, "value"); err != nil {
			t.Fatal(err)
		}

		// The value lives in the process, not in a Redis key.
		if n, err := client.Exists(ctx, strconv.FormatUint(uint64(key), 10)).Result(); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal("redis holds a value key, want invalidation broadcasts only")
		}
	})
}

func TestInstancesUseSeparateConnectionsAndSubscriptions(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	_, _, client1 := newInstanceWithConn(t, ctx, channel)
	_, _, client2 := newInstanceWithConn(t, ctx, channel)
	_, _, client3 := newInstanceWithConn(t, ctx, channel)

	clientID := func(client *goredis.Client) int64 {
		t.Helper()
		id, err := client.Do(ctx, "CLIENT", "ID").Int64()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	ids := []int64{clientID(client1), clientID(client2), clientID(client3)}
	for i := range ids {
		for j := i + 1; j < len(ids); j++ {
			if ids[i] == ids[j] {
				t.Fatalf("instances share a connection: %v", ids)
			}
		}
	}

	subs, err := client1.PubSubNumSub(ctx, channel).Result()
	if err != nil {
		t.Fatal(err)
	}
	if subs[channel] != 3 {
		t.Fatalf("server sees %d subscribers, want 3", subs[channel])
	}
}

func TestBulkDelInvalidatesEveryKey(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1 := newInstance(t, ctx, channel)
	c2 := newInstance(t, ctx, channel)

	const (
		k1 = uint64Key(1)
		k2 = uint64Key(2)
	)

	if err := c2.Set(ctx, k1, "one"); err != nil {
		t.Fatal(err)
	}
	if err := c2.Set(ctx, k2, "two"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := c1.Del(ctx, k1, k2); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		_, ok1 := c2.Get(k1)
		_, ok2 := c2.Get(k2)
		return !ok1 && !ok2
	})
}

func TestThreeInstancesReceiveTheSameInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1 := newInstance(t, ctx, channel)
	c2 := newInstance(t, ctx, channel)
	c3 := newInstance(t, ctx, channel)

	const key = uint64Key(1)
	if err := c2.Set(ctx, key, "two"); err != nil {
		t.Fatal(err)
	}
	if err := c3.Set(ctx, key, "three"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := c1.Set(ctx, key, "one"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		_, ok2 := c2.Get(key)
		_, ok3 := c3.Get(key)
		return !ok2 && !ok3
	})
}

func TestExternalPublisherIsDecoded(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1, _, client := newInstanceWithConn(t, ctx, channel)
	c2 := newInstance(t, ctx, channel)

	const key = uint64Key(1)
	if err := c2.Set(ctx, key, "two"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := c1.Set(ctx, key, "one"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := client.Publish(ctx, channel, wireMessage(0xdeadbeef, key)).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		_, ok := c1.Get(key)
		return !ok
	})
}

func TestMalformedMessagesAreIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1, _, client := newInstanceWithConn(t, ctx, channel)

	const key = uint64Key(1)
	if err := c1.Set(ctx, key, "one"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	payloads := [][]byte{
		nil,
		{0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5}, // zero origin, count 5, no keys
		append(wireMessage(1, key), 0),       // trailing byte
	}
	for _, payload := range payloads {
		if err := client.Publish(ctx, channel, payload).Err(); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	if got, ok := c1.Get(key); !ok || got != "one" {
		t.Fatal(got, ok)
	}
}

func TestBinarySafeKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1 := newInstance(t, ctx, channel)
	c2 := newInstance(t, ctx, channel)

	const key = uint64Key(0x000a0d00ff000001)

	if err := c2.Set(ctx, key, "old"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := c1.Set(ctx, key, "new"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		_, ok := c2.Get(key)
		return !ok
	})
}

func TestPublishWithoutKeysIsNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, inv, _ := newInstanceWithConn(t, ctx, testChannel(t))
	if err := inv.Publish(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReconnectAfterConnectionKill(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := testChannel(t)
	c1, _, client1 := newInstanceWithConn(t, ctx, channel)
	c2, _, client2 := newInstanceWithConn(t, ctx, channel)

	if err := client1.Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err(); err != nil {
		t.Fatal(err)
	}

	eventually(t, 5*time.Second, func() bool {
		subs, err := client2.PubSubNumSub(ctx, channel).Result()
		return err == nil && subs[channel] == 2
	})

	const key = uint64Key(1)
	if err := c2.Set(ctx, key, "old"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := c1.Set(ctx, key, "new"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, func() bool {
		_, ok := c2.Get(key)
		return !ok
	})
}

type badPublisherKey string

func (badPublisherKey) AppendBinary([]byte) ([]byte, error) { return nil, errors.New("boom") }

func (*badPublisherKey) UnmarshalBinary([]byte) error { return nil }

func TestPublishReturnsKeyMarshalError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := testRedisClient(t)
	inv := qredis.NewRedisInvalidator[badPublisherKey](ctx, client, testChannel(t))
	if err := inv.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	if err := inv.Publish(ctx, badPublisherKey("x")); err == nil {
		t.Fatal("want error")
	}
}

func BenchmarkRedisPublish(b *testing.B) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	inv := qredis.NewRedisInvalidator[uint64Key](ctx, testRedisClient(b), testChannel(b))
	if err := inv.Ready(ctx); err != nil {
		b.Fatal(err)
	}

	const key = uint64Key(1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := inv.Publish(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisInvalidationRoundTrip(b *testing.B) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	channel := testChannel(b)
	pub := qredis.NewRedisInvalidator[uint64Key](ctx, testRedisClient(b), channel)
	sub := qredis.NewRedisInvalidator[uint64Key](ctx, testRedisClient(b), channel)
	if err := sub.Ready(ctx); err != nil {
		b.Fatal(err)
	}

	got := make(chan struct{}, 1)
	sub.Subscribe(func(keys ...uint64Key) {
		select {
		case got <- struct{}{}:
		default:
		}
	})

	const key = uint64Key(1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := pub.Publish(ctx, key); err != nil {
			b.Fatal(err)
		}
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			b.Fatal("invalidation timeout")
		}
	}
}

func TestRedisInvalidatorReadyFailsWithoutRedis(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	// The constructor itself never blocks or fails; the connection problem shows up
	// when a caller asks for the subscription to be confirmed.
	inv := qredis.NewRedisInvalidator[uint64Key](ctx, client, "test")
	if err := inv.Ready(ctx); err == nil {
		t.Fatal("want error")
	}
}
