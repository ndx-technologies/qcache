package redis

import (
	"encoding/binary"
	"errors"
	"testing"
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

func TestMessageRoundTrip(t *testing.T) {
	want := message[uint64Key, *uint64Key]{
		Origin: 0x0123456789abcdef,
		Keys: []uint64Key{
			1,
			0,
			0x0102030405060708,
			^uint64Key(0),
		},
	}

	payload, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	var got message[uint64Key, *uint64Key]
	if err := got.UnmarshalBinary(payload); err != nil {
		t.Fatal(err)
	}
	if got.Origin != want.Origin {
		t.Fatal(got.Origin)
	}
	if len(got.Keys) != len(want.Keys) {
		t.Fatal(got.Keys)
	}
	for i := range want.Keys {
		if got.Keys[i] != want.Keys[i] {
			t.Fatal(got.Keys[i], want.Keys[i])
		}
	}
}

func TestDecodeMessageRejectsBadPayload(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		{0x05, 'a'},
		{0x01, 'a', 0x02},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5}, // zero origin, count 5, no keys
	} {
		var m message[uint64Key, *uint64Key]
		if err := m.UnmarshalBinary(payload); err == nil {
			t.Fatalf("decoded bad payload %v", payload)
		}
	}
}

type badAppendKey uint64

func (k badAppendKey) AppendBinary([]byte) ([]byte, error) { return nil, errors.New("append") }

func (k *badAppendKey) UnmarshalBinary([]byte) error { return nil }

type badUnmarshalKey uint64

func (k badUnmarshalKey) AppendBinary(b []byte) ([]byte, error) {
	return binary.BigEndian.AppendUint64(b, uint64(k)), nil
}

func (k *badUnmarshalKey) UnmarshalBinary([]byte) error { return errors.New("unmarshal") }

func TestMessageEmptyOriginAndKeys(t *testing.T) {
	want := message[uint64Key, *uint64Key]{}
	payload, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	var got message[uint64Key, *uint64Key]
	if err := got.UnmarshalBinary(payload); err != nil {
		t.Fatal(err)
	}
	if got.Origin != 0 || len(got.Keys) != 0 {
		t.Fatal(got)
	}
}

func TestAppendBinaryKeyError(t *testing.T) {
	m := message[badAppendKey, *badAppendKey]{
		Origin: 1,
		Keys:   []badAppendKey{1},
	}
	if _, err := m.MarshalBinary(); err == nil {
		t.Fatal("want error")
	}
}

func TestUnmarshalBinaryKeyError(t *testing.T) {
	payload, err := message[badUnmarshalKey, *badUnmarshalKey]{
		Origin: 1,
		Keys:   []badUnmarshalKey{1},
	}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	var got message[badUnmarshalKey, *badUnmarshalKey]
	if err := got.UnmarshalBinary(payload); err == nil {
		t.Fatal("want error")
	}
}

func TestUnmarshalBinaryRejectsEveryTruncation(t *testing.T) {
	payload, err := message[uint64Key, *uint64Key]{
		Origin: 2,
		Keys:   []uint64Key{1, 2},
	}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	for i := range len(payload) {
		var got message[uint64Key, *uint64Key]
		if err := got.UnmarshalBinary(payload[:i]); err == nil {
			t.Fatalf("accepted a %d/%d byte prefix", i, len(payload))
		}
	}
}

func TestUnmarshalBinaryRejectsTrailingBytes(t *testing.T) {
	payload, err := message[uint64Key, *uint64Key]{
		Origin: 1,
		Keys:   []uint64Key{1},
	}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	var got message[uint64Key, *uint64Key]
	if err := got.UnmarshalBinary(append(payload, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
}

func BenchmarkMessageMarshal(b *testing.B) {
	m := message[uint64Key, *uint64Key]{
		Origin: 3,
		Keys:   []uint64Key{1, 2, 3},
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := m.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessageUnmarshal(b *testing.B) {
	payload, err := message[uint64Key, *uint64Key]{
		Origin: 3,
		Keys:   []uint64Key{1, 2, 3},
	}.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		var m message[uint64Key, *uint64Key]
		if err := m.UnmarshalBinary(payload); err != nil {
			b.Fatal(err)
		}
	}
}
