package redis

import (
	"encoding"
	"encoding/binary"
	"errors"
)

// maxFieldSize is the largest value a uint32 length/count field can hold.
const maxFieldSize = uint64(1<<32 - 1)

// wireKey is what the Redis transport needs from a cache key.
type wireKey interface {
	comparable
	encoding.BinaryAppender
}

// keyPtr is the pointer form of K. Key types usually implement
// UnmarshalBinary on *K, which a value-typed K cannot satisfy.
type keyPtr[K wireKey] interface {
	*K
	encoding.BinaryUnmarshaler
}

type message[K wireKey, P keyPtr[K]] struct {
	Origin uint64
	Keys   []K
}

func (m message[K, P]) MarshalBinary() ([]byte, error) {
	if uint64(len(m.Keys)) > maxFieldSize {
		return nil, errors.New("qcache/redis: too many keys")
	}

	b := make([]byte, 0, 8+4+len(m.Keys)*16)
	b = binary.BigEndian.AppendUint64(b, m.Origin)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Keys)))

	for _, key := range m.Keys {
		off := len(b)
		b = append(b, 0, 0, 0, 0) // length placeholder

		var err error
		b, err = key.AppendBinary(b)
		if err != nil {
			return nil, err
		}

		n := len(b) - off - 4
		if uint64(n) > maxFieldSize {
			return nil, errors.New("qcache/redis: key too long")
		}
		binary.BigEndian.PutUint32(b[off:], uint32(n))
	}
	return b, nil
}

func (m *message[K, P]) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return errors.New("qcache/redis: truncated origin")
	}
	m.Origin = binary.BigEndian.Uint64(data)
	data = data[8:]

	if len(data) < 4 {
		return errors.New("qcache/redis: bad key count")
	}
	count := binary.BigEndian.Uint32(data)
	data = data[4:]
	if uint64(count) > uint64(len(data))/4 {
		return errors.New("qcache/redis: bad key count")
	}

	m.Keys = make([]K, int(count))
	for i := range int(count) {
		raw, rest, err := readBytes(data)
		if err != nil {
			return err
		}

		p := P(&m.Keys[i])
		if err := p.UnmarshalBinary(raw); err != nil {
			return err
		}

		data = rest
	}
	if len(data) != 0 {
		return errors.New("qcache/redis: trailing bytes")
	}
	return nil
}

func readBytes(src []byte) ([]byte, []byte, error) {
	if len(src) < 4 {
		return nil, nil, errors.New("qcache/redis: truncated length")
	}
	n := binary.BigEndian.Uint32(src)
	src = src[4:]
	if uint64(n) > uint64(len(src)) {
		return nil, nil, errors.New("qcache/redis: truncated bytes")
	}
	size := int(n)
	return src[:size], src[size:], nil
}
