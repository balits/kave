package util

import (
	"crypto/rand"
	"encoding/binary"
)

// NextNonNullID új id-t generál, ami sosem lehet 0
func NextNonNullID() int64 {
	var buf [8]byte
	rand.Read(buf[:])
	id := int64(binary.BigEndian.Uint64(buf[:]))
	if id <= 0 {
		id = -id
	}
	return id
}
