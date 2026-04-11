package util

import (
	"crypto/rand"
	"encoding/binary"
)

// NextNonNullID új id-t generál, ami sosem lehet 0.
// mivel a crypto/rand-ot használja, ezért ha elhasal
// az id generálás, azonnal crashel a program.
func NextNonNullID() int64 {
	var buf [8]byte
	rand.Read(buf[:])
	id := int64(binary.BigEndian.Uint64(buf[:]))
	if id == 0 {
		// chances of id == 0 is 1 out of 2^64 extremely unlikely,
		return NextNonNullID()
	}
	if id < 0 {
		id = -id
	}
	return id
}
