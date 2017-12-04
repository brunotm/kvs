package utils

import "encoding/binary"

// BytesToUint64 decodes a uint64 from a byte slice
func BytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// Uint64ToBytes encodes a uint64 into a byte slice
func Uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, u)
	return buf
}

// CopyBytes copies the given byte slice
func CopyBytes(b []byte) (c []byte) {
	c = make([]byte, len(b))
	copy(c, b)
	return c
}
