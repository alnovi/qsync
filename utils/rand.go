package utils

import (
	"crypto/rand"
	"math/big"
)

const base62Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandBase62(length int) string {
	b := RandBytes(length)

	charsetLen := big.NewInt(int64(len(base62Chars)))
	for i := 0; i < length; i++ {
		randomIndex, _ := rand.Int(rand.Reader, charsetLen) // nolint:gosec
		b[i] = base62Chars[randomIndex.Int64()]
	}

	return string(b)
}

func RandBytes(length int) []byte {
	if length <= 0 {
		panic("length must be positive")
	}

	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}
