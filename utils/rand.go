package utils

import (
	"crypto/rand"
	"errors"
	"math/big"
)

const base62Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandBase62(length int) (string, error) {
	b, err := RandBytes(length)
	if err != nil {
		return "", err
	}

	charsetLen := big.NewInt(int64(len(base62Chars)))
	for i := 0; i < length; i++ {
		randomIndex, _ := rand.Int(rand.Reader, charsetLen) // nolint:gosec
		b[i] = base62Chars[randomIndex.Int64()]
	}

	return string(b), nil
}

func MustRandBase62(length int) string {
	res, err := RandBase62(length)
	if err != nil {
		panic(err)
	}
	return res
}

func RandBytes(length int) ([]byte, error) {
	if length <= 0 {
		return nil, errors.New("length must be positive")
	}

	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}

	return b, nil
}
