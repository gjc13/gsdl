package utils

import (
	"hash/fnv"

	pager "github.com/gjc13/gsdl/pager"
)

func PadToPage(data []byte) []byte {
	origin_len := len(data)
	pad_len := int64(pager.PGSIZE) - int64(origin_len)
	if pad_len < 0 {
		panic("Cannot pad, too big input")
	}
	pad_data := make([]byte, pad_len)
	return append(data, pad_data...)
}

func ShrinkString(s string) string {
	s1 := []byte(s)
	for i, v := range s1 {
		if v == 0 {
			return string(s1[:i])
		}
	}
	return s
}

func HashString(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(ShrinkString(s)))
	return int64(h.Sum64())
}
