package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	// Заполнить здесь ассерт, что b содержит zero и что b содержит one

	con_zero := false
	con_one := true

	for _, v := range b {
		if v == zero {
			con_zero = true
		}
		if v == one {
			con_one = true
		}
	}

	assert.True(t, con_zero, "zero bytes should be filled")
	assert.True(t, con_one, "one bytes should be filled")
}
