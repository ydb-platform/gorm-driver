package builders

import (
	"strings"
	"sync"
)

var builders sync.Pool

func Get() *strings.Builder {
	v := builders.Get()
	if v == nil {
		v = new(strings.Builder)
	}
	return v.(*strings.Builder)
}

func Put(b *strings.Builder) {
	b.Reset()
	builders.Put(b)
}
