package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func newItem(n int, dType DataType) Item {
	it := Item{N: n, Ts: time.Now().UnixNano() / 1000000,}
	switch dType {
	case SawingNumbersType:
		it.Data = []byte{byte(n % 10)}
	case RandomNumbersType:
		it.Data = []byte{byte(rand.Intn(10))}
	case RandomLettersType:
		it.Data = []byte{byte(0x41 + rand.Intn(25))}
	default:
		panic("not implemented!")
	}
	return it
}

type buffer struct {
	data     []Item // synced
	capacity int
	rh       int // synced
	n        int // synced
	mx       sync.RWMutex
}

func newBuffer(capacity int) buffer {
	return buffer{data: make([]Item, capacity), capacity: capacity, mx: sync.RWMutex{}}
}

func (b *buffer) addItem(it Item) {
	b.mx.Lock()
	wh := b.n % b.capacity
	if b.n > 0 && wh == b.rh {
		b.rh = (b.rh + 1) % b.capacity
	}
	b.data[wh] = it
	b.n += 1
	b.mx.Unlock()
}

func (b *buffer) readFrom(from int) []Item {
	b.mx.RLock()
	var is []Item
	for i := 0; i < b.capacity; i += 1 {
		it := b.data[(b.rh+i)%b.capacity]
		if it.N > from {
			is = append(is, it)
		}
	}
	b.mx.RUnlock()
	return is
}

type BufferedItemStream interface {
	Name() string
	ReadFrom(int) []Item
}

type stream struct {
	name  string
	DType DataType
	delay time.Duration
	buf   buffer
	n     int // synced
	mx    sync.RWMutex
}

func newStream(name string, dType DataType, delay time.Duration, bufferSize int) BufferedItemStream {
	s := &stream{
		name:  name,
		DType: dType,
		delay: delay,
		buf:   newBuffer(bufferSize),
		n:     0,
		mx:    sync.RWMutex{},
	}
	go s.run()
	return s
}

func (s *stream) run() {
	for ; ; {
		s.mx.Lock()
		it := newItem(s.n, s.DType)
		s.buf.addItem(it)
		s.n += 1
		s.mx.Unlock()

		time.Sleep(s.delay)
	}
}

func (s stream) Name() string {
	return s.name
}

func (s *stream) ReadFrom(from int) []Item {
	is := s.buf.readFrom(from)
	for i, it := range is {
		is[i].Name = fmt.Sprintf("%s/items/%v", s.Name(), it.N)
	}
	return is
}
