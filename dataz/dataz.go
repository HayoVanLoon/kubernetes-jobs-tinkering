package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	bufferSize   = 8
	defaultDelay = 2 * time.Second
)

var pathRegex = regexp.MustCompile("/streams/(?P<stream>.+)/items")

type generatingHandler struct {
	streams map[string]*stream
	delay   time.Duration
}

func newGeneratingHandler(streams []string, delays []time.Duration) *generatingHandler {
	ss := make(map[string]*stream)
	for i := range streams {
		ss[streams[i]] = newStream(delays[i])
	}
	return &generatingHandler{streams: ss}
}

type stream struct {
	buf   buffer
	n     int // synced
	delay time.Duration
	mx    sync.RWMutex
}

func newStream(delay time.Duration) *stream {
	return &stream{
		buf:   newBuffer(bufferSize),
		n:     0,
		delay: delay,
		mx:    sync.RWMutex{},
	}
}

type buffer struct {
	data []item // synced
	size int
	rh   int // synced
	n    int // synced
	mx   sync.RWMutex
}

func newBuffer(size int) buffer {
	return buffer{data: make([]item, size), size: size, mx: sync.RWMutex{}}
}

type item struct {
	Name string `json:"name"`
	N    int    `json:"n"`
	Ts   int64  `json:"ts"`
	Data []byte `json:"data"`
}

func (s *stream) loop() {
	for ; ; {
		s.mx.Lock()
		it := generateItem(s.n)
		s.buf.addItem(it)
		s.n += 1
		s.mx.Unlock()

		time.Sleep(s.delay)
	}
}

func (b *buffer) addItem(it item) {
	b.mx.Lock()
	wh := b.n % b.size
	if b.n > 0 && wh == b.rh {
		b.rh = (b.rh + 1) % b.size
	}
	b.data[wh] = it
	b.n += 1
	b.mx.Unlock()
}

func (b *buffer) readFrom(from int) []item {
	b.mx.RLock()
	var is []item
	for i := 0; i < b.size; i += 1 {
		it := b.data[(b.rh+i)%b.size]
		if it.N > from {
			is = append(is, it)
		}
	}
	b.mx.RUnlock()
	return is
}

func generateItem(n int) item {
	return item{
		N:    n,
		Ts:   time.Now().UnixNano() / 1000000,
		Data: []byte(strconv.Itoa(n % 10)),
	}
}

func (h *generatingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m := pathRegex.FindStringSubmatch(r.URL.Path)
	if len(m) >= 2 {
		stream := m[1]
		if _, ok := h.streams[stream]; !ok {
			http.NotFound(w, r)
			return
		}
		from, err := getFromParameter(r)
		if err != nil {
			badRequest(w)
			return
		}

		h.handleSearchNumbers(w, stream, from)
	} else {
		http.NotFound(w, r)
	}
}

func getFromParameter(r *http.Request) (int, error) {
	for k, vs := range r.URL.Query() {
		if k == "from" && len(vs) > 0 {
			f, err := strconv.Atoi(vs[0])
			if err != nil {
				return -1, err
			}
			return f, nil
		}
	}
	return 0, nil
}

func (h *generatingHandler) handleSearchNumbers(w http.ResponseWriter, stream string, from int) {
	is := h.streams[stream].buf.readFrom(from)
	is2 := make([]item, len(is))
	for i, it := range is {
		is2[i] = it
		is2[i].Name = fmt.Sprintf("/streams/%s/items/%v", stream, it.N)
	}
	msg, err := json.Marshal(is2)
	if err != nil {
		log.Println(err)
	}
	if _, err := w.Write(msg); err != nil {
		log.Println(err)
	}
}

func badRequest(w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadRequest)
}

func main() {
	h := newGeneratingHandler(
		[]string{"fast", "medium", "slow"},
		[]time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second},
	)
	for _, s := range h.streams {
		go s.loop()
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(":"+port, h))
}
