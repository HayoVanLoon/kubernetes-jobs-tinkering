package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

type DataType int

const (
	SawingNumbersType DataType = iota + 1
	RandomNumbersType
	RandomLettersType
)

type Item struct {
	Name string `json:"name"`
	N    int    `json:"n"`
	Ts   int64  `json:"ts"`
	Data []byte `json:"data"`
}

var pathRegex = regexp.MustCompile("(?P<stream>/streams/.+)/items")

type generatingHandler struct {
	streams map[string]BufferedItemStream
}

func newGeneratingHandler(streams map[string]BufferedItemStream) *generatingHandler {
	return &generatingHandler{streams: streams}
}

func (h *generatingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m := pathRegex.FindStringSubmatch(r.URL.Path)
	if len(m) >= 2 {
		stream := m[1]
		if _, ok := h.streams[stream]; !ok {
			log.Println("404: " + r.URL.String())
			http.NotFound(w, r)
			return
		}
		from, err := getFromParameter(r)
		if err != nil {
			badRequest(w)
			return
		}

		is := h.streams[stream].ReadFrom(from)
		writeJson(w, is)
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

func writeJson(w http.ResponseWriter, v interface{}) {
	msg, err := json.Marshal(v)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(msg); err != nil {
		log.Println(err)
	}
}

func badRequest(w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadRequest)
}

func mapBy(f func(BufferedItemStream) string, ss []BufferedItemStream) map[string]BufferedItemStream {
	m := make(map[string]BufferedItemStream)
	for _, s := range ss {
		m[f(s)] = s
	}
	return m
}

func main() {
	streams := mapBy(BufferedItemStream.Name, []BufferedItemStream{
		newStream("/streams/fast", RandomNumbersType, 1*time.Second, 8),
		newStream("/streams/medium", RandomLettersType, 2*time.Second, 8),
		newStream("/streams/slow", SawingNumbersType, 4*time.Second, 8),
	})

	h := newGeneratingHandler(streams)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(":"+port, h))
}
