package main

import (
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"net/http"
	"os"
	"regexp"
)

type DataType int

const (
	SawingNumbersType DataType = iota + 1
	RandomNumbersType
	RandomLettersType
)

type JobConfig struct {
	Name   string   `json:"name"`
	Dataz  string   `json:"dataz"`
	Stream string   `json:"stream"`
	DType  DataType `json:"dType"`
}

type Job struct {
	Name      string    `json:"name"`
	Scheduler string    `json:"scheduler"`
	Config    JobConfig `json:"config"`
}

type scheduler struct {
	configs map[string]JobConfig
	jobs    map[string]Job
}

var jobConfigsPath = regexp.MustCompile("(?P<job>/jobs/.+)/configs$")

func (h *scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.Path)
	if m := jobConfigsPath.FindStringSubmatch(r.URL.Path); len(m) >= 2 {
		h.handleGetConfigs(w, r, m[1])
	} else if r.URL.Path == "/configs:poll" {
		h.pollStart(w)
	} else {
		http.NotFound(w, r)
	}
}

func (h *scheduler) handleGetConfigs(w http.ResponseWriter, r *http.Request, name string) {
	if j, ok := h.jobs[name]; ok {
		if jc, ok := h.configs[j.Config.Name]; ok {
			writeJson(w, jc)
			return
		}
	}
	http.NotFound(w, r)
}

func (h *scheduler) pollStart(w http.ResponseWriter) {
	for name, cfg := range h.configs {
		found := false
		for _, j := range h.jobs {
			if found = name == j.Config.Name; found {
				break
			}
		}
		if found {
			w.WriteHeader(http.StatusPreconditionFailed)
		} else {
			j := Job{Name: uuid.New().String(), Config: cfg}
			h.jobs[j.Name] = j
			// TODO: actually start job
			w.WriteHeader(http.StatusOK)
			writeJson(w, j)
		}
	}
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

func mapBy(f func(JobConfig) string, ss []JobConfig) map[string]JobConfig {
	m := make(map[string]JobConfig)
	for _, s := range ss {
		m[f(s)] = s
	}
	return m
}

func main() {
	d := os.Getenv("DATAZ_SERVICE")
	if d == "" {
		d = "http://localhost:8081"
	}

	jcs := mapBy(func(j JobConfig) string { return j.Name }, []JobConfig{
		{Name: "/configs/123", Dataz: d, Stream: "/streams/fast", DType: RandomNumbersType,},
		{Name: "/configs/456", Dataz: d, Stream: "/streams/medium", DType: RandomLettersType,},
		{Name: "/configs/789", Dataz: d, Stream: "/streams/slow", DType: SawingNumbersType,},
	})

	tmp := map[string]Job{
		"/jobs/123": {Name: "/jobs/123", Config: jcs["/configs/123"]},
		"/jobs/456": {Name: "/jobs/456", Config: jcs["/configs/456"]},
		"/jobs/789": {Name: "/jobs/789", Config: jcs["/configs/789"]},
	}

	h := &scheduler{configs: jcs, jobs: tmp}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(":"+port, h))
}
