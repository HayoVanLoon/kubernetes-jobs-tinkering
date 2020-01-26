package main

import (
	"flag"
	"log"
	"net/http"
	"os"
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

type JobConfig struct {
	Name   string   `json:"name"`
	Dataz  string   `json:"dataz"`
	Stream string   `json:"stream"`
	DType  DataType `json:"dType"`
	MaxDuration int64
}

type Job struct {
	Name      string    `json:"name"`
	Scheduler string    `json:"scheduler"`
	Config    JobConfig `json:"config"`
}

const (
	delay    = 1 * time.Second
	endAfter = 16
)

var client = http.Client{}

func run(j RunnableJob) error {
	conf, err := j.GetConfig()
	if err != nil {
		return err
	}
	j.SetConfig(*conf)

	var all []Item
	for p := 0; len(all) < endAfter; {
		is, err := j.Fetch(p)
		if err != nil {
			log.Fatal(err)
		}
		if len(is) > 0 {
			p = is[len(is)-1].N
			all = concat(all, is)
		}

		time.Sleep(delay)
	}

	j.Report(all)
	return nil
}

func concat(xs, ys []Item) []Item {
	zs := make([]Item, len(xs)+len(ys))
	copy(zs, xs)
	copy(zs[len(xs):], ys)
	return zs
}

func main() {
	n := flag.String("job", "/jobs/123", "job name")
	flag.Parse()

	s := os.Getenv("SCHEDULER_SERVICE")
	if s == "" {
		s = "http://localhost:8080"
	}

	j := NewRunnableJob(*n, s, delay)

	err := run(j)
	if err != nil {
		log.Fatal(err)
	}
}
