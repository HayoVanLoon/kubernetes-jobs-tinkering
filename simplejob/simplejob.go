package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const endAfter = 16

type item struct {
	Name string `json:"name"`
	N      int    `json:"n"`
	Ts     int64  `json:"ts"`
	Data   []byte `json:"data"`
}

func (i item) String() string {
	return fmt.Sprintf("{%s: %v [%x]}", i.Name, time.Unix(i.Ts / 1000, 0), i.Data)
}

func closeFn(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Print(err)
	}
}

func run(scheme, stream string) {
	var dataz []item
	cl := http.Client{}
	i := 0
	for p := 0; i < endAfter; {
		is, err := fetch(cl, scheme, stream, p)
		if err != nil {
			log.Fatal(err)
		}
		if len(is) > 0 {
			dataz = concat(dataz, is)
			i += len(is)
			p = dataz[i-1].N

			for _, it := range is {
				fmt.Println(it)
			}
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Printf("received %v items\n", i)
}

func concat(xs, ys []item) []item {
	zs := make([]item, len(xs)+len(ys))
	copy(zs, xs)
	copy(zs[len(xs):], ys)
	return zs
}

func fetch(cl http.Client, scheme, stream string, from int) ([]item, error) {
	url := scheme + "/streams/"+ stream + "/items"
	if from > 0 {
		url += "?from=" + strconv.Itoa(from)
	}
	resp, err := cl.Get(url)
	if err != nil {
		log.Fatal(err)
	}

	is, err := decode(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFn(resp.Body)

	return is, nil
}

func decode(r io.Reader) ([]item, error) {
	var is []item
	d := json.NewDecoder(r)
	if d.More() {
		err := d.Decode(&is)
		if err != nil {
			return nil, err
		}
	}
	return is, nil
}

func main() {
	stream := os.Getenv("STREAM")
	if stream == "" {
		stream = "medium"
	}

	scheme := os.Getenv("NUMBERS_SERVICE")
	if scheme == "" {
		scheme = "http://localhost:8080"
	}
	run(scheme, stream)
}
