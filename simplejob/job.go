package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

func decode(r io.Reader) ([]Item, error) {
	var is []Item
	d := json.NewDecoder(r)
	if d.More() {
		err := d.Decode(&is)
		if err != nil {
			return nil, err
		}
	}
	return is, nil
}

func (i Item) String() string {
	return fmt.Sprintf("{%s: %v [%x]}", i.Name, time.Unix(i.Ts/1000, 0), i.Data)
}

type RunnableJob interface {
	GetConfig() (*JobConfig, error)
	SetConfig(JobConfig)
	Fetch(int) ([]Item, error)
	Op([]Item)
	Report([]Item)
}

type runnableJob struct {
	Job
	delay time.Duration
}

func NewRunnableJob(name, scheduler string, delay time.Duration) RunnableJob {
	return &runnableJob{
		Job:   Job{Name: name, Scheduler: scheduler},
		delay: delay,
	}
}

func closeFn(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Print(err)
	}
}

func (j runnableJob) GetConfig() (*JobConfig, error) {
	resp, err := client.Get(j.Scheduler + j.Name + "/configs")
	if err != nil {
		return nil, err
	}
	defer closeFn(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received %s", resp.Status)
	}

	jc := &JobConfig{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, jc)
	if err != nil {
		return nil, err
	}
	return jc, nil
}

func (j *runnableJob) SetConfig(jc JobConfig) {
	j.Config = jc
}

func (j runnableJob) Fetch(from int) ([]Item, error) {
	url := j.Config.Dataz + j.Config.Stream + "/items"
	if from > 0 {
		url += "?from=" + strconv.Itoa(from)
	}
	log.Println("polling " + url)
	resp, err := client.Get(url)
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

func (j runnableJob) Report(is []Item) {
	fmt.Printf("received %v items\n", len(is))
	fmt.Printf("data type: %v\n", j.Config.DType)
	fmt.Println("--------------------------------")
	j.Op(is)
	fmt.Println("--------------------------------")
}

func (j runnableJob) Op(is []Item) {
	var printFn func([]byte) string
	switch j.Config.DType {
	case SawingNumbersType:
		printFn = func(bs []byte) string { return fmt.Sprintf("%v", int(bs[0])) }
	case RandomNumbersType:
		printFn = func(bs []byte) string { return fmt.Sprintf("%v", int(bs[0])) }
	case RandomLettersType:
		printFn = func(bs []byte) string { return fmt.Sprintf("%s", string(bs)) }
	default:
		panic("not implemented!")
	}

	for _, i := range is {
		fmt.Printf("%v: %s\n", time.Unix(i.Ts/1000, 0), printFn(i.Data))
	}
}
