package main

import (
	"fmt"
	"sync"
)

// сюда писать код

func SingleHash(in, out chan interface{}) {

}

func MultiHash(in, out chan interface{}) {

}

func CombineResults(in, out chan interface{}) {

}

func ExecutePipeline(jobs ...job) {
	var wg sync.WaitGroup

	in := make(chan interface{})

	for i, work := range jobs {

		out := make(chan interface{})

		wg.Add(1)
		go func(fn job, inCh chan interface{}, outCh chan interface{}) {
			defer wg.Done()
			defer close(outCh)
			fn(inCh, outCh)
		}(work, in, out)

		in = out

		fmt.Printf("Job %d worked\n", i)
	}

	wg.Wait()
}

func main() {

}
