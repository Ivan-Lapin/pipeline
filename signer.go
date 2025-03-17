package main

import (
	"fmt"
	"sync"
	"time"
)

// сюда писать код

type Pair struct {
	index int
	val   string
}

func SingleHash(in, out chan interface{}) {
	var m sync.Mutex
	index := 0

	for i := range in {
		go func(inCh interface{}, outCh chan interface{}, idx int) {
			data, _ := inCh.(string)
			data_crc32 := DataSignerCrc32(data)

			m.Lock()
			data_md5 := DataSignerMd5(data)
			m.Unlock()

			data_md5_crc32 := DataSignerCrc32(data_md5)
			result := data_crc32 + "~" + data_md5_crc32
			p := Pair{val: result, index: idx}

			fmt.Printf("%v  SingleHash data %v\n", inCh, inCh)
			fmt.Printf("%v  SingleHash md5(data) %v\n", inCh, data_md5)
			fmt.Printf("%v  crc32(md5(data)) %v\n", inCh, data_md5_crc32)
			fmt.Printf("%v  crc32(data) %v\n", inCh, data_crc32)
			fmt.Printf("%v  SingleHash result %v\n", inCh, result)
			outCh <- p
		}(i, out, index)
		index++
	}
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
		go func(fn job, inCh chan interface{}, outCh chan interface{}, num int) {
			defer wg.Done()
			defer close(outCh)
			fn(inCh, outCh)
			time.Sleep(5 * time.Second)
			fmt.Printf("Job %d worked\n", num)
		}(work, in, out, i)

		in = out

	}

	wg.Wait()
}

func main() {

}
