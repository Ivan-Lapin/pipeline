package main

import (
	"fmt"
	"strconv"
	"sync"
)

type Pair struct {
	index int
	val   string
}

func SingleHash(in, out chan interface{}) {
	var m sync.Mutex
	var wg sync.WaitGroup
	index := 0

	for val := range in {
		wg.Add(1)
		go func(value interface{}, outCh chan interface{}, idx int) {
			defer wg.Done()
			data_int, _ := value.(int)
			data := strconv.Itoa(data_int)
			data_crc32 := DataSignerCrc32(data)

			m.Lock()
			data_md5 := DataSignerMd5(data)
			m.Unlock()

			data_md5_crc32 := DataSignerCrc32(data_md5)
			result := data_crc32 + "~" + data_md5_crc32
			p := Pair{val: result, index: idx}

			fmt.Printf("%v  SingleHash data %v\n", data, data)
			fmt.Printf("%v  SingleHash md5(data) %v\n", data, data_md5)
			fmt.Printf("%v  SingleHash crc32(md5(data)) %v\n", data, data_md5_crc32)
			fmt.Printf("%v  SingleHash crc32(data) %v\n", data, data_crc32)
			fmt.Printf("%v  SingleHash result %v\n", data, result)
			outCh <- p
		}(val, out, index)
		index++
	}

	wg.Wait()
	fmt.Println("Single Hash finished")
}

func MultiHash(in, out chan interface{}) {

}

func CombineResults(in, out chan interface{}) {

}

func ExecutePipeline(jobs ...job) {
	var wg sync.WaitGroup

	in := make(chan interface{})
	close(in)

	for i, work := range jobs {

		out := make(chan interface{})

		wg.Add(1)
		go func(fn job, inCh chan interface{}, outCh chan interface{}, num int) {
			defer wg.Done()
			defer close(outCh)
			fn(inCh, outCh)
			fmt.Printf("Job %d worked\n", num)
		}(work, in, out, i)

		in = out

	}

	wg.Wait()
	fmt.Println("Waiting is finished")
}

func main() {

}
