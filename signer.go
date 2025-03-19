package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

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

			fmt.Printf("%v  SingleHash data %v\n", data, data)
			fmt.Printf("%v  SingleHash md5(data) %v\n", data, data_md5)
			fmt.Printf("%v  SingleHash crc32(md5(data)) %v\n", data, data_md5_crc32)
			fmt.Printf("%v  SingleHash crc32(data) %v\n", data, data_crc32)
			fmt.Printf("%v  SingleHash result %v\n", data, result)
			outCh <- result
		}(val, out, index)
		index++
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup

	for val := range in {
		wg.Add(1)
		go func(inVal interface{}, out chan interface{}) {
			defer wg.Done()
			result := ""
			for i := 0; i < 6; i++ {
				num := strconv.Itoa(i)
				value, _ := inVal.(string)
				data := num + value
				data_crc32 := DataSignerCrc32(data)
				result += data_crc32
				fmt.Printf("%v MultiHash: crc32(th+step1) %d %s\n", inVal, i, data_crc32)
			}
			fmt.Printf("%v MultiHash result %s\n", inVal, result)
		}(val, out)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var wg sync.WaitGroup
	slice := []string{}

	for val := range in {
		wg.Add(1)
		go func(inVal interface{}, out chan interface{}) {
			defer wg.Done()
			value_str, _ := inVal.(string)
			slice = append(slice, value_str)

		}(val, out)
	}
	wg.Wait()

	sort.Strings(slice)
	result := strings.Join(slice, "_")
	out <- result
	fmt.Printf("CombineResults\n%s", result)
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
		}(work, in, out, i)

		in = out

	}

	wg.Wait()
}

func main() {

}
