package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	var m sync.Mutex
	var wg sync.WaitGroup

	for val := range in {
		wg.Add(1)
		go func(value interface{}, outCh chan interface{}) {
			defer wg.Done()
			ch1 := make(chan string)
			ch2 := make(chan string)
			result := ""
			data_int, _ := value.(int)
			data := strconv.Itoa(data_int)
			m.Lock()
			data_md5 := DataSignerMd5(data)
			m.Unlock()

			go func() {
				data_crc32 := DataSignerCrc32(data)
				ch1 <- data_crc32
			}()

			go func() {
				data_md5_crc32 := DataSignerCrc32(data_md5)
				ch2 <- data_md5_crc32
			}()

			result = <-ch1 + "~" + <-ch2

			// fmt.Printf("%v  SingleHash data %v\n", data, data)
			// fmt.Printf("%v  SingleHash md5(data) %v\n", data, data_md5)
			// fmt.Printf("%v  SingleHash crc32(md5(data)) %v\n", data, data_md5_crc32)
			// fmt.Printf("%v  SingleHash crc32(data) %v\n", data, data_crc32)
			// fmt.Printf("%v  SingleHash result %v\n", data, result)
			outCh <- result

		}(val, out)
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup

	for val := range in {
		wg.Add(1)
		go func(inVal interface{}, out chan interface{}) {
			defer wg.Done()
			slice := make([]string, 6)
			result := ""
			var wg2 sync.WaitGroup
			for i := 0; i < 6; i++ {
				wg2.Add(1)
				go func(index int) {
					defer wg2.Done()
					num := strconv.Itoa(index)
					value, _ := inVal.(string)
					data := num + value
					data_crc32 := DataSignerCrc32(data)
					slice[index] = data_crc32
				}(i)
			}
			wg2.Wait()
			for i := 0; i < 6; i++ {
				result += slice[i]
				// fmt.Printf("%v MultiHash: crc32(th+step1) %d %v\n", inVal, i, slice[i])
			}
			// fmt.Printf("%v MultiHash result %s\n", inVal, result)
			out <- result
		}(val, out)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	slice := []string{}

	for val := range in {
		value_str, _ := val.(string)
		slice = append(slice, value_str)
	}

	sort.Strings(slice)
	result := strings.Join(slice, "_")
	out <- result
	// fmt.Printf("CombineResults\n%s\n", result)
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
