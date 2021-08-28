package main

import (
	"fmt"
	"time"
)

func main() {

	c := make(chan int, 10)

	go func(ch chan int) {

		for i := 0; i < 5; i++ {

			ch <- i
			fmt.Println("len:", ch)

		}

		close(c)
	}(c)

	time.Sleep(time.Duration(1) * time.Second)

	for i := range c {

		fmt.Println(i)

	}

}
