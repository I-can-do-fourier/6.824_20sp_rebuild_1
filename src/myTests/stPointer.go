package main

import (
	"fmt"
)

type st struct {
	val int
}

func main() {

	for i := 0; i < 10; i++ {

		s := &st{}
		a := 4
		a++
		b := 4
		b++
		fmt.Printf("addr:%p\n", s)

	}

}
