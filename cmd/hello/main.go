package main

import (
	"fmt"
	"io"
	"os"
)

func run(w io.Writer) {
	fmt.Fprintln(w, "Hello, world!")
}

func main() {
	run(os.Stdout)
}
