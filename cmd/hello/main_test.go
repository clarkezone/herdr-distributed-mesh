package main

import (
	"bytes"
	"testing"
)

func TestRun(t *testing.T) {
	var output bytes.Buffer

	run(&output)

	const expected = "Hello, world!\n"
	if output.String() != expected {
		t.Fatalf("run() output = %q, want %q", output.String(), expected)
	}
}
