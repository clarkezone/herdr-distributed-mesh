package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/app"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err := app.Run(ctx, os.Args[1:], app.IO{In: os.Stdin, Out: os.Stdout, Err: os.Stderr})
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "managed-run" {
		log.Printf("fatal: %v", err)
	} else {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
	os.Exit(1)
}
