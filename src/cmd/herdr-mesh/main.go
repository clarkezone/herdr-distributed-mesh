package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/app"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err := app.Run(ctx, os.Args[1:], app.IO{Out: os.Stdout, Err: os.Stderr})
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	log.Printf("fatal: %v", err)
	os.Exit(1)
}
