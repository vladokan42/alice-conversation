package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	device "alice-conversation/device"
)

func main() {
	path := flag.String("config", "", "JSON configuration path (no inline credentials)")
	flag.Parse()
	if *path == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	cfg, err := device.LoadConfig(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ambient: configuration rejected:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = device.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "ambient: stopped:", err)
		os.Exit(1)
	}
}
