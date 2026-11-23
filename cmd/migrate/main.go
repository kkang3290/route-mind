package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kkang3290/route-mind/internal/migration"
)

func main() {
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	if e := migration.CLI(ctx, os.Args[1:], os.Stdout, os.Getenv); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
