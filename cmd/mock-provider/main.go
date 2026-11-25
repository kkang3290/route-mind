package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kkang3290/route-mind/internal/demo"
)

func main() {
	address := flag.String("address", "127.0.0.1:18081", "literal loopback listen address (development only)")
	delay := flag.Duration("delay", 0, "bounded response delay, up to 60s (verification only)")
	failFirst := flag.Int("fail-first", 0, "fail the first 0..1000 authenticated calls (verification only)")
	flag.Parse()
	host, _, e := net.SplitHostPort(*address)
	ip := net.ParseIP(host)
	if e != nil || ip == nil || !ip.IsLoopback() {
		slog.Error("mock provider must bind a literal loopback address")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	handler, e := demo.NewScenario(*delay, *failFirst)
	if e != nil {
		slog.Error("invalid mock verification settings")
		os.Exit(1)
	}
	s := &http.Server{Addr: *address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 70 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(c)
	}()
	slog.Info("LOCAL MOCK ONLY; no AI or external provider calls", "address", *address)
	if e = s.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		slog.Error("mock listener failed")
		os.Exit(1)
	}
}
