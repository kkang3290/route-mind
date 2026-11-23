package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/kkang3290/route-mind/internal/app"
	"github.com/kkang3290/route-mind/internal/config"
)

var version = "dev"

func main() {
	path := flag.String("conf", "configs/config.yaml", "operator-owned YAML configuration")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, e := config.Load(*path, os.Getenv)
	if e != nil {
		logger.Error("configuration_failed", "error", e.Error())
		os.Exit(1)
	}
	runtime, e := app.Open(context.Background(), cfg, logger)
	if e != nil {
		logger.Error("startup_failed", "error", e.Error())
		os.Exit(1)
	}
	application := kratos.New(kratos.Name("route-mind"), kratos.Version(version), kratos.Server(runtime.HTTP), kratos.StopTimeout(10*time.Second))
	e = application.Run()
	runtime.Close()
	if e != nil {
		logger.Error("service_stopped", "error", "server shutdown failed")
		os.Exit(1)
	}
}
