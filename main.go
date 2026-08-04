package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"Proxy2API/internal/app"
	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"

	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Setup logging based on config
	setupLogging(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "proxy pool exited with error: %v\n", err)
		os.Exit(1)
	}
}

func setupLogging(cfg *config.Config) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Always include the in-memory ring buffer for dashboard console
	writers := []io.Writer{os.Stdout, monitor.LogWriter()}

	if cfg.Log.Output == "file" {
		// Ensure log directory exists
		logDir := filepath.Dir(cfg.Log.File)
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			log.Printf("\u26a0\ufe0f Failed to create log dir %s: %v, falling back to stdout", logDir, err)
		} else {
			lj := &lumberjack.Logger{
				Filename:   cfg.Log.File,
				MaxSize:    cfg.Log.MaxSize, // MB
				MaxBackups: cfg.Log.MaxBackups,
				MaxAge:     cfg.Log.MaxAge, // days
				Compress:   cfg.Log.Compress,
			}
			writers = append(writers, lj)
			log.Printf("\u2705 Log rotation enabled: file=%s, maxSize=%dMB, maxBackups=%d, maxAge=%dd",
				cfg.Log.File, cfg.Log.MaxSize, cfg.Log.MaxBackups, cfg.Log.MaxAge)
		}
	}

	log.SetOutput(io.MultiWriter(writers...))
}
