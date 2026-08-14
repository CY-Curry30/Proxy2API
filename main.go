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
	workspace, err := config.LoadWorkspace(configPath, cfg)
	if err != nil {
		log.Fatalf("load project workspace: %v", err)
	}

	// Setup logging based on config
	setupLogging(workspace.Log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.RunWorkspace(ctx, workspace); err != nil {
		fmt.Fprintf(os.Stderr, "proxy pool exited with error: %v\n", err)
		os.Exit(1)
	}
}

func setupLogging(logCfg config.LogConfig) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Always include the in-memory ring buffer for dashboard console
	writers := []io.Writer{os.Stdout, monitor.LogWriter()}

	if logCfg.Output == "file" {
		// Ensure log directory exists
		logDir := filepath.Dir(logCfg.File)
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			log.Printf("\u26a0\ufe0f Failed to create log dir %s: %v, falling back to stdout", logDir, err)
		} else {
			lj := &lumberjack.Logger{
				Filename:   logCfg.File,
				MaxSize:    logCfg.MaxSize, // MB
				MaxBackups: logCfg.MaxBackups,
				MaxAge:     logCfg.MaxAge, // days
				Compress:   logCfg.Compress,
			}
			writers = append(writers, lj)
			log.Printf("\u2705 Log rotation enabled: file=%s, maxSize=%dMB, maxBackups=%d, maxAge=%dd",
				logCfg.File, logCfg.MaxSize, logCfg.MaxBackups, logCfg.MaxAge)
		}
	}

	log.SetOutput(io.MultiWriter(writers...))
}
