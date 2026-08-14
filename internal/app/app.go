package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"Proxy2API/internal/config"
	"Proxy2API/internal/monitor"
	"Proxy2API/internal/project"
)

// Run builds the runtime components from config and blocks until shutdown.
func Run(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	workspace, err := config.LoadWorkspace(cfg.FilePath(), cfg)
	if err != nil {
		return err
	}
	return RunWorkspace(ctx, workspace)
}

// RunWorkspace starts the process-wide control plane and all configured project
// runtimes, then blocks until shutdown.
func RunWorkspace(ctx context.Context, workspace *config.Workspace) error {
	registry, err := project.NewRegistry(ctx, workspace)
	if err != nil {
		return fmt.Errorf("initialize project registry: %w", err)
	}
	defer registry.Close()

	monitorCfg := monitor.Config{
		Enabled:  workspace.ManagementEnabled(),
		Listen:   workspace.Management.Listen,
		Password: workspace.Management.Password,
	}
	server := monitor.NewServer(monitorCfg, nil, log.Default())
	if server != nil {
		server.SetProjectController(registry)
		server.Start(ctx)
		defer server.Shutdown(context.Background())
	}
	registry.StartAutostartProjects()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case <-ctx.Done():
		fmt.Println("Context cancelled, initiating graceful shutdown...")
	case sig := <-sigCh:
		fmt.Printf("Received %s, initiating graceful shutdown...\n", sig)
	}

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Graceful shutdown sequence
	fmt.Println("Stopping project runtimes...")
	if err := registry.Close(); err != nil {
		fmt.Printf("Error closing project runtimes: %v\n", err)
	}

	// Wait for connections to drain
	fmt.Println("Waiting for connections to drain...")
	select {
	case <-time.After(2 * time.Second):
		fmt.Println("Graceful shutdown completed")
	case <-shutdownCtx.Done():
		fmt.Println("Shutdown timeout exceeded, forcing exit")
	}

	return nil
}
