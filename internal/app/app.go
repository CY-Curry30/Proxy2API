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
		return fmt.Errorf("配置不能为空")
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
		return fmt.Errorf("初始化项目注册表失败: %w", err)
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
		fmt.Println("上下文已取消，开始平滑关闭...")
	case sig := <-sigCh:
		fmt.Printf("收到信号 %s，开始平滑关闭...\n", sig)
	}

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Graceful shutdown sequence
	fmt.Println("正在停止项目运行时...")
	if err := registry.Close(); err != nil {
		fmt.Printf("关闭项目运行时失败: %v\n", err)
	}

	// Wait for connections to drain
	fmt.Println("正在等待现有连接排空...")
	select {
	case <-time.After(2 * time.Second):
		fmt.Println("平滑关闭已完成")
	case <-shutdownCtx.Done():
		fmt.Println("关闭操作超时，正在强制退出")
	}

	return nil
}
