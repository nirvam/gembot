package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nirvam/gembot/internal/acp"
	"github.com/nirvam/gembot/internal/config"
	"github.com/nirvam/gembot/internal/core"
	"github.com/nirvam/gembot/internal/im"
	"github.com/nirvam/gembot/internal/store"
)

func main() {
	log.Println("Starting Gembot...")

	// 1. Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Init SQLite Store
	s, err := store.NewStore(cfg.DBPath)
	if err != nil {
		log.Fatalf("Failed to initialize store: %v", err)
	}
	defer s.Close()

	// 3. Init ACP Bridge
	log.Printf("Starting Agent subprocess: %s %v", cfg.AgentCommand, cfg.AgentArgs)
	bridge, err := acp.NewBridge(cfg.AgentCommand, cfg.AgentArgs...)
	if err != nil {
		log.Fatalf("Failed to initialize ACP bridge: %v", err)
	}
	defer bridge.Close()

	// 4. Init Core Manager
	manager := core.NewManager(cfg, s, bridge)
	defer manager.Stop()

	// 5. Init Feishu Adapter
	adapter := im.NewFeishuAdapter(
		cfg.AppID,
		cfg.AppSecret,
		cfg.VerificationToken,
		cfg.EncryptKey,
		manager,
		s,
	)
	manager.SetAdapter(adapter)

	// 6. Start Adapter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := adapter.Start(ctx); err != nil {
		log.Fatalf("Failed to start Feishu adapter: %v", err)
	}

	log.Println("Gembot started successfully. Press Ctrl+C to stop.")

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Gembot...")
}
