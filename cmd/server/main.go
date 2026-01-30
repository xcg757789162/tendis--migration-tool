package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"tendis-migrate/internal/api"
	"tendis-migrate/internal/engine"
)

var (
	port      = flag.Int("port", 8080, "HTTP server port")
	dataDir   = flag.String("data", "./data", "Data directory")
	workers   = flag.Int("workers", 4, "Number of workers")
)

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting Tendis Migration Tool...")

	// 创建配置
	cfg := engine.DefaultConfig()
	cfg.Port = *port
	cfg.DataDir = *dataDir
	cfg.WorkerCount = *workers

	// 创建Master
	master, err := engine.NewMaster(cfg)
	if err != nil {
		log.Fatalf("Failed to create master: %v", err)
	}

	// 启动Master
	if err := master.Start(); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}

	// 创建API服务器
	server := api.NewServer(master, cfg.Port)

	// 启动HTTP服务
	go func() {
		log.Printf("HTTP server listening on :%d", cfg.Port)
		if err := server.Run(); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	master.Stop()
	log.Println("Goodbye!")
}
