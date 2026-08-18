package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"hermitmq/internal/broker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("starting HermitMQ Message Broker")

	cfg, err := broker.LoadConfig("hermitmq.json")
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	b := broker.NewBroker(cfg)
	b.StartBackgroundTasks()

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("failed to bind TCP listener", "address", addr, "error", err)
		os.Exit(1)
	}
	slog.Info("broker is listening", "address", addr)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				slog.Warn("listener closed or error accepting connection", "error", err)
				return
			}

			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				slog.Warn("failed to cast connection to TCPConn")
				conn.Close()
				continue
			}

			go broker.HandleConnection(b, tcpConn)
		}
	}()

	sig := <-sigChan
	slog.Info("received shutdown signal, shutting down...", "signal", sig.String())

	listener.Close()
	b.Close()

	slog.Info("graceful shutdown complete. All file descriptors closed safely.")
}
