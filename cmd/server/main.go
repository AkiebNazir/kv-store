package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"kvstore/internal/api"
	"kvstore/internal/cluster"
	"kvstore/internal/config"
	"kvstore/internal/metrics"
	"kvstore/internal/storage"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Load Config
	cfg := config.LoadConfig()

	// Override using flags
	port := flag.String("port", cfg.Port, "Server Port")
	peers := flag.String("peers", cfg.Peers, "Comma separated peer addresses")
	dataDir := flag.String("dir", cfg.DataDir, "Data Directory")
	flag.Parse()

	// Update config
	cfg.Port = *port
	cfg.Peers = *peers
	cfg.DataDir = *dataDir

	// Parse peer list
	var peerList []string
	me := "localhost:" + cfg.Port

	if cfg.Peers != "" {
		for _, p := range strings.Split(cfg.Peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peerList = append(peerList, p)
			}
		}
	}

	clusterSize := len(peerList)

	// Validate minimum cluster size
	if clusterSize < 3 {
		log.Fatalf("ERROR: Minimum cluster size is 3 nodes. Got %d nodes. Please provide at least 3 peer addresses.", clusterSize)
	}

	// Fixed configuration for 3+ node clusters
	// R=2, W=2, N=3 ensures strong consistency with fault tolerance
	cfg.ReplicationFactor = min(3, clusterSize)
	cfg.ReadQuorum = 2
	cfg.WriteQuorum = 2

	fmt.Printf("===============================================\n")
	fmt.Printf(" kv-store Starting...\n")
	fmt.Printf("===============================================\n")
	fmt.Printf("Port:             %s\n", cfg.Port)
	fmt.Printf("Cluster Size:     %d nodes\n", clusterSize)
	fmt.Printf("Data Directory:   %s\n", cfg.DataDir)
	fmt.Printf("MemTable Size:    %d MB\n", cfg.MemTableSize/(1024*1024))
	fmt.Printf("Virtual Nodes:    %d\n", cfg.VirtualNodes)
	fmt.Printf("Replication (N):  %d\n", cfg.ReplicationFactor)
	fmt.Printf("Write Quorum (W): %d\n", cfg.WriteQuorum)
	fmt.Printf("Read Quorum (R):  %d\n", cfg.ReadQuorum)
	fmt.Printf("Consistency:      Strong (R+W>N)\n")
	fmt.Printf("Fault Tolerance:  1 node failure\n")
	fmt.Printf("===============================================\n")

	// Initialize Storage Engine
	nodeDataDir := fmt.Sprintf("%s_%s", cfg.DataDir, cfg.Port)
	engine := storage.NewEngine(cfg, nodeDataDir)

	// Initialize Cluster Ring
	ring := cluster.NewRing(cfg.VirtualNodes)

	// Add all peers to ring
	for _, p := range peerList {
		ring.AddNode(p)
		if p != me {
			fmt.Printf("✓ Added peer: %s\n", p)
		}
	}

	// Initialize Server with persistent hint store
	server := &api.Server{
		Engine: engine,
		Ring:   ring,
		Self:   me,
		Config: cfg,
		HttpClient: &http.Client{
			Timeout: cfg.NetworkTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
	}

	// Initialize hint store
	server.HintStore = api.NewHintStore(nodeDataDir)

	// Start hinted handoff replayer
	server.StartHintReplayer()
	fmt.Printf("✓ Hinted Handoff: ACTIVE\n")

	// Periodic Metrics Logger
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			metrics.GlobalMetrics.PrintStats()
		}
	}()

	// REGISTER ROUTES

	// Public Routes
	http.HandleFunc("/put", server.HandlePut)
	http.HandleFunc("/get", server.HandleGet)
	http.HandleFunc("/batch", server.HandleBatch)
	http.HandleFunc("/scan", server.HandleScan)
	http.HandleFunc("/delete", server.HandleDelete)

	// Internal Routes
	http.HandleFunc("/internal/put", server.HandleInternalPut)
	http.HandleFunc("/internal/put_raw", server.HandleInternalPutRaw)
	http.HandleFunc("/internal/get", server.HandleInternalGet)
	http.HandleFunc("/internal/batch", server.HandleInternalBatch)
	http.HandleFunc("/internal/scan", server.HandleInternalScan)

	// Health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Metrics endpoint
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"writes":      metrics.GlobalMetrics.TotalWrites,
			"reads":       metrics.GlobalMetrics.TotalReads,
			"batches":     metrics.GlobalMetrics.TotalBatches,
			"scans":       metrics.GlobalMetrics.TotalScans,
			"deletes":     metrics.GlobalMetrics.TotalDeletes,
			"errors":      metrics.GlobalMetrics.TotalErrors,
			"compactions": metrics.GlobalMetrics.Compactions,
			"flushes":     metrics.GlobalMetrics.MemTableFlushes,
			"wal_syncs":   metrics.GlobalMetrics.WALSyncs,
		})
	})

	fmt.Printf("===============================================\n")
	fmt.Printf("🚀 kv-store Node running on port %s\n", cfg.Port)
	fmt.Printf("===============================================\n\n")

	// Setup graceful shutdown
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      nil,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Channel to listen for interrupt signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt
	<-stop
	fmt.Printf("\n🛑 Shutting down gracefully...\n")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// Shutdown hint store
	server.HintStore.Shutdown()

	fmt.Printf("Server stopped\n")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
