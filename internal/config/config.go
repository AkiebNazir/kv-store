package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Server Config
	Port    string
	Peers   string
	DataDir string

	NetworkTimeout   time.Duration
	BatchConcurrency int

	// Cluster Config
	VirtualNodes      int
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int

	// Storage Engine Config
	MemTableSize        int64
	MaxLevel0Files      int
	SparseIndexInterval int

	// WAL Config
	WALCommitInterval time.Duration
	WALMaxBatchSize   int

	// Bloom Filters
	BloomFilterSizeL0 int
	BloomFilterSizeL1 int
}

// LoadConfig reads .env (if present) and overrides with OS env vars, falling back to defaults
func LoadConfig() *Config {
	// Try multiple config file names
	// Priority: .env -> test.env -> defaults only
	if err := loadEnvFile(".env"); err != nil {
		// .env failed, try test.env
		loadEnvFile("test.env") // Ignore error - will use defaults
	}

	return &Config{
		// Server Defaults
		Port:    getEnv("NET_PORT", "8080"),
		Peers:   getEnv("KVSTORE_PEERS", ""),
		DataDir: getEnv("KVSTORE_DIR", "./data"),

		// Cluster Defaults
		VirtualNodes:      getEnvAsInt("CLUSTER_VIRTUAL_NODES", 10),
		ReplicationFactor: getEnvAsInt("CLUSTER_REPLICATION_FACTOR", 3),
		WriteQuorum:       getEnvAsInt("CLUSTER_WRITE_QUORUM", 1),
		ReadQuorum:        getEnvAsInt("CLUSTER_READ_QUORUM", 1),

		// Storage Defaults
		MemTableSize:        int64(getEnvAsInt("MEMTABLE_SIZE", 64*1024*1024)), // 64MB
		MaxLevel0Files:      getEnvAsInt("MAX_L0_FILES", 4),
		SparseIndexInterval: getEnvAsInt("SPARSE_INDEX_INTERVAL", 128),

		// WAL Defaults
		WALCommitInterval: getEnvAsDuration("WAL_COMMIT_INTERVAL", 2*time.Millisecond),
		WALMaxBatchSize:   getEnvAsInt("WAL_MAX_BATCH_SIZE", 512),

		// Network Defaults
		NetworkTimeout:   getEnvAsDuration("NETWORK_TIMEOUT", 2*time.Second),
		BatchConcurrency: getEnvAsInt("BATCH_CONCURRENCY", 50),

		// Bloom Defaults
		BloomFilterSizeL0: getEnvAsInt("BLOOM_SIZE_L0", 20000),
		BloomFilterSizeL1: getEnvAsInt("BLOOM_SIZE_L1", 100000),
	}
}

// --- Standard Lib .env Parser ---

func loadEnvFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split KEY=VALUE
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove quotes if present
		value = strings.Trim(value, `"'`)

		// Only set if not already set in OS environment
		// (OS env vars > .env file)
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	return nil
}

// --- Helpers ---

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	strValue := getEnv(key, "")
	if value, err := strconv.Atoi(strValue); err == nil {
		return value
	}
	return fallback
}

func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	strValue := getEnv(key, "")
	if value, err := time.ParseDuration(strValue); err == nil {
		return value
	}
	return fallback
}
