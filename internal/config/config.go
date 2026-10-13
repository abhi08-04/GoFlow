package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                  string
	GrpcPort              string
	JobQueueSize          int
	MaxRetries            int
	BaseBackoff           time.Duration
	ShutdownTimeout       time.Duration
	DefaultLease          time.Duration
	MaxLease              time.Duration
	LeaseSweepInterval    time.Duration
	MaxPrefetch           int
}

func Load() (Config, error) {
	_ = godotenv.Load()
	return LoadFromEnv()
}

func LoadFromEnv() (Config, error) {
	port, err := getString("PORT")
	if err != nil {
		return Config{}, err
	}

	grpcPort := getStringWithDefault("GRPC_PORT", "9090")

	queueSize, err := getInt("JOB_QUEUE_SIZE")
	if err != nil {
		return Config{}, err
	}

	maxRetries, err := getInt("MAX_RETRIES")
	if err != nil {
		return Config{}, err
	}

	baseBackoff, err := getSeconds("BASE_BACKOFF_SECONDS")
	if err != nil {
		return Config{}, err
	}

	shutdownTimeout, err := getSeconds("SHUTDOWN_TIMEOUT_SECONDS")
	if err != nil {
		return Config{}, err
	}

	defaultLease := time.Duration(getIntWithDefault("DEFAULT_LEASE_SECONDS", 60)) * time.Second
	maxLease := time.Duration(getIntWithDefault("MAX_LEASE_SECONDS", 300)) * time.Second
	sweepInterval := time.Duration(getIntWithDefault("LEASE_SWEEP_INTERVAL_SECONDS", 1)) * time.Second
	maxPrefetch := getIntWithDefault("MAX_PREFETCH", 64)

	cfg := Config{
		Port:               port,
		GrpcPort:           grpcPort,
		JobQueueSize:       queueSize,
		MaxRetries:         maxRetries,
		BaseBackoff:        baseBackoff,
		ShutdownTimeout:    shutdownTimeout,
		DefaultLease:       defaultLease,
		MaxLease:           maxLease,
		LeaseSweepInterval: sweepInterval,
		MaxPrefetch:        maxPrefetch,
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if c.Port == "" {
		return fmt.Errorf("PORT is required and must be non-empty")
	}

	port, err := strconv.Atoi(c.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("PORT must be a number between 1 and 65535")
	}

	if c.GrpcPort == "" {
		return fmt.Errorf("GRPC_PORT is required and must be non-empty")
	}
	grpcPort, err := strconv.Atoi(c.GrpcPort)
	if err != nil || grpcPort < 1 || grpcPort > 65535 {
		return fmt.Errorf("GRPC_PORT must be a number between 1 and 65535")
	}
	if grpcPort == port {
		return fmt.Errorf("GRPC_PORT must differ from PORT")
	}

	if c.JobQueueSize <= 0 {
		return fmt.Errorf("JOB_QUEUE_SIZE must be greater than 0")
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("MAX_RETRIES must be greater than or equal to 0")
	}

	if c.BaseBackoff < 0 {
		return fmt.Errorf("BASE_BACKOFF_SECONDS must be greater than or equal to 0")
	}

	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT_SECONDS must be greater than 0")
	}

	if c.DefaultLease <= 0 || c.MaxLease <= 0 || c.LeaseSweepInterval <= 0 {
		return fmt.Errorf("lease duration/sweep values must be greater than 0")
	}
	if c.MaxLease < c.DefaultLease {
		return fmt.Errorf("MAX_LEASE_SECONDS must be >= DEFAULT_LEASE_SECONDS")
	}
	if c.MaxPrefetch <= 0 {
		return fmt.Errorf("MAX_PREFETCH must be greater than 0")
	}

	return nil
}

func getString(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("missing required env var: %s", key)
	}
	return v, nil
}

func getStringWithDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, fmt.Errorf("missing required env var: %s", key)
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid int for env var %s: %w", key, err)
	}
	return v, nil
}

func getIntWithDefault(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func getSeconds(key string) (time.Duration, error) {
	seconds, err := getInt(key)
	if err != nil {
		return 0, err
	}
	return time.Duration(seconds) * time.Second, nil
}
