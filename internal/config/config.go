package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port            string
	GrpcPort        string
	NumWorkers      int
	JobQueueSize    int
	JobTimeout      time.Duration
	MaxRetries      int
	BaseBackoff     time.Duration
	ShutdownTimeout time.Duration
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

	numWorkers, err := getInt("NUM_WORKERS")
	if err != nil {
		return Config{}, err
	}

	queueSize, err := getInt("JOB_QUEUE_SIZE")
	if err != nil {
		return Config{}, err
	}

	jobTimeout, err := getSeconds("JOB_TIMEOUT_SECONDS")
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

	cfg := Config{
		Port:            port,
		NumWorkers:      numWorkers,
		JobQueueSize:    queueSize,
		JobTimeout:      jobTimeout,
		MaxRetries:      maxRetries,
		BaseBackoff:     baseBackoff,
		ShutdownTimeout: shutdownTimeout,
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

	if c.NumWorkers <= 0 {
		return fmt.Errorf("NUM_WORKERS must be greater than 0")
	}

	if c.JobQueueSize <= 0 {
		return fmt.Errorf("JOB_QUEUE_SIZE must be greater than 0")
	}

	if c.JobTimeout <= 0 {
		return fmt.Errorf("JOB_TIMEOUT_SECONDS must be greater than 0")
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

	return nil
}

func getString(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("missing required env var: %s", key)
	}
	return v, nil
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

func getSeconds(key string) (time.Duration, error) {
	seconds, err := getInt(key)
	if err != nil {
		return 0, err
	}
	return time.Duration(seconds) * time.Second, nil
}
