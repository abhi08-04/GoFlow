package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds every tunable value for the app, loaded once at startup
type Config struct {
	Port            string
	NumWorkers      int
	JobQueueSize    int
	JobTimeout      time.Duration
	MaxRetries      int
	BaseBackoff     time.Duration
	ShutdownTimeout time.Duration
}

// Load reads the .env file (if present) and parses every expected variable.

func Load() Config {

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	return Config{
		Port:            getString("PORT"),
		NumWorkers:      getInt("NUM_WORKERS"),
		JobQueueSize:    getInt("JOB_QUEUE_SIZE"),
		JobTimeout:      getSeconds("JOB_TIMEOUT_SECONDS"),
		MaxRetries:      getInt("MAX_RETRIES"),
		BaseBackoff:     getSeconds("BASE_BACKOFF_SECONDS"),
		ShutdownTimeout: getSeconds("SHUTDOWN_TIMEOUT_SECONDS"),
	}
}

// getString returns a required string env var, failing fast if it is empty.
func getString(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var: %s", key)
	}
	return v
}

// getInt parses a required integer env var.
func getInt(key string) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		log.Fatalf("invalid int for env var %s: %v", key, err)
	}
	return v
}

// getSeconds parses an integer env var and interprets it as a number of seconds.
func getSeconds(key string) time.Duration {
	return time.Duration(getInt(key)) * time.Second
}
