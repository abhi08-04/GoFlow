package config

import (
	"testing"
	"time"
)

func TestGetString(t *testing.T) {
	t.Setenv("TEST_PORT", "8080")

	got, err := getString("TEST_PORT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "8080" {
		t.Fatalf("expected %q, got %q", "8080", got)
	}
}

func TestGetInt(t *testing.T) {
	t.Setenv("TEST_NUM_WORKERS", "5")

	got, err := getInt("TEST_NUM_WORKERS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestGetSeconds(t *testing.T) {
	t.Setenv("TEST_TIMEOUT", "10")

	got, err := getSeconds("TEST_TIMEOUT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := 10 * time.Second

	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestGetSecondsZero(t *testing.T) {
	t.Setenv("TEST_TIMEOUT", "0")

	got, err := getSeconds("TEST_TIMEOUT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestGetStringMissing(t *testing.T) {
	t.Setenv("TEST_MISSING_STRING", "")

	_, err := getString("TEST_MISSING_STRING")
	if err == nil {
		t.Fatal("expected error for missing environment variable")
	}
}

func TestGetIntMissing(t *testing.T) {
	t.Setenv("TEST_MISSING_INT", "")

	_, err := getInt("TEST_MISSING_INT")
	if err == nil {
		t.Fatal("expected error for missing environment variable")
	}
}

func TestGetIntInvalid(t *testing.T) {
	t.Setenv("TEST_INVALID_INT", "abc")

	_, err := getInt("TEST_INVALID_INT")
	if err == nil {
		t.Fatal("expected error for invalid integer")
	}
}

func TestGetSecondsInvalid(t *testing.T) {
	t.Setenv("TEST_INVALID_SECONDS", "abc")

	_, err := getSeconds("TEST_INVALID_SECONDS")
	if err == nil {
		t.Fatal("expected error for invalid seconds")
	}
}

func setValidConfigEnv(t *testing.T) {
	t.Helper()

	t.Setenv("PORT", "8080")
	t.Setenv("NUM_WORKERS", "3")
	t.Setenv("JOB_QUEUE_SIZE", "10")
	t.Setenv("JOB_TIMEOUT_SECONDS", "5")
	t.Setenv("MAX_RETRIES", "3")
	t.Setenv("BASE_BACKOFF_SECONDS", "2")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "10")
}

func TestLoadFromEnv(t *testing.T) {
	setValidConfigEnv(t)

	got, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := Config{
		Port:            "8080",
		GrpcPort:        "9090",
		NumWorkers:      3,
		JobQueueSize:    10,
		JobTimeout:      5 * time.Second,
		MaxRetries:      3,
		BaseBackoff:     2 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	}

	if got != want {
		t.Fatalf("expected config %+v, got %+v", want, got)
	}
}

func TestLoadFromEnvMissingPort(t *testing.T) {
	setValidConfigEnv(t)
	t.Setenv("PORT", "")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("expected error for missing PORT")
	}
}

func TestLoadFromEnvInvalidWorkers(t *testing.T) {
	setValidConfigEnv(t)
	t.Setenv("NUM_WORKERS", "three")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("expected error for invalid NUM_WORKERS")
	}
}

func TestConfigValidate(t *testing.T) {
	valid := Config{
		Port:            "8080",
		GrpcPort:        "9090",
		NumWorkers:      3,
		JobQueueSize:    10,
		JobTimeout:      5 * time.Second,
		MaxRetries:      3,
		BaseBackoff:     2 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	}

	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:   "valid config",
			modify: func(c *Config) {},
		},
		{
			name:    "empty port",
			modify:  func(c *Config) { c.Port = "" },
			wantErr: true,
		},
		{
			name:    "non-numeric port",
			modify:  func(c *Config) { c.Port = "abc" },
			wantErr: true,
		},
		{
			name:    "port out of range",
			modify:  func(c *Config) { c.Port = "70000" },
			wantErr: true,
		},
		{
			name:    "zero workers",
			modify:  func(c *Config) { c.NumWorkers = 0 },
			wantErr: true,
		},
		{
			name:    "negative queue size",
			modify:  func(c *Config) { c.JobQueueSize = -1 },
			wantErr: true,
		},
		{
			name:    "zero job timeout",
			modify:  func(c *Config) { c.JobTimeout = 0 },
			wantErr: true,
		},
		{
			name:    "negative retries",
			modify:  func(c *Config) { c.MaxRetries = -1 },
			wantErr: true,
		},
		{
			name:    "negative backoff",
			modify:  func(c *Config) { c.BaseBackoff = -time.Second },
			wantErr: true,
		},
		{
			name:    "zero shutdown timeout",
			modify:  func(c *Config) { c.ShutdownTimeout = 0 },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.modify(&cfg)

			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
