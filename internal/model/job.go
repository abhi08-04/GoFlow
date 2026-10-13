package model

import "time"

type Job struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	Payload    string    `json:"payload"`
	Status     string    `json:"status"`
	Attempts   int       `json:"attempts"`
	MaxRetries int       `json:"max_retries"`
	LastError  string    `json:"last_error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CreateJobRequest struct {
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	MaxRetries int    `json:"max_retries,omitempty"`
}

const (
	JobPending   = "pending"
	JobRunning   = "running"
	JobCompleted = "completed"
	JobFailed    = "failed"
)
