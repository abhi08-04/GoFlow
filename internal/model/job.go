package model

type Job struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Payload  string `json:"payload"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}

type CreateJobRequest struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

const (
	JobPending   = "pending"
	JobRunning   = "running"
	JobCompleted = "completed"
	JobFailed    = "failed"
)
