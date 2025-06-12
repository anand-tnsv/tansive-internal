package session

type SessionStatus string

const (
	SessionStatusCreated    SessionStatus = "created"
	SessionStatusRunning    SessionStatus = "running"
	SessionStatusCompleted  SessionStatus = "completed"
	SessionStatusFailed     SessionStatus = "failed"
	SessionStatusExpired    SessionStatus = "expired"
	SessionStatusCancelled  SessionStatus = "cancelled"
	SessionStatusPaused     SessionStatus = "paused"
	SessionStatusResumed    SessionStatus = "resumed"
	SessionStatusSuspended  SessionStatus = "suspended"
	SessionStatusTerminated SessionStatus = "terminated"
)
