package domain

import "time"

type Status string

const (
	StatusPendingFunds Status = "pending_funds"
	StatusReserved     Status = "reserved"
	StatusCaptured     Status = "captured"
	StatusReleased     Status = "released"
	StatusExpired      Status = "expired"
	StatusCancelled    Status = "cancelled"
	StatusFailed       Status = "failed"
)

type Payment struct {
	ID                    int64      `db:"id"`
	OrderID               int64      `db:"order_id"`
	UserID                int64      `db:"user_id"`
	Amount                string     `db:"amount"`
	CurrencyCode          int64      `db:"currency_code"`
	Status                Status     `db:"status"`
	AttemptCount          int32      `db:"attempt_count"`
	PaymentDeadlineAt     time.Time  `db:"payment_deadline_at"`
	NextAttemptAt         time.Time  `db:"next_attempt_at"`
	ProcessingLockedUntil *time.Time `db:"processing_locked_until"`
	LastError             string     `db:"last_error"`
	HoldTransactionID     *int64     `db:"hold_transaction_id"`
	CaptureTransactionID  *int64     `db:"capture_transaction_id"`
	ReleaseTransactionID  *int64     `db:"release_transaction_id"`
	CreatedAt             time.Time  `db:"created_at"`
	UpdatedAt             time.Time  `db:"updated_at"`
}

type Event struct {
	OrderID      int64  `json:"order_id"`
	UserID       int64  `json:"user_id"`
	Status       Status `json:"status"`
	Amount       string `json:"amount,omitempty"`
	CurrencyCode int64  `json:"currency_code,omitempty"`
	Reason       string `json:"reason,omitempty"`
}
