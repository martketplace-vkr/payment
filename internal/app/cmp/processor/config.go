package processor

import "time"

type Config struct {
	PollInterval           time.Duration `validate:"required"`
	BatchSize              int           `validate:"required"`
	ProcessingLockDuration time.Duration `validate:"required"`
	PaymentTimeout         time.Duration `validate:"required"`
	RetryDelay             time.Duration `validate:"required"`
}
