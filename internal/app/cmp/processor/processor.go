package processor

import (
	"context"
	"time"
)

const cmpName = "payment_processor"

type service interface {
	ProcessDuePayments(ctx context.Context) error
}

type cmp struct {
	cfg    Config
	svc    service
	cancel context.CancelFunc
	done   chan struct{}
}

func New(cfg Config, svc service) *cmp {
	return &cmp{
		cfg:  cfg,
		svc:  svc,
		done: make(chan struct{}),
	}
}

func (c *cmp) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go func() {
		defer close(c.done)

		ticker := time.NewTicker(c.cfg.PollInterval)
		defer ticker.Stop()

		for {
			_ = c.svc.ProcessDuePayments(runCtx)

			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return nil
}

func (c *cmp) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *cmp) GetName() string {
	return cmpName
}

func (c *cmp) GetShutdownDelay() time.Duration {
	return time.Second
}

func (c *cmp) GetStartTimeout() time.Duration {
	return 5 * time.Second
}

func (c *cmp) GetStopTimeout() time.Duration {
	return 5 * time.Second
}
