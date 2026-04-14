package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/martketplace-vkr/pkg/outbox"
)

const (
	cmpName = "outbox"
)

type outboxCmp struct {
	cfg          Config
	OutboxClient outbox.Outbox
}

func New(cfg Config, outboxClient outbox.Outbox) *outboxCmp {
	return &outboxCmp{
		cfg:          cfg,
		OutboxClient: outboxClient,
	}
}

func (c *outboxCmp) GetName() string {
	return cmpName
}

func (c *outboxCmp) GetShutdownDelay() time.Duration {
	return time.Second
}

func (c *outboxCmp) GetStartTimeout() time.Duration {
	return 5 * time.Second
}

func (c *outboxCmp) GetStopTimeout() time.Duration {
	return 5 * time.Second
}

func (c *outboxCmp) Start(ctx context.Context) error {
	return c.OutboxClient.Run(ctx)
}

func (c *outboxCmp) Stop(ctx context.Context) error {
	return c.OutboxClient.Shutdown(ctx)
}

func (c *outboxCmp) Send(ctx context.Context, eventType string, payload any) (err error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = c.OutboxClient.CreateEvent(ctx, outbox.CreateEvent{
		Context:   ctx,
		EventType: eventType,
		Key:       uuid.NewString(),
		Payload:   body,
		Topics:    c.cfg.Topics,
	})
	if err != nil {
		return err
	}

	return nil
}
