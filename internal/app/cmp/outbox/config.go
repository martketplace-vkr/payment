package outbox

import "github.com/martketplace-vkr/pkg/outbox"

type Config struct {
	Topics []string      `validate:"required"`
	Outbox outbox.Config `validate:"required"`
}
