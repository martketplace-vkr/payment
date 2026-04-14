package config

import (
	balance "github.com/martketplace-vkr/balance/pkg/api/grpc/v1"
	"github.com/martketplace-vkr/payment/internal/app/cmp/inbox"
	"github.com/martketplace-vkr/payment/internal/app/cmp/outbox"
	"github.com/martketplace-vkr/payment/internal/app/cmp/processor"
	"github.com/martketplace-vkr/pkg/build/components/pgxsqlxcomponent"
	"github.com/martketplace-vkr/pkg/kafkaconnector"
)

type Config struct {
	Postgres  pgxsqlxcomponent.Config     `validate:"required"`
	Inbox     inbox.Config                `validate:"required"`
	Outbox    outbox.Config               `validate:"required"`
	Balance   balance.Config              `validate:"required"`
	Processor processor.Config            `validate:"required"`
	Kafka     kafkaconnector.ClientConfig `validate:"required"`
}
