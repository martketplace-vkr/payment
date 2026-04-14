package app

import (
	"context"

	balance "github.com/martketplace-vkr/balance/pkg/api/grpc/v1"
	"github.com/martketplace-vkr/payment/config"
	inboxComponent "github.com/martketplace-vkr/payment/internal/app/cmp/inbox"
	outboxComponent "github.com/martketplace-vkr/payment/internal/app/cmp/outbox"
	processorComponent "github.com/martketplace-vkr/payment/internal/app/cmp/processor"
	repository "github.com/martketplace-vkr/payment/internal/repository/pg"
	paymentservice "github.com/martketplace-vkr/payment/internal/service/payment"
	"github.com/martketplace-vkr/payment/pkg/eventmapper"
	"github.com/martketplace-vkr/pkg/build"
	"github.com/martketplace-vkr/pkg/build/components/pgxsqlxcomponent"
	"github.com/martketplace-vkr/pkg/kafkaconnector"
	outboxclient "github.com/martketplace-vkr/pkg/outbox"
)

func Run(ctx context.Context, cfg *config.Config) error {
	pg := pgxsqlxcomponent.New(cfg.Postgres)

	kafkaClient := kafkaconnector.NewClient(cfg.Kafka)
	kafkaProducer := kafkaClient.NewSyncProducer()

	outboxCl, err := outboxclient.NewDefaultWithOptions(
		cfg.Outbox.Outbox,
		outboxclient.WithSqlxDB(pg.DB),
		outboxclient.WithKafkaProducer(kafkaProducer),
	)
	if err != nil {
		return err
	}

	outboxCmp := outboxComponent.New(cfg.Outbox, outboxCl)
	balanceClient := balance.New(cfg.Balance)
	repo := repository.New(pg.DB)
	service := paymentservice.New(repo, balanceClient, outboxCmp, cfg.Processor)
	processorCmp := processorComponent.New(cfg.Processor, service)

	inboxCmp := inboxComponent.New(
		cfg.Inbox,
		pg,
		kafkaClient,
		eventmapper.GetEventMapper(service),
	)

	cmps := build.Components{
		pg,
		outboxCmp,
		balanceClient,
		inboxCmp,
		processorCmp,
	}

	app, err := build.NewApp(cmps)
	if err != nil {
		return err
	}

	return build.Run(ctx, app)
}
