package inbox

import (
	"context"
	"time"

	"github.com/martketplace-vkr/pkg/build/components/pgxsqlxcomponent"
	"github.com/martketplace-vkr/pkg/inbox"
	"github.com/martketplace-vkr/pkg/inbox/dto"
	"github.com/martketplace-vkr/pkg/kafkaconnector"
)

const (
	cmpName      = "inbox"
	startTimeout = 5 * time.Second
	stopTimeout  = 5 * time.Second
)

type cmp struct {
	cfg         Config
	pg          *pgxsqlxcomponent.PgxSqlxConnector
	kafkaClient kafkaconnector.Client
	inboxClient inbox.Inbox

	handlerMap map[string]func(context.Context, dto.Event) error
}

func New(
	cfg Config,
	pg *pgxsqlxcomponent.PgxSqlxConnector,
	kafkaClient kafkaconnector.Client,
	handlerMap map[string]func(context.Context, dto.Event) error,
) *cmp {
	return &cmp{
		pg:          pg,
		kafkaClient: kafkaClient,
		cfg:         cfg,
		handlerMap:  handlerMap,
	}
}

func (c *cmp) Start(ctx context.Context) (err error) {
	c.inboxClient, err = inbox.NewDefaultWithOptions(
		c.cfg.Inbox,
		inbox.WithKafkaEventsProvider(
			c.kafkaClient.NewSaramaConsumerGroup,
		),
		inbox.WithSqlxDB(c.pg.DB),
		inbox.WithHandlerMap(c.handlerMap),
	)
	if err != nil {
		return err
	}

	err = c.inboxClient.Run(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *cmp) Stop(ctx context.Context) error {
	return c.inboxClient.Shutdown(ctx)
}

func (c *cmp) GetName() string {
	return cmpName
}

func (c *cmp) GetShutdownDelay() time.Duration {
	return time.Second
}

func (c *cmp) GetStartTimeout() time.Duration {
	return startTimeout
}

func (c *cmp) GetStopTimeout() time.Duration {
	return stopTimeout
}
