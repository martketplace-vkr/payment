package eventmapper

import (
	"context"

	"github.com/martketplace-vkr/pkg/inbox/dto"
)

type service interface {
	HandleOrderCreate(context.Context, dto.Event) error
	HandleOrderCancel(context.Context, dto.Event) error
	HandleOrderPickedUp(context.Context, dto.Event) error
	HandleBalanceTopUp(context.Context, dto.Event) error
}

func GetEventMapper(svc service) map[string]func(context.Context, dto.Event) error {
	return map[string]func(context.Context, dto.Event) error{
		"order_create":            svc.HandleOrderCreate,
		"order_cancelled":         svc.HandleOrderCancel,
		"order_picked_up":         svc.HandleOrderPickedUp,
		"balance_topup_completed": svc.HandleBalanceTopUp,
	}
}
