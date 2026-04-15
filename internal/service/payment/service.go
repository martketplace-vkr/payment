package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	balancev1 "github.com/martketplace-vkr/balance/pkg/api/grpc/v1"
	adminPb "github.com/martketplace-vkr/balance/pkg/api/grpc/v1/admin"
	domainPb "github.com/martketplace-vkr/balance/pkg/api/grpc/v1/domain"
	orderPb "github.com/martketplace-vkr/balance/pkg/api/grpc/v1/order"
	ordomain "github.com/martketplace-vkr/order/domain"
	processorconfig "github.com/martketplace-vkr/payment/internal/app/cmp/processor"
	paydomain "github.com/martketplace-vkr/payment/internal/domain"
	"github.com/martketplace-vkr/payment/internal/repository/pg"
	"github.com/martketplace-vkr/pkg/inbox/dto"
	"github.com/martketplace-vkr/pkg/utils/currency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	eventOrderPaid       = "order_paid"
	eventPaymentExpired  = "payment_expired"
	eventPaymentReleased = "payment_released"
	eventPaymentCaptured = "payment_captured"
)

type outbox interface {
	Send(ctx context.Context, eventType string, payload any) error
}

type Service struct {
	repository *pg.Repository
	balance    *balancev1.Connector
	outbox     outbox
	cfg        processorconfig.Config
}

type balanceTopUpEvent struct {
	UserID int64 `json:"user_id"`
}

type cryptoDepositConfirmedEvent struct {
	UserID        int64  `json:"user_id"`
	Address       string `json:"address"`
	Network       string `json:"network"`
	Asset         string `json:"asset"`
	TxHash        string `json:"tx_hash"`
	LogIndex      int64  `json:"log_index"`
	Amount        string `json:"amount"`
	BlockNumber   int64  `json:"block_number"`
	Confirmations int64  `json:"confirmations"`
}

func New(repository *pg.Repository, balance *balancev1.Connector, outbox outbox, cfg processorconfig.Config) *Service {
	return &Service{
		repository: repository,
		balance:    balance,
		outbox:     outbox,
		cfg:        cfg,
	}
}

func (s *Service) HandleOrderCreate(ctx context.Context, event dto.Event) error {
	var order ordomain.Order

	if err := json.Unmarshal(event.Payload, &order); err != nil {
		return err
	}

	now := time.Now().UTC()

	return s.repository.Create(ctx, paydomain.Payment{
		OrderID:           order.ID,
		UserID:            order.UserID,
		Amount:            order.TotalPrice,
		CurrencyCode:      order.Payment.CurrencyID,
		Status:            paydomain.StatusPendingFunds,
		AttemptCount:      0,
		PaymentDeadlineAt: now.Add(s.cfg.PaymentTimeout),
		NextAttemptAt:     now,
		LastError:         "",
		CreatedAt:         now,
		UpdatedAt:         now,
	})
}

func (s *Service) HandleOrderCancel(ctx context.Context, event dto.Event) error {
	var payload paydomain.Event
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	payment, err := s.repository.GetByOrderID(ctx, payload.OrderID)
	if err != nil {
		if pg.IsNotFound(err) {
			return nil
		}
		return err
	}

	switch payment.Status {
	case paydomain.StatusReserved:
		return s.releasePayment(ctx, *payment, "order_cancelled")
	case paydomain.StatusPendingFunds, paydomain.StatusFailed:
		return s.repository.MarkCancelled(ctx, payload.OrderID, "order_cancelled")
	default:
		return nil
	}
}

func (s *Service) HandleOrderPickedUp(ctx context.Context, event dto.Event) error {
	var payload paydomain.Event
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	payment, err := s.repository.GetByOrderID(ctx, payload.OrderID)
	if err != nil {
		if pg.IsNotFound(err) {
			return nil
		}
		return err
	}
	if payment.Status != paydomain.StatusReserved {
		return nil
	}

	resp, err := s.balance.Order.CaptureFunds(ctx, &orderPb.CaptureFundsRequest{
		UserId:         payment.UserID,
		OrderId:        payment.OrderID,
		IdempotencyKey: fmt.Sprintf("payment-capture-%d", payment.OrderID),
		Reason:         "order_picked_up",
		Money: &domainPb.Money{
			Amount:       payment.Amount,
			CurrencyCode: payment.CurrencyCode,
		},
	})
	if err != nil {
		return err
	}

	var txID int64
	if resp.GetTransaction() != nil {
		txID = resp.GetTransaction().GetId()
	}

	if err := s.repository.MarkCaptured(ctx, payment.OrderID, txID); err != nil {
		return err
	}

	return s.outbox.Send(ctx, eventPaymentCaptured, paydomain.Event{
		OrderID:      payment.OrderID,
		UserID:       payment.UserID,
		Status:       paydomain.StatusCaptured,
		Amount:       payment.Amount,
		CurrencyCode: payment.CurrencyCode,
	})
}

func (s *Service) HandleBalanceTopUp(ctx context.Context, event dto.Event) error {
	var payload balanceTopUpEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	return s.repository.WakePendingByUserID(ctx, payload.UserID, time.Now().UTC())
}

func (s *Service) HandleCryptoDepositConfirmed(ctx context.Context, event dto.Event) error {
	var payload cryptoDepositConfirmedEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	if payload.Network != "TRON" || payload.Asset != "USDT" {
		return nil
	}

	_, err := s.balance.Admin.PostAdjustment(ctx, &adminPb.PostAdjustmentRequest{
		OwnerType:      domainPb.WalletOwnerType_WALLET_OWNER_TYPE_USER,
		OwnerId:        payload.UserID,
		IdempotencyKey: fmt.Sprintf("crypto-deposit-%s-%d", payload.TxHash, payload.LogIndex),
		Reason:         fmt.Sprintf("confirmed crypto deposit %s:%d", payload.TxHash, payload.LogIndex),
		Money: &domainPb.Money{
			Amount:       payload.Amount,
			CurrencyCode: int64(currency.USDTinTRC),
		},
	})
	if err != nil {
		return err
	}

	return s.outbox.Send(ctx, "balance_topup_completed", balanceTopUpEvent{
		UserID: payload.UserID,
	})
}

func (s *Service) ProcessDuePayments(ctx context.Context) error {
	payments, err := s.repository.AcquireDuePayments(ctx, s.cfg.BatchSize, s.cfg.ProcessingLockDuration)
	if err != nil {
		return err
	}

	for _, payment := range payments {
		if err := s.processPayment(ctx, payment); err != nil {
			_ = s.repository.ReleaseProcessingLock(ctx, payment.OrderID)
		}
	}

	return nil
}

func (s *Service) processPayment(ctx context.Context, payment paydomain.Payment) error {
	now := time.Now().UTC()

	switch payment.Status {
	case paydomain.StatusPendingFunds:
		if !payment.PaymentDeadlineAt.After(now) {
			if err := s.repository.MarkExpired(ctx, payment.OrderID, 0, "payment_deadline_exceeded"); err != nil {
				return err
			}

			return s.outbox.Send(ctx, eventPaymentExpired, paydomain.Event{
				OrderID:      payment.OrderID,
				UserID:       payment.UserID,
				Status:       paydomain.StatusExpired,
				Amount:       payment.Amount,
				CurrencyCode: payment.CurrencyCode,
				Reason:       "payment_deadline_exceeded",
			})
		}

		return s.tryReserve(ctx, payment)
	case paydomain.StatusReserved:
		if payment.PaymentDeadlineAt.After(now) {
			return s.repository.ReleaseProcessingLock(ctx, payment.OrderID)
		}

		return s.releasePayment(ctx, payment, "payment_deadline_exceeded")
	default:
		return s.repository.ReleaseProcessingLock(ctx, payment.OrderID)
	}
}

func (s *Service) tryReserve(ctx context.Context, payment paydomain.Payment) error {
	resp, err := s.balance.Order.ReserveFunds(ctx, &orderPb.ReserveFundsRequest{
		UserId:         payment.UserID,
		OrderId:        payment.OrderID,
		IdempotencyKey: fmt.Sprintf("payment-reserve-%d", payment.OrderID),
		Reason:         "order_create",
		Money: &domainPb.Money{
			Amount:       payment.Amount,
			CurrencyCode: payment.CurrencyCode,
		},
	})
	if err != nil {
		if isInsufficientFunds(err) {
			return s.repository.MarkRetry(
				ctx,
				payment.OrderID,
				err.Error(),
				time.Now().UTC().Add(s.cfg.RetryDelay),
			)
		}

		return err
	}

	var txID int64
	if resp.GetTransaction() != nil {
		txID = resp.GetTransaction().GetId()
	}

	if err := s.repository.MarkReserved(ctx, payment.OrderID, txID, payment.PaymentDeadlineAt); err != nil {
		return err
	}

	return s.outbox.Send(ctx, eventOrderPaid, paydomain.Event{
		OrderID:      payment.OrderID,
		UserID:       payment.UserID,
		Status:       paydomain.StatusReserved,
		Amount:       payment.Amount,
		CurrencyCode: payment.CurrencyCode,
	})
}

func (s *Service) releasePayment(ctx context.Context, payment paydomain.Payment, reason string) error {
	resp, err := s.balance.Order.ReleaseFunds(ctx, &orderPb.ReleaseFundsRequest{
		UserId:         payment.UserID,
		OrderId:        payment.OrderID,
		IdempotencyKey: fmt.Sprintf("payment-release-%d", payment.OrderID),
		Reason:         reason,
		Money: &domainPb.Money{
			Amount:       payment.Amount,
			CurrencyCode: payment.CurrencyCode,
		},
	})
	if err != nil {
		return err
	}

	var txID int64
	if resp.GetTransaction() != nil {
		txID = resp.GetTransaction().GetId()
	}

	if reason == "payment_deadline_exceeded" {
		if err := s.repository.MarkExpired(ctx, payment.OrderID, txID, reason); err != nil {
			return err
		}

		return s.outbox.Send(ctx, eventPaymentExpired, paydomain.Event{
			OrderID:      payment.OrderID,
			UserID:       payment.UserID,
			Status:       paydomain.StatusExpired,
			Amount:       payment.Amount,
			CurrencyCode: payment.CurrencyCode,
			Reason:       reason,
		})
	}

	if err := s.repository.MarkReleased(ctx, payment.OrderID, txID, reason); err != nil {
		return err
	}

	return s.outbox.Send(ctx, eventPaymentReleased, paydomain.Event{
		OrderID:      payment.OrderID,
		UserID:       payment.UserID,
		Status:       paydomain.StatusReleased,
		Amount:       payment.Amount,
		CurrencyCode: payment.CurrencyCode,
		Reason:       reason,
	})
}

func isInsufficientFunds(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}
