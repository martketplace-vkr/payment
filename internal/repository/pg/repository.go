package pg

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/martketplace-vkr/payment/internal/domain"
)

type Repository struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Create(ctx context.Context, payment domain.Payment) error {
	query := `
		insert into "payment".payment (
			order_id,
			user_id,
			amount,
			currency_code,
			status,
			attempt_count,
			payment_deadline_at,
			next_attempt_at,
			last_error,
			created_at,
			updated_at
		) values (
			:order_id,
			:user_id,
			:amount,
			:currency_code,
			:status,
			:attempt_count,
			:payment_deadline_at,
			:next_attempt_at,
			:last_error,
			:created_at,
			:updated_at
		)
		on conflict (order_id) do nothing
	`

	_, err := r.db.NamedExecContext(ctx, query, payment)
	return err
}

func (r *Repository) GetByOrderID(ctx context.Context, orderID int64) (*domain.Payment, error) {
	query := `
		select
			id,
			order_id,
			user_id,
			amount,
			currency_code,
			status,
			attempt_count,
			payment_deadline_at,
			next_attempt_at,
			processing_locked_until,
			last_error,
			hold_transaction_id,
			capture_transaction_id,
			release_transaction_id,
			created_at,
			updated_at
		from "payment".payment
		where order_id = $1
	`

	var payment domain.Payment
	if err := r.db.GetContext(ctx, &payment, query, orderID); err != nil {
		return nil, err
	}

	return &payment, nil
}

func (r *Repository) AcquireDuePayments(ctx context.Context, batchSize int, lockDuration time.Duration) ([]domain.Payment, error) {
	query := `
		with picked as (
			select id
			from "payment".payment
			where status in ('pending_funds', 'reserved')
				and next_attempt_at <= now()
				and (processing_locked_until is null or processing_locked_until <= now())
			order by next_attempt_at asc, id asc
			limit $1
			for update skip locked
		)
		update "payment".payment p
		set
			processing_locked_until = now() + make_interval(secs => $2),
			updated_at = now()
		from picked
		where p.id = picked.id
		returning
			p.id,
			p.order_id,
			p.user_id,
			p.amount,
			p.currency_code,
			p.status,
			p.attempt_count,
			p.payment_deadline_at,
			p.next_attempt_at,
			p.processing_locked_until,
			p.last_error,
			p.hold_transaction_id,
			p.capture_transaction_id,
			p.release_transaction_id,
			p.created_at,
			p.updated_at
	`

	lockSeconds := int(lockDuration / time.Second)
	if lockSeconds <= 0 {
		lockSeconds = 1
	}

	payments := make([]domain.Payment, 0, batchSize)
	if err := r.db.SelectContext(ctx, &payments, query, batchSize, lockSeconds); err != nil {
		return nil, err
	}

	return payments, nil
}

func (r *Repository) MarkRetry(ctx context.Context, orderID int64, lastError string, nextAttemptAt time.Time) error {
	query := `
		update "payment".payment
		set
			status = 'pending_funds',
			attempt_count = attempt_count + 1,
			next_attempt_at = $2,
			processing_locked_until = null,
			last_error = $3,
			updated_at = now()
		where order_id = $1
			and status = 'pending_funds'
	`

	_, err := r.db.ExecContext(ctx, query, orderID, nextAttemptAt, lastError)
	return err
}

func (r *Repository) MarkReserved(ctx context.Context, orderID int64, txID int64, nextAttemptAt time.Time) error {
	query := `
		update "payment".payment
		set
			status = 'reserved',
			attempt_count = attempt_count + 1,
			hold_transaction_id = $2,
			next_attempt_at = $3,
			processing_locked_until = null,
			last_error = '',
			updated_at = now()
		where order_id = $1
			and status = 'pending_funds'
	`

	_, err := r.db.ExecContext(ctx, query, orderID, txID, nextAttemptAt)
	return err
}

func (r *Repository) MarkExpired(ctx context.Context, orderID int64, txID int64, reason string) error {
	query := `
		update "payment".payment
		set
			status = 'expired',
			release_transaction_id = nullif($2, 0),
			next_attempt_at = now(),
			processing_locked_until = null,
			last_error = $3,
			updated_at = now()
		where order_id = $1
			and status in ('pending_funds', 'reserved')
	`

	_, err := r.db.ExecContext(ctx, query, orderID, txID, reason)
	return err
}

func (r *Repository) MarkReleased(ctx context.Context, orderID int64, txID int64, reason string) error {
	query := `
		update "payment".payment
		set
			status = 'released',
			release_transaction_id = $2,
			next_attempt_at = now(),
			processing_locked_until = null,
			last_error = $3,
			updated_at = now()
		where order_id = $1
			and status = 'reserved'
	`

	_, err := r.db.ExecContext(ctx, query, orderID, txID, reason)
	return err
}

func (r *Repository) MarkCaptured(ctx context.Context, orderID int64, txID int64) error {
	query := `
		update "payment".payment
		set
			status = 'captured',
			capture_transaction_id = $2,
			next_attempt_at = now(),
			processing_locked_until = null,
			last_error = '',
			updated_at = now()
		where order_id = $1
			and status = 'reserved'
	`

	_, err := r.db.ExecContext(ctx, query, orderID, txID)
	return err
}

func (r *Repository) MarkCancelled(ctx context.Context, orderID int64, reason string) error {
	query := `
		update "payment".payment
		set
			status = 'cancelled',
			next_attempt_at = now(),
			processing_locked_until = null,
			last_error = $2,
			updated_at = now()
		where order_id = $1
			and status in ('pending_funds', 'failed')
	`

	_, err := r.db.ExecContext(ctx, query, orderID, reason)
	return err
}

func (r *Repository) WakePendingByUserID(ctx context.Context, userID int64, nextAttemptAt time.Time) error {
	query := `
		update "payment".payment
		set
			next_attempt_at = $2,
			processing_locked_until = null,
			updated_at = now()
		where user_id = $1
			and status = 'pending_funds'
			and payment_deadline_at > now()
	`

	_, err := r.db.ExecContext(ctx, query, userID, nextAttemptAt)
	return err
}

func (r *Repository) ReleaseProcessingLock(ctx context.Context, orderID int64) error {
	query := `
		update "payment".payment
		set
			processing_locked_until = null,
			updated_at = now()
		where order_id = $1
	`

	_, err := r.db.ExecContext(ctx, query, orderID)
	return err
}

func IsNotFound(err error) bool {
	return err == sql.ErrNoRows
}
