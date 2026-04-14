create schema if not exists "payment";

create table if not exists "payment".payment (
    id bigserial primary key,
    order_id bigint not null unique,
    user_id bigint not null,
    amount text not null,
    currency_code bigint not null,
    status text not null,
    attempt_count integer not null default 0,
    payment_deadline_at timestamp not null,
    next_attempt_at timestamp not null,
    processing_locked_until timestamp null,
    last_error text not null default '',
    hold_transaction_id bigint null,
    capture_transaction_id bigint null,
    release_transaction_id bigint null,
    created_at timestamp not null,
    updated_at timestamp not null
);

create index if not exists idx_payment_due
    on "payment".payment (status, next_attempt_at, processing_locked_until);

create index if not exists idx_payment_user_status
    on "payment".payment (user_id, status, payment_deadline_at);
