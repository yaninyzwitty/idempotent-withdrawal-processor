-- name: CreateWithdrawal :one
INSERT INTO withdrawals (
    id, idempotency_key, user_id, asset, amount, destination_addr, network,
    status, retry_count, max_retries, error_message, tx_hash,
    created_at, updated_at, processed_at, processing_version
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
) RETURNING *;

-- name: GetWithdrawalByID :one
SELECT * FROM withdrawals WHERE id = $1;

-- name: GetWithdrawalByIdempotencyKey :one
SELECT * FROM withdrawals WHERE idempotency_key = $1;

-- name: UpdateWithdrawal :one
UPDATE withdrawals SET
    status = $2,
    retry_count = $3,
    error_message = $4,
    tx_hash = $5,
    updated_at = $6,
    processed_at = $7,
    processing_version = processing_version + 1
WHERE id = $1
RETURNING *;

-- name: UpdateWithdrawalWithVersion :one
UPDATE withdrawals SET
    status = $2,
    retry_count = $3,
    error_message = $4,
    tx_hash = $5,
    updated_at = $6,
    processed_at = $7,
    processing_version = processing_version + 1
WHERE id = $1 AND processing_version = $8
RETURNING *;

-- name: ListWithdrawalsByUserID :many
SELECT * FROM withdrawals WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3;

-- name: ListWithdrawalsByStatus :many
SELECT * FROM withdrawals WHERE status = $1 ORDER BY created_at ASC LIMIT $2;

-- name: GetPendingWithdrawals :many
SELECT * FROM withdrawals WHERE status IN ('PENDING', 'RETRYING') ORDER BY created_at ASC LIMIT $1;

-- name: DeleteWithdrawal :exec
DELETE FROM withdrawals WHERE id = $1;
