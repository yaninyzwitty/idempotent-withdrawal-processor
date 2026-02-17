-- name: StoreIdempotencyKey :one
INSERT INTO idempotency_keys (key, withdrawal_id, created_at, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetIdempotencyKey :one
SELECT * FROM idempotency_keys WHERE key = $1;

-- name: DeleteIdempotencyKey :exec
DELETE FROM idempotency_keys WHERE key = $1;

-- name: ExistsIdempotencyKey :one
SELECT EXISTS (
    SELECT 1 FROM idempotency_keys 
    WHERE key = $1 AND expires_at > NOW()
);

-- name: AcquireIdempotencyKey :one
INSERT INTO idempotency_keys (key, withdrawal_id, created_at, expires_at)
VALUES ($1, $2, NOW(), NOW() + INTERVAL '1 second' * $3)
ON CONFLICT (key) DO UPDATE
SET withdrawal_id = EXCLUDED.withdrawal_id,
    expires_at = EXCLUDED.expires_at,
    created_at = EXCLUDED.created_at
WHERE idempotency_keys.expires_at <= NOW()
RETURNING *;

-- name: ReleaseIdempotencyKey :exec
DELETE FROM idempotency_keys WHERE key = $1;

-- name: DeleteExpiredIdempotencyKeys :exec
DELETE FROM idempotency_keys WHERE expires_at <= NOW();
