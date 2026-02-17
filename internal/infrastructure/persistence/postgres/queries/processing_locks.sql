-- name: AcquireProcessingLock :one
INSERT INTO processing_locks (withdrawal_id, locked_by, locked_at, expires_at)
VALUES ($1, $2, NOW(), NOW() + INTERVAL '1 second' * $3)
ON CONFLICT (withdrawal_id) DO UPDATE
SET locked_by = EXCLUDED.locked_by,
    locked_at = EXCLUDED.locked_at,
    expires_at = EXCLUDED.expires_at
WHERE processing_locks.expires_at <= NOW()
RETURNING *;

-- name: ReleaseProcessingLock :exec
DELETE FROM processing_locks WHERE withdrawal_id = $1 AND locked_by = $2;

-- name: IsProcessingLocked :one
SELECT EXISTS (
    SELECT 1 FROM processing_locks 
    WHERE withdrawal_id = $1 AND expires_at > NOW()
);

-- name: ExtendProcessingLock :one
UPDATE processing_locks
SET expires_at = expires_at + INTERVAL '1 second' * $3
WHERE withdrawal_id = $1 AND locked_by = $2 AND expires_at > NOW()
RETURNING *;

-- name: DeleteExpiredProcessingLocks :exec
DELETE FROM processing_locks WHERE expires_at <= NOW();
