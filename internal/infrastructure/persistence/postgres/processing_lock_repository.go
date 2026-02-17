package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/persistence/postgres/db"
)

type PostgresProcessingLockRepository struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewPostgresProcessingLockRepository(pool *pgxpool.Pool) *PostgresProcessingLockRepository {
	r := &PostgresProcessingLockRepository{
		pool:    pool,
		queries: db.New(pool),
	}
	go r.cleanup()
	return r
}

func (r *PostgresProcessingLockRepository) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	ctx := context.Background()
	for range ticker.C {
		_ = r.queries.DeleteExpiredProcessingLocks(ctx)
	}
}

func (r *PostgresProcessingLockRepository) Acquire(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
	row, err := r.queries.AcquireProcessingLock(ctx, db.AcquireProcessingLockParams{
		WithdrawalID: withdrawalID,
		LockedBy:     db.ToPgText(processorID),
		Column3:      ttl,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to acquire processing lock: %w", err)
	}
	return db.FromPgText(row.LockedBy) == processorID, nil
}

func (r *PostgresProcessingLockRepository) Release(ctx context.Context, withdrawalID string, processorID string) error {
	return r.queries.ReleaseProcessingLock(ctx, db.ReleaseProcessingLockParams{
		WithdrawalID: withdrawalID,
		LockedBy:     db.ToPgText(processorID),
	})
}

func (r *PostgresProcessingLockRepository) IsLocked(ctx context.Context, withdrawalID string) (bool, error) {
	return r.queries.IsProcessingLocked(ctx, withdrawalID)
}

func (r *PostgresProcessingLockRepository) Extend(ctx context.Context, withdrawalID string, processorID string, additionalTTL int64) error {
	_, err := r.queries.ExtendProcessingLock(ctx, db.ExtendProcessingLockParams{
		WithdrawalID: withdrawalID,
		LockedBy:     db.ToPgText(processorID),
		Column3:      additionalTTL,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock not found or expired")
		}
		return fmt.Errorf("failed to extend processing lock: %w", err)
	}
	return nil
}

var _ repositories.ProcessingLockRepository = (*PostgresProcessingLockRepository)(nil)
