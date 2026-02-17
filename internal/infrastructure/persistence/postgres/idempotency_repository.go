package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/persistence/postgres/db"
)

type PostgresIdempotencyRepository struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	ttl     time.Duration
}

func NewPostgresIdempotencyRepository(pool *pgxpool.Pool, ttl time.Duration) *PostgresIdempotencyRepository {
	r := &PostgresIdempotencyRepository{
		pool:    pool,
		queries: db.New(pool),
		ttl:     ttl,
	}
	go r.cleanup()
	return r
}

func (r *PostgresIdempotencyRepository) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	ctx := context.Background()
	for range ticker.C {
		_ = r.queries.DeleteExpiredIdempotencyKeys(ctx)
	}
}

func (r *PostgresIdempotencyRepository) Store(ctx context.Context, key *entities.IdempotencyKey) error {
	_, err := r.queries.StoreIdempotencyKey(ctx, db.StoreIdempotencyKeyParams{
		Key:          key.Key,
		WithdrawalID: db.ToPgText(key.WithdrawalID),
		CreatedAt:    db.ToPgTimestamptz(&key.CreatedAt),
		ExpiresAt:    db.ToPgTimestamptz(&key.ExpiresAt),
	})
	if err != nil {
		return fmt.Errorf("failed to store idempotency key: %w", err)
	}
	return nil
}

func (r *PostgresIdempotencyRepository) Get(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
	row, err := r.queries.GetIdempotencyKey(ctx, key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("idempotency key not found: %s", key)
		}
		return nil, fmt.Errorf("failed to get idempotency key: %w", err)
	}
	return &entities.IdempotencyKey{
		Key:          row.Key,
		WithdrawalID: db.FromPgText(row.WithdrawalID),
		CreatedAt:    *db.FromPgTimestamptz(row.CreatedAt),
		ExpiresAt:    *db.FromPgTimestamptz(row.ExpiresAt),
	}, nil
}

func (r *PostgresIdempotencyRepository) Delete(ctx context.Context, key string) error {
	return r.queries.DeleteIdempotencyKey(ctx, key)
}

func (r *PostgresIdempotencyRepository) Exists(ctx context.Context, key string) (bool, error) {
	return r.queries.ExistsIdempotencyKey(ctx, key)
}

func (r *PostgresIdempotencyRepository) Acquire(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
	row, err := r.queries.AcquireIdempotencyKey(ctx, db.AcquireIdempotencyKeyParams{
		Key:          key,
		WithdrawalID: db.ToPgText(withdrawalID),
		Column3:      ttl,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to acquire idempotency key: %w", err)
	}
	return row.Key == key, nil
}

func (r *PostgresIdempotencyRepository) Release(ctx context.Context, key string) error {
	return r.queries.ReleaseIdempotencyKey(ctx, key)
}

var _ repositories.IdempotencyRepository = (*PostgresIdempotencyRepository)(nil)
