package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/persistence/postgres/db"
	"go.uber.org/zap"
)

type PostgresWithdrawalRepository struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	logger  *zap.Logger
}

type PostgresWithdrawalRepositoryOption func(*PostgresWithdrawalRepository)

func WithPostgresLogger(logger *zap.Logger) PostgresWithdrawalRepositoryOption {
	return func(r *PostgresWithdrawalRepository) {
		r.logger = logger
	}
}

func NewPostgresWithdrawalRepository(pool *pgxpool.Pool, opts ...PostgresWithdrawalRepositoryOption) *PostgresWithdrawalRepository {
	r := &PostgresWithdrawalRepository{
		pool:    pool,
		queries: db.New(pool),
		logger:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *PostgresWithdrawalRepository) Create(ctx context.Context, withdrawal *entities.Withdrawal) error {
	_, err := r.queries.CreateWithdrawal(ctx, db.CreateWithdrawalParams{
		ID:                withdrawal.ID,
		IdempotencyKey:    withdrawal.IdempotencyKey,
		UserID:            withdrawal.UserID,
		Asset:             withdrawal.Asset,
		Amount:            withdrawal.Amount.String(),
		DestinationAddr:   withdrawal.DestinationAddr,
		Network:           withdrawal.Network,
		Status:            withdrawal.Status.String(),
		RetryCount:        int32(withdrawal.RetryCount),
		MaxRetries:        int32(withdrawal.MaxRetries),
		ErrorMessage:      db.ToPgText(withdrawal.ErrorMessage),
		TxHash:            db.ToPgText(withdrawal.TxHash),
		CreatedAt:         pgtype.Timestamptz{Time: withdrawal.CreatedAt, Valid: true},
		UpdatedAt:         pgtype.Timestamptz{Time: withdrawal.UpdatedAt, Valid: true},
		ProcessedAt:       db.ToPgTimestamptz(withdrawal.ProcessedAt),
		ProcessingVersion: withdrawal.ProcessingVersion,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entities.ErrWithdrawalNotFound
		}
		if isDuplicateKeyError(err) {
			return entities.ErrWithdrawalAlreadyExist
		}
		return fmt.Errorf("failed to create withdrawal: %w", err)
	}
	r.logger.Debug("created withdrawal", zap.String("id", withdrawal.ID))
	return nil
}

func (r *PostgresWithdrawalRepository) GetByID(ctx context.Context, id string) (*entities.Withdrawal, error) {
	row, err := r.queries.GetWithdrawalByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, entities.ErrWithdrawalNotFound
		}
		return nil, fmt.Errorf("failed to get withdrawal: %w", err)
	}
	return r.scanWithdrawal(row), nil
}

func (r *PostgresWithdrawalRepository) GetByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error) {
	row, err := r.queries.GetWithdrawalByIdempotencyKey(ctx, key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, entities.ErrWithdrawalNotFound
		}
		return nil, fmt.Errorf("failed to get withdrawal by idempotency key: %w", err)
	}
	return r.scanWithdrawal(row), nil
}

func (r *PostgresWithdrawalRepository) Update(ctx context.Context, withdrawal *entities.Withdrawal) error {
	row, err := r.queries.UpdateWithdrawal(ctx, db.UpdateWithdrawalParams{
		ID:           withdrawal.ID,
		Status:       withdrawal.Status.String(),
		RetryCount:   int32(withdrawal.RetryCount),
		ErrorMessage: db.ToPgText(withdrawal.ErrorMessage),
		TxHash:       db.ToPgText(withdrawal.TxHash),
		UpdatedAt:    pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		ProcessedAt:  db.ToPgTimestamptz(withdrawal.ProcessedAt),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entities.ErrWithdrawalNotFound
		}
		return fmt.Errorf("failed to update withdrawal: %w", err)
	}
	withdrawal.ProcessingVersion = row.ProcessingVersion
	withdrawal.UpdatedAt = row.UpdatedAt.Time
	return nil
}

func (r *PostgresWithdrawalRepository) UpdateWithVersion(ctx context.Context, withdrawal *entities.Withdrawal, expectedVersion int64) error {
	row, err := r.queries.UpdateWithdrawalWithVersion(ctx, db.UpdateWithdrawalWithVersionParams{
		ID:                withdrawal.ID,
		Status:            withdrawal.Status.String(),
		RetryCount:        int32(withdrawal.RetryCount),
		ErrorMessage:      db.ToPgText(withdrawal.ErrorMessage),
		TxHash:            db.ToPgText(withdrawal.TxHash),
		UpdatedAt:         pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		ProcessedAt:       db.ToPgTimestamptz(withdrawal.ProcessedAt),
		ProcessingVersion: expectedVersion,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("version conflict: expected %d", expectedVersion)
		}
		return fmt.Errorf("failed to update withdrawal with version: %w", err)
	}
	withdrawal.ProcessingVersion = row.ProcessingVersion
	withdrawal.UpdatedAt = row.UpdatedAt.Time
	return nil
}

func (r *PostgresWithdrawalRepository) ListByUserID(ctx context.Context, userID string, limit, offset int) ([]*entities.Withdrawal, error) {
	rows, err := r.queries.ListWithdrawalsByUserID(ctx, db.ListWithdrawalsByUserIDParams{
		UserID: userID,
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list withdrawals by user: %w", err)
	}
	withdrawals := make([]*entities.Withdrawal, 0, len(rows))
	for _, row := range rows {
		withdrawals = append(withdrawals, r.scanWithdrawal(row))
	}
	return withdrawals, nil
}

func (r *PostgresWithdrawalRepository) ListByStatus(ctx context.Context, status entities.WithdrawalStatus, limit int) ([]*entities.Withdrawal, error) {
	rows, err := r.queries.ListWithdrawalsByStatus(ctx, db.ListWithdrawalsByStatusParams{
		Status: status.String(),
		Limit:  int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list withdrawals by status: %w", err)
	}
	withdrawals := make([]*entities.Withdrawal, 0, len(rows))
	for _, row := range rows {
		withdrawals = append(withdrawals, r.scanWithdrawal(row))
	}
	return withdrawals, nil
}

func (r *PostgresWithdrawalRepository) GetPendingWithdrawals(ctx context.Context, limit int) ([]*entities.Withdrawal, error) {
	rows, err := r.queries.GetPendingWithdrawals(ctx, int32(limit))
	if err != nil {
		return nil, fmt.Errorf("failed to get pending withdrawals: %w", err)
	}
	withdrawals := make([]*entities.Withdrawal, 0, len(rows))
	for _, row := range rows {
		withdrawals = append(withdrawals, r.scanWithdrawal(row))
	}
	return withdrawals, nil
}

func (r *PostgresWithdrawalRepository) Delete(ctx context.Context, id string) error {
	return r.queries.DeleteWithdrawal(ctx, id)
}

func (r *PostgresWithdrawalRepository) scanWithdrawal(row db.Withdrawal) *entities.Withdrawal {
	amount := new(big.Int)
	amount.SetString(row.Amount, 10)
	return &entities.Withdrawal{
		ID:                row.ID,
		IdempotencyKey:    row.IdempotencyKey,
		UserID:            row.UserID,
		Asset:             row.Asset,
		Amount:            amount,
		DestinationAddr:   row.DestinationAddr,
		Network:           row.Network,
		Status:            stringToStatus(row.Status),
		RetryCount:        int(row.RetryCount),
		MaxRetries:        int(row.MaxRetries),
		ErrorMessage:      db.FromPgText(row.ErrorMessage),
		TxHash:            db.FromPgText(row.TxHash),
		CreatedAt:         row.CreatedAt.Time,
		UpdatedAt:         row.UpdatedAt.Time,
		ProcessedAt:       db.FromPgTimestamptz(row.ProcessedAt),
		ProcessingVersion: row.ProcessingVersion,
	}
}

func stringToStatus(s string) entities.WithdrawalStatus {
	switch s {
	case "PENDING":
		return entities.StatusPending
	case "PROCESSING":
		return entities.StatusProcessing
	case "COMPLETED":
		return entities.StatusCompleted
	case "FAILED":
		return entities.StatusFailed
	case "RETRYING":
		return entities.StatusRetrying
	default:
		return entities.StatusUnspecified
	}
}

func isDuplicateKeyError(err error) bool {
	return err != nil && (err.Error() == "ERROR: duplicate key value violates unique constraint" ||
		err.Error() == "ERROR: duplicate key value violates unique constraint \"withdrawals_pkey\"" ||
		err.Error() == "ERROR: duplicate key value violates unique constraint \"idx_withdrawals_idempotency_key\"")
}

var _ repositories.WithdrawalRepository = (*PostgresWithdrawalRepository)(nil)
