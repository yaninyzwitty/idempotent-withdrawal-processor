package repositories

import (
	"context"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
)

type WithdrawalRepository interface {
	Create(ctx context.Context, withdrawal *entities.Withdrawal) error
	GetByID(ctx context.Context, id string) (*entities.Withdrawal, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error)
	Update(ctx context.Context, withdrawal *entities.Withdrawal) error
	UpdateWithVersion(ctx context.Context, withdrawal *entities.Withdrawal, expectedVersion int64) error
	ListByUserID(ctx context.Context, userID string, limit, offset int) ([]*entities.Withdrawal, error)
	ListByStatus(ctx context.Context, status entities.WithdrawalStatus, limit int) ([]*entities.Withdrawal, error)
	GetPendingWithdrawals(ctx context.Context, limit int) ([]*entities.Withdrawal, error)
}

type IdempotencyRepository interface {
	Store(ctx context.Context, key *entities.IdempotencyKey) error
	Get(ctx context.Context, key string) (*entities.IdempotencyKey, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Acquire(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error)
	Release(ctx context.Context, key string) error
}

type ProcessingLockRepository interface {
	Acquire(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error)
	Release(ctx context.Context, withdrawalID string, processorID string) error
	IsLocked(ctx context.Context, withdrawalID string) (bool, error)
	Extend(ctx context.Context, withdrawalID string, processorID string, additionalTTL int64) error
}

type EventRepository interface {
	Publish(ctx context.Context, event *entities.WithdrawalEvent) error
	Subscribe(ctx context.Context, handler func(event *entities.WithdrawalEvent) error) error
}
