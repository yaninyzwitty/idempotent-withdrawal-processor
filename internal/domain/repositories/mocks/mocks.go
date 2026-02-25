package mocks

import (
	"context"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
)

type MockWithdrawalRepository struct {
	CreateFunc                func(ctx context.Context, withdrawal *entities.Withdrawal) error
	GetByIDFunc               func(ctx context.Context, id string) (*entities.Withdrawal, error)
	GetByIdempotencyKeyFunc   func(ctx context.Context, key string) (*entities.Withdrawal, error)
	UpdateFunc                func(ctx context.Context, withdrawal *entities.Withdrawal) error
	UpdateWithVersionFunc     func(ctx context.Context, withdrawal *entities.Withdrawal, expectedVersion int64) error
	DeleteFunc                func(ctx context.Context, id string) error
	ListByUserIDFunc          func(ctx context.Context, userID string, limit, offset int) ([]*entities.Withdrawal, error)
	ListByStatusFunc          func(ctx context.Context, status entities.WithdrawalStatus, limit int) ([]*entities.Withdrawal, error)
	GetPendingWithdrawalsFunc func(ctx context.Context, limit int) ([]*entities.Withdrawal, error)
}

func (m *MockWithdrawalRepository) Create(ctx context.Context, withdrawal *entities.Withdrawal) error {
	if m.CreateFunc != nil {
		return m.CreateFunc(ctx, withdrawal)
	}
	return nil
}

func (m *MockWithdrawalRepository) GetByID(ctx context.Context, id string) (*entities.Withdrawal, error) {
	if m.GetByIDFunc != nil {
		return m.GetByIDFunc(ctx, id)
	}
	return nil, entities.ErrWithdrawalNotFound
}

func (m *MockWithdrawalRepository) GetByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error) {
	if m.GetByIdempotencyKeyFunc != nil {
		return m.GetByIdempotencyKeyFunc(ctx, key)
	}
	return nil, entities.ErrWithdrawalNotFound
}

func (m *MockWithdrawalRepository) Update(ctx context.Context, withdrawal *entities.Withdrawal) error {
	if m.UpdateFunc != nil {
		return m.UpdateFunc(ctx, withdrawal)
	}
	return nil
}

func (m *MockWithdrawalRepository) UpdateWithVersion(ctx context.Context, withdrawal *entities.Withdrawal, expectedVersion int64) error {
	if m.UpdateWithVersionFunc != nil {
		return m.UpdateWithVersionFunc(ctx, withdrawal, expectedVersion)
	}
	return nil
}

func (m *MockWithdrawalRepository) ListByUserID(ctx context.Context, userID string, limit, offset int) ([]*entities.Withdrawal, error) {
	if m.ListByUserIDFunc != nil {
		return m.ListByUserIDFunc(ctx, userID, limit, offset)
	}
	return []*entities.Withdrawal{}, nil
}

func (m *MockWithdrawalRepository) ListByStatus(ctx context.Context, status entities.WithdrawalStatus, limit int) ([]*entities.Withdrawal, error) {
	if m.ListByStatusFunc != nil {
		return m.ListByStatusFunc(ctx, status, limit)
	}
	return []*entities.Withdrawal{}, nil
}

func (m *MockWithdrawalRepository) GetPendingWithdrawals(ctx context.Context, limit int) ([]*entities.Withdrawal, error) {
	if m.GetPendingWithdrawalsFunc != nil {
		return m.GetPendingWithdrawalsFunc(ctx, limit)
	}
	return []*entities.Withdrawal{}, nil
}

func (m *MockWithdrawalRepository) Delete(ctx context.Context, id string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, id)
	}
	return nil
}

var _ repositories.WithdrawalRepository = (*MockWithdrawalRepository)(nil)

type MockIdempotencyRepository struct {
	StoreFunc   func(ctx context.Context, key *entities.IdempotencyKey) error
	GetFunc     func(ctx context.Context, key string) (*entities.IdempotencyKey, error)
	DeleteFunc  func(ctx context.Context, key string) error
	ExistsFunc  func(ctx context.Context, key string) (bool, error)
	AcquireFunc func(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error)
	ReleaseFunc func(ctx context.Context, key string) error
}

func (m *MockIdempotencyRepository) Store(ctx context.Context, key *entities.IdempotencyKey) error {
	if m.StoreFunc != nil {
		return m.StoreFunc(ctx, key)
	}
	return nil
}

func (m *MockIdempotencyRepository) Get(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, key)
	}
	return nil, nil
}

func (m *MockIdempotencyRepository) Delete(ctx context.Context, key string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, key)
	}
	return nil
}

func (m *MockIdempotencyRepository) Exists(ctx context.Context, key string) (bool, error) {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(ctx, key)
	}
	return false, nil
}

func (m *MockIdempotencyRepository) Acquire(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
	if m.AcquireFunc != nil {
		return m.AcquireFunc(ctx, key, withdrawalID, ttl)
	}
	return true, nil
}

func (m *MockIdempotencyRepository) Release(ctx context.Context, key string) error {
	if m.ReleaseFunc != nil {
		return m.ReleaseFunc(ctx, key)
	}
	return nil
}

var _ repositories.IdempotencyRepository = (*MockIdempotencyRepository)(nil)

type MockProcessingLockRepository struct {
	AcquireFunc  func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error)
	ReleaseFunc  func(ctx context.Context, withdrawalID string, processorID string) error
	IsLockedFunc func(ctx context.Context, withdrawalID string) (bool, error)
	ExtendFunc   func(ctx context.Context, withdrawalID string, processorID string, additionalTTL int64) error
}

func (m *MockProcessingLockRepository) Acquire(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
	if m.AcquireFunc != nil {
		return m.AcquireFunc(ctx, withdrawalID, processorID, ttl)
	}
	return true, nil
}

func (m *MockProcessingLockRepository) Release(ctx context.Context, withdrawalID string, processorID string) error {
	if m.ReleaseFunc != nil {
		return m.ReleaseFunc(ctx, withdrawalID, processorID)
	}
	return nil
}

func (m *MockProcessingLockRepository) IsLocked(ctx context.Context, withdrawalID string) (bool, error) {
	if m.IsLockedFunc != nil {
		return m.IsLockedFunc(ctx, withdrawalID)
	}
	return false, nil
}

func (m *MockProcessingLockRepository) Extend(ctx context.Context, withdrawalID string, processorID string, additionalTTL int64) error {
	if m.ExtendFunc != nil {
		return m.ExtendFunc(ctx, withdrawalID, processorID, additionalTTL)
	}
	return nil
}

var _ repositories.ProcessingLockRepository = (*MockProcessingLockRepository)(nil)
