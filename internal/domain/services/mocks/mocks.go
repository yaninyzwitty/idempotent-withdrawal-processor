package mocks

import (
	"context"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
)

type MockBlockchainService struct {
	BroadcastTransactionFunc func(ctx context.Context, withdrawal *entities.Withdrawal) (string, error)
	GetTransactionStatusFunc func(ctx context.Context, txHash string) (string, error)
	ValidateAddressFunc      func(ctx context.Context, address string, network string) (bool, error)
	EstimateFeeFunc          func(ctx context.Context, asset string, network string) (string, error)
}

func (m *MockBlockchainService) BroadcastTransaction(ctx context.Context, withdrawal *entities.Withdrawal) (string, error) {
	if m.BroadcastTransactionFunc != nil {
		return m.BroadcastTransactionFunc(ctx, withdrawal)
	}
	return "0xmocktxhash", nil
}

func (m *MockBlockchainService) GetTransactionStatus(ctx context.Context, txHash string) (string, error) {
	if m.GetTransactionStatusFunc != nil {
		return m.GetTransactionStatusFunc(ctx, txHash)
	}
	return "confirmed", nil
}

func (m *MockBlockchainService) ValidateAddress(ctx context.Context, address string, network string) (bool, error) {
	if m.ValidateAddressFunc != nil {
		return m.ValidateAddressFunc(ctx, address, network)
	}
	return true, nil
}

func (m *MockBlockchainService) EstimateFee(ctx context.Context, asset string, network string) (string, error) {
	if m.EstimateFeeFunc != nil {
		return m.EstimateFeeFunc(ctx, asset, network)
	}
	return "1000000000000000", nil
}

var _ domain.BlockchainService = (*MockBlockchainService)(nil)

type MockWithdrawalService struct {
	CreateWithdrawalFunc              func(ctx context.Context, req *domain.CreateWithdrawalRequest) (*entities.Withdrawal, bool, error)
	GetWithdrawalFunc                 func(ctx context.Context, id string) (*entities.Withdrawal, error)
	GetWithdrawalByIdempotencyKeyFunc func(ctx context.Context, key string) (*entities.Withdrawal, error)
	ListWithdrawalsFunc               func(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error)
	RetryWithdrawalFunc               func(ctx context.Context, id string) (*entities.Withdrawal, error)
}

func (m *MockWithdrawalService) CreateWithdrawal(ctx context.Context, req *domain.CreateWithdrawalRequest) (*entities.Withdrawal, bool, error) {
	if m.CreateWithdrawalFunc != nil {
		return m.CreateWithdrawalFunc(ctx, req)
	}
	return nil, false, nil
}

func (m *MockWithdrawalService) GetWithdrawal(ctx context.Context, id string) (*entities.Withdrawal, error) {
	if m.GetWithdrawalFunc != nil {
		return m.GetWithdrawalFunc(ctx, id)
	}
	return nil, entities.ErrWithdrawalNotFound
}

func (m *MockWithdrawalService) GetWithdrawalByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error) {
	if m.GetWithdrawalByIdempotencyKeyFunc != nil {
		return m.GetWithdrawalByIdempotencyKeyFunc(ctx, key)
	}
	return nil, entities.ErrWithdrawalNotFound
}

func (m *MockWithdrawalService) ListWithdrawals(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error) {
	if m.ListWithdrawalsFunc != nil {
		return m.ListWithdrawalsFunc(ctx, userID, status, limit, offset)
	}
	return []*entities.Withdrawal{}, nil
}

func (m *MockWithdrawalService) RetryWithdrawal(ctx context.Context, id string) (*entities.Withdrawal, error) {
	if m.RetryWithdrawalFunc != nil {
		return m.RetryWithdrawalFunc(ctx, id)
	}
	return nil, entities.ErrWithdrawalNotFound
}

var _ domain.WithdrawalService = (*MockWithdrawalService)(nil)

type MockWithdrawalProcessor struct {
	ProcessFunc      func(ctx context.Context, withdrawal *entities.Withdrawal) error
	ProcessBatchFunc func(ctx context.Context, withdrawals []*entities.Withdrawal) error
	StartFunc        func(ctx context.Context) error
	StopFunc         func(ctx context.Context) error
}

func (m *MockWithdrawalProcessor) Process(ctx context.Context, withdrawal *entities.Withdrawal) error {
	if m.ProcessFunc != nil {
		return m.ProcessFunc(ctx, withdrawal)
	}
	return nil
}

func (m *MockWithdrawalProcessor) ProcessBatch(ctx context.Context, withdrawals []*entities.Withdrawal) error {
	if m.ProcessBatchFunc != nil {
		return m.ProcessBatchFunc(ctx, withdrawals)
	}
	return nil
}

func (m *MockWithdrawalProcessor) Start(ctx context.Context) error {
	if m.StartFunc != nil {
		return m.StartFunc(ctx)
	}
	return nil
}

func (m *MockWithdrawalProcessor) Stop(ctx context.Context) error {
	if m.StopFunc != nil {
		return m.StopFunc(ctx)
	}
	return nil
}

var _ domain.WithdrawalProcessor = (*MockWithdrawalProcessor)(nil)

type MockRetryService struct {
	ShouldRetryFunc       func(ctx context.Context, withdrawal *entities.Withdrawal, err error) bool
	GetNextRetryDelayFunc func(ctx context.Context, retryCount int) int64
	ScheduleRetryFunc     func(ctx context.Context, withdrawalID string, delaySeconds int64) error
}

func (m *MockRetryService) ShouldRetry(ctx context.Context, withdrawal *entities.Withdrawal, err error) bool {
	if m.ShouldRetryFunc != nil {
		return m.ShouldRetryFunc(ctx, withdrawal, err)
	}
	return true
}

func (m *MockRetryService) GetNextRetryDelay(ctx context.Context, retryCount int) int64 {
	if m.GetNextRetryDelayFunc != nil {
		return m.GetNextRetryDelayFunc(ctx, retryCount)
	}
	return 1
}

func (m *MockRetryService) ScheduleRetry(ctx context.Context, withdrawalID string, delaySeconds int64) error {
	if m.ScheduleRetryFunc != nil {
		return m.ScheduleRetryFunc(ctx, withdrawalID, delaySeconds)
	}
	return nil
}

var _ domain.RetryService = (*MockRetryService)(nil)

type MockIdempotencyService struct {
	AcquireKeyFunc  func(ctx context.Context, key string, withdrawalID string) (bool, error)
	ReleaseKeyFunc  func(ctx context.Context, key string) error
	CheckKeyFunc    func(ctx context.Context, key string) (*entities.IdempotencyKey, error)
	IsProcessedFunc func(ctx context.Context, key string) (bool, *entities.Withdrawal, error)
}

func (m *MockIdempotencyService) AcquireKey(ctx context.Context, key string, withdrawalID string) (bool, error) {
	if m.AcquireKeyFunc != nil {
		return m.AcquireKeyFunc(ctx, key, withdrawalID)
	}
	return true, nil
}

func (m *MockIdempotencyService) ReleaseKey(ctx context.Context, key string) error {
	if m.ReleaseKeyFunc != nil {
		return m.ReleaseKeyFunc(ctx, key)
	}
	return nil
}

func (m *MockIdempotencyService) CheckKey(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
	if m.CheckKeyFunc != nil {
		return m.CheckKeyFunc(ctx, key)
	}
	return nil, nil
}

func (m *MockIdempotencyService) IsProcessed(ctx context.Context, key string) (bool, *entities.Withdrawal, error) {
	if m.IsProcessedFunc != nil {
		return m.IsProcessedFunc(ctx, key)
	}
	return false, nil, nil
}

var _ domain.IdempotencyService = (*MockIdempotencyService)(nil)
