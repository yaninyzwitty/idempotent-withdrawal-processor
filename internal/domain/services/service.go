package services

import (
	"context"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
)

type WithdrawalProcessor interface {
	Process(ctx context.Context, withdrawal *entities.Withdrawal) error
	ProcessBatch(ctx context.Context, withdrawals []*entities.Withdrawal) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type WithdrawalService interface {
	CreateWithdrawal(ctx context.Context, req *CreateWithdrawalRequest) (*entities.Withdrawal, bool, error)
	GetWithdrawal(ctx context.Context, id string) (*entities.Withdrawal, error)
	GetWithdrawalByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error)
	ListWithdrawals(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error)
	RetryWithdrawal(ctx context.Context, id string) (*entities.Withdrawal, error)
}

type CreateWithdrawalRequest struct {
	IdempotencyKey  string
	UserID          string
	Asset           string
	Amount          string
	DestinationAddr string
	Network         string
	MaxRetries      int
}

type IdempotencyService interface {
	AcquireKey(ctx context.Context, key string, withdrawalID string) (bool, error)
	ReleaseKey(ctx context.Context, key string) error
	CheckKey(ctx context.Context, key string) (*entities.IdempotencyKey, error)
	IsProcessed(ctx context.Context, key string) (bool, *entities.Withdrawal, error)
}

type BlockchainService interface {
	BroadcastTransaction(ctx context.Context, withdrawal *entities.Withdrawal) (string, error)
	GetTransactionStatus(ctx context.Context, txHash string) (string, error)
	ValidateAddress(ctx context.Context, address string, network string) (bool, error)
	EstimateFee(ctx context.Context, asset string, network string) (string, error)
}

type RetryService interface {
	ShouldRetry(ctx context.Context, withdrawal *entities.Withdrawal, err error) bool
	GetNextRetryDelay(ctx context.Context, retryCount int) int64
	ScheduleRetry(ctx context.Context, withdrawalID string, delaySeconds int64) error
}
