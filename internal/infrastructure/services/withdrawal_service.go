package services

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/persistence"
	"go.uber.org/zap"
)

type withdrawalService struct {
	withdrawalRepo  repositories.WithdrawalRepository
	idempotencyRepo repositories.IdempotencyRepository
	lockRepo        repositories.ProcessingLockRepository
	logger          *zap.Logger
}

type WithdrawalServiceOption func(*withdrawalService)

func WithWithdrawalServiceLogger(logger *zap.Logger) WithdrawalServiceOption {
	return func(s *withdrawalService) {
		s.logger = logger
	}
}

func NewWithdrawalService(
	withdrawalRepo repositories.WithdrawalRepository,
	idempotencyRepo repositories.IdempotencyRepository,
	lockRepo repositories.ProcessingLockRepository,
	opts ...WithdrawalServiceOption,
) domain.WithdrawalService {
	s := &withdrawalService{
		withdrawalRepo:  withdrawalRepo,
		idempotencyRepo: idempotencyRepo,
		lockRepo:        lockRepo,
		logger:          zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *withdrawalService) CreateWithdrawal(ctx context.Context, req *domain.CreateWithdrawalRequest) (*entities.Withdrawal, bool, error) {
	existing, err := s.idempotencyRepo.Get(ctx, req.IdempotencyKey)
	if err == nil && existing != nil {
		s.logger.Info("idempotency key found, returning existing withdrawal",
			zap.String("idempotency_key", req.IdempotencyKey),
			zap.String("withdrawal_id", existing.WithdrawalID),
		)

		withdrawal, err := s.withdrawalRepo.GetByIdempotencyKey(ctx, req.IdempotencyKey)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get existing withdrawal: %w", err)
		}
		return withdrawal, true, nil
	}

	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok {
		return nil, false, entities.ErrInvalidAmount
	}

	id := persistence.GenerateID()

	withdrawal, err := entities.NewWithdrawal(
		id,
		req.IdempotencyKey,
		req.UserID,
		req.Asset,
		amount,
		req.DestinationAddr,
		req.Network,
		entities.WithMaxRetries(req.MaxRetries),
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create withdrawal entity: %w", err)
	}

	acquired, err := s.idempotencyRepo.Acquire(ctx, req.IdempotencyKey, id, 86400)
	if err != nil {
		return nil, false, fmt.Errorf("failed to acquire idempotency key: %w", err)
	}
	if !acquired {
		withdrawal, err := s.withdrawalRepo.GetByIdempotencyKey(ctx, req.IdempotencyKey)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get existing withdrawal after acquire failed: %w", err)
		}
		return withdrawal, true, nil
	}

	if err := s.withdrawalRepo.Create(ctx, withdrawal); err != nil {
		_ = s.idempotencyRepo.Release(ctx, req.IdempotencyKey)
		return nil, false, fmt.Errorf("failed to create withdrawal: %w", err)
	}

	s.logger.Info("created new withdrawal",
		zap.String("id", withdrawal.ID),
		zap.String("idempotency_key", req.IdempotencyKey),
	)

	return withdrawal, false, nil
}

func (s *withdrawalService) GetWithdrawal(ctx context.Context, id string) (*entities.Withdrawal, error) {
	return s.withdrawalRepo.GetByID(ctx, id)
}

func (s *withdrawalService) GetWithdrawalByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error) {
	return s.withdrawalRepo.GetByIdempotencyKey(ctx, key)
}

func (s *withdrawalService) ListWithdrawals(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error) {
	if userID != "" {
		return s.withdrawalRepo.ListByUserID(ctx, userID, limit, offset)
	}

	if status != entities.StatusUnspecified {
		return s.withdrawalRepo.ListByStatus(ctx, status, limit)
	}

	return s.withdrawalRepo.GetPendingWithdrawals(ctx, limit)
}

func (s *withdrawalService) RetryWithdrawal(ctx context.Context, id string) (*entities.Withdrawal, error) {
	withdrawal, err := s.withdrawalRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if !withdrawal.CanRetry() {
		return nil, entities.ErrMaxRetriesExceeded
	}

	if err := withdrawal.PrepareForRetry(); err != nil {
		return nil, fmt.Errorf("failed to prepare withdrawal for retry: %w", err)
	}

	if err := s.withdrawalRepo.Update(ctx, withdrawal); err != nil {
		return nil, fmt.Errorf("failed to update withdrawal: %w", err)
	}

	s.logger.Info("withdrawal prepared for retry",
		zap.String("id", withdrawal.ID),
		zap.Int("retry_count", withdrawal.RetryCount),
	)

	return withdrawal, nil
}

var _ domain.WithdrawalService = (*withdrawalService)(nil)

type idempotencyService struct {
	repo           repositories.IdempotencyRepository
	withdrawalRepo repositories.WithdrawalRepository
	ttl            time.Duration
}

func NewIdempotencyService(
	repo repositories.IdempotencyRepository,
	withdrawalRepo repositories.WithdrawalRepository,
	ttl time.Duration,
) domain.IdempotencyService {
	return &idempotencyService{
		repo:           repo,
		withdrawalRepo: withdrawalRepo,
		ttl:            ttl,
	}
}

func (s *idempotencyService) AcquireKey(ctx context.Context, key string, withdrawalID string) (bool, error) {
	return s.repo.Acquire(ctx, key, withdrawalID, int64(s.ttl.Seconds()))
}

func (s *idempotencyService) ReleaseKey(ctx context.Context, key string) error {
	return s.repo.Release(ctx, key)
}

func (s *idempotencyService) CheckKey(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
	return s.repo.Get(ctx, key)
}

func (s *idempotencyService) IsProcessed(ctx context.Context, key string) (bool, *entities.Withdrawal, error) {
	ik, err := s.repo.Get(ctx, key)
	if err != nil {
		return false, nil, nil
	}

	if ik.IsExpired() {
		return false, nil, nil
	}

	withdrawal, err := s.withdrawalRepo.GetByID(ctx, ik.WithdrawalID)
	if err != nil {
		return false, nil, nil
	}

	return true, withdrawal, nil
}

var _ domain.IdempotencyService = (*idempotencyService)(nil)
