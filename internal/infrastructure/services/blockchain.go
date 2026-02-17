package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	"go.uber.org/zap"
)

type MockBlockchainService struct {
	logger       *zap.Logger
	failMode     bool
	failCount    int
	currentFails int
}

type MockBlockchainServiceOption func(*MockBlockchainService)

func WithFailMode(fail bool, count int) MockBlockchainServiceOption {
	return func(s *MockBlockchainService) {
		s.failMode = fail
		s.failCount = count
	}
}

func WithBlockchainLogger(logger *zap.Logger) MockBlockchainServiceOption {
	return func(s *MockBlockchainService) {
		s.logger = logger
	}
}

func NewMockBlockchainService(opts ...MockBlockchainServiceOption) domain.BlockchainService {
	s := &MockBlockchainService{
		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *MockBlockchainService) BroadcastTransaction(ctx context.Context, withdrawal *entities.Withdrawal) (string, error) {
	if s.failMode {
		if s.currentFails < s.failCount {
			s.currentFails++
			s.logger.Debug("mock blockchain failing transaction",
				zap.String("withdrawal_id", withdrawal.ID),
				zap.Int("fail_count", s.currentFails),
			)
			return "", fmt.Errorf("mock blockchain error: simulated failure")
		}
	}

	txHash := s.generateTxHash()

	s.logger.Info("mock blockchain broadcasted transaction",
		zap.String("withdrawal_id", withdrawal.ID),
		zap.String("tx_hash", txHash),
	)

	return txHash, nil
}

func (s *MockBlockchainService) GetTransactionStatus(ctx context.Context, txHash string) (string, error) {
	return "confirmed", nil
}

func (s *MockBlockchainService) ValidateAddress(ctx context.Context, address string, network string) (bool, error) {
	if len(address) < 26 || len(address) > 62 {
		return false, nil
	}
	return true, nil
}

func (s *MockBlockchainService) EstimateFee(ctx context.Context, asset string, network string) (string, error) {
	return "1000000000000000", nil
}

func (s *MockBlockchainService) generateTxHash() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return "0x" + hex.EncodeToString(b)
}

var _ domain.BlockchainService = (*MockBlockchainService)(nil)

type retryService struct {
	baseDelay  int64
	maxDelay   int64
	multiplier float64
}

type RetryServiceOption func(*retryService)

func WithRetryBaseDelaySeconds(delay int64) RetryServiceOption {
	return func(r *retryService) {
		r.baseDelay = delay
	}
}

func WithRetryMaxDelaySeconds(delay int64) RetryServiceOption {
	return func(r *retryService) {
		r.maxDelay = delay
	}
}

func WithRetryMultiplier(mult float64) RetryServiceOption {
	return func(r *retryService) {
		r.multiplier = mult
	}
}

func NewRetryService(opts ...RetryServiceOption) domain.RetryService {
	r := &retryService{
		baseDelay:  1,
		maxDelay:   30,
		multiplier: 2.0,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *retryService) ShouldRetry(ctx context.Context, withdrawal *entities.Withdrawal, err error) bool {
	if withdrawal.RetryCount >= withdrawal.MaxRetries {
		return false
	}

	return true
}

func (r *retryService) GetNextRetryDelay(ctx context.Context, retryCount int) int64 {
	delay := float64(r.baseDelay)
	for i := 0; i < retryCount; i++ {
		delay *= r.multiplier
	}

	if delay > float64(r.maxDelay) {
		return r.maxDelay
	}

	return int64(delay)
}

func (r *retryService) ScheduleRetry(ctx context.Context, withdrawalID string, delaySeconds int64) error {
	return nil
}

var _ domain.RetryService = (*retryService)(nil)
