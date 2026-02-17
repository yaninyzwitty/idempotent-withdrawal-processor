package services

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories/mocks"
	svcmocks "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services/mocks"
)

func TestWithdrawalProcessor_Process(t *testing.T) {
	ctx := context.Background()

	t.Run("processes withdrawal successfully", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				return nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return true, nil
			},
			ReleaseFunc: func(ctx context.Context, withdrawalID string, processorID string) error {
				return nil
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{
			BroadcastTransactionFunc: func(ctx context.Context, w *entities.Withdrawal) (string, error) {
				return "0xmocktxhash", nil
			},
		}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
			WithLockTTL(300),
		)

		err := processor.Process(ctx, withdrawal)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if withdrawal.Status != entities.StatusCompleted {
			t.Errorf("Status = %v, want %v", withdrawal.Status, entities.StatusCompleted)
		}
		if withdrawal.TxHash != "0xmocktxhash" {
			t.Errorf("TxHash = %v, want '0xmocktxhash'", withdrawal.TxHash)
		}
	})

	t.Run("does not process when lock cannot be acquired", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return false, nil
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
		)

		err := processor.Process(ctx, withdrawal)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if withdrawal.Status != entities.StatusPending {
			t.Errorf("Status should remain pending when lock not acquired")
		}
	})

	t.Run("returns error when lock acquisition fails", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return false, errors.New("lock error")
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
		)

		err := processor.Process(ctx, withdrawal)

		if err == nil {
			t.Error("expected error when lock acquisition fails")
		}
	})

	t.Run("schedules retry when broadcast fails and can retry", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			RetryCount:      0,
			MaxRetries:      3,
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				return nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return true, nil
			},
			ReleaseFunc: func(ctx context.Context, withdrawalID string, processorID string) error {
				return nil
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{
			BroadcastTransactionFunc: func(ctx context.Context, w *entities.Withdrawal) (string, error) {
				return "", errors.New("broadcast failed")
			},
		}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
		)

		err := processor.Process(ctx, withdrawal)

		if err == nil {
			t.Error("expected error when broadcast fails")
		}
		if withdrawal.RetryCount != 1 {
			t.Errorf("RetryCount = %v, want 1", withdrawal.RetryCount)
		}
		if withdrawal.Status != entities.StatusRetrying {
			t.Errorf("Status = %v, want %v", withdrawal.Status, entities.StatusRetrying)
		}
	})

	t.Run("marks failed when max retries exceeded", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			RetryCount:      3,
			MaxRetries:      3,
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				return nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return true, nil
			},
			ReleaseFunc: func(ctx context.Context, withdrawalID string, processorID string) error {
				return nil
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{
			BroadcastTransactionFunc: func(ctx context.Context, w *entities.Withdrawal) (string, error) {
				return "", errors.New("broadcast failed")
			},
		}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
		)

		err := processor.Process(ctx, withdrawal)

		if err == nil {
			t.Error("expected error when broadcast fails")
		}
		if withdrawal.Status != entities.StatusFailed {
			t.Errorf("Status = %v, want %v", withdrawal.Status, entities.StatusFailed)
		}
		if withdrawal.ErrorMessage == "" {
			t.Error("ErrorMessage should not be empty")
		}
	})
}

func TestWithdrawalProcessor_ProcessBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("processes batch successfully", func(t *testing.T) {
		withdrawals := []*entities.Withdrawal{
			{ID: "w1", IdempotencyKey: "k1", Status: entities.StatusPending, Amount: big.NewInt(100), DestinationAddr: "addr1"},
			{ID: "w2", IdempotencyKey: "k2", Status: entities.StatusPending, Amount: big.NewInt(200), DestinationAddr: "addr2"},
		}

		var mu sync.Mutex
		processedCount := 0

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				mu.Lock()
				processedCount++
				mu.Unlock()
				return nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return true, nil
			},
			ReleaseFunc: func(ctx context.Context, withdrawalID string, processorID string) error {
				return nil
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{
			BroadcastTransactionFunc: func(ctx context.Context, w *entities.Withdrawal) (string, error) {
				return "0xmocktxhash", nil
			},
		}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
			WithMaxConcurrent(10),
		)

		err := processor.ProcessBatch(ctx, withdrawals)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		mu.Lock()
		count := processedCount
		mu.Unlock()

		if count != 2 {
			t.Errorf("processedCount = %v, want 2", count)
		}
	})

	t.Run("handles partial failures in batch", func(t *testing.T) {
		withdrawals := []*entities.Withdrawal{
			{ID: "w1", IdempotencyKey: "k1", Status: entities.StatusPending, Amount: big.NewInt(100), DestinationAddr: "addr1"},
			{ID: "w2", IdempotencyKey: "k2", Status: entities.StatusPending, Amount: big.NewInt(200), DestinationAddr: "addr2"},
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				return nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{
			AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
				return true, nil
			},
			ReleaseFunc: func(ctx context.Context, withdrawalID string, processorID string) error {
				return nil
			},
		}
		blockchainSvc := &svcmocks.MockBlockchainService{
			BroadcastTransactionFunc: func(ctx context.Context, w *entities.Withdrawal) (string, error) {
				if w.ID == "w2" {
					return "", errors.New("broadcast failed")
				}
				return "0xmocktxhash", nil
			},
		}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
		)

		err := processor.ProcessBatch(ctx, withdrawals)

		if err == nil {
			t.Error("expected error for partial failures")
		}
	})
}

func TestWithdrawalProcessor_StartStop(t *testing.T) {
	t.Run("starts and stops processor", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetPendingWithdrawalsFunc: func(ctx context.Context, limit int) ([]*entities.Withdrawal, error) {
				return []*entities.Withdrawal{}, nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
			WithPollInterval(10*time.Millisecond),
		)

		ctx := context.Background()

		err := processor.Start(ctx)
		if err != nil {
			t.Errorf("unexpected error starting processor: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		err = processor.Stop(ctx)
		if err != nil {
			t.Errorf("unexpected error stopping processor: %v", err)
		}
	})

	t.Run("returns error when starting already running processor", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetPendingWithdrawalsFunc: func(ctx context.Context, limit int) ([]*entities.Withdrawal, error) {
				return []*entities.Withdrawal{}, nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
			WithPollInterval(100*time.Millisecond),
		)

		ctx := context.Background()

		_ = processor.Start(ctx)
		defer processor.Stop(ctx)

		err := processor.Start(ctx)
		if err == nil {
			t.Error("expected error when starting already running processor")
		}
	})

	t.Run("returns error when stopping non-running processor", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("test-processor"),
		)

		ctx := context.Background()

		err := processor.Stop(ctx)
		if err == nil {
			t.Error("expected error when stopping non-running processor")
		}
	})
}

func TestWithdrawalProcessor_GetStats(t *testing.T) {
	ctx := context.Background()

	withdrawal := &entities.Withdrawal{
		ID:              "test-id",
		IdempotencyKey:  "test-key",
		Status:          entities.StatusPending,
		Amount:          big.NewInt(100000000),
		DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
	}

	withdrawalRepo := &mocks.MockWithdrawalRepository{
		UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
			return nil
		},
	}
	lockRepo := &mocks.MockProcessingLockRepository{
		AcquireFunc: func(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
			return true, nil
		},
		ReleaseFunc: func(ctx context.Context, withdrawalID string, processorID string) error {
			return nil
		},
	}
	blockchainSvc := &svcmocks.MockBlockchainService{
		BroadcastTransactionFunc: func(ctx context.Context, w *entities.Withdrawal) (string, error) {
			return "0xmocktxhash", nil
		},
	}

	processor := NewWithdrawalProcessor(
		withdrawalRepo,
		lockRepo,
		blockchainSvc,
		WithProcessorID("test-processor"),
	)

	_ = processor.Process(ctx, withdrawal)

	stats := processor.(*withdrawalProcessor).GetStats()

	if stats.TotalProcessed != 1 {
		t.Errorf("TotalProcessed = %v, want 1", stats.TotalProcessed)
	}
	if stats.TotalSucceeded != 1 {
		t.Errorf("TotalSucceeded = %v, want 1", stats.TotalSucceeded)
	}
}

func TestProcessorConfig(t *testing.T) {
	t.Run("applies config options", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(
			withdrawalRepo,
			lockRepo,
			blockchainSvc,
			WithProcessorID("custom-processor"),
			WithBatchSize(50),
			WithPollInterval(200*time.Millisecond),
			WithLockTTL(600),
			WithMaxConcurrent(20),
			WithRetryBaseDelay(2*time.Second),
			WithRetryMaxDelay(60*time.Second),
		)

		p := processor.(*withdrawalProcessor)
		if p.config.ProcessorID != "custom-processor" {
			t.Errorf("ProcessorID = %v, want 'custom-processor'", p.config.ProcessorID)
		}
		if p.config.BatchSize != 50 {
			t.Errorf("BatchSize = %v, want 50", p.config.BatchSize)
		}
		if p.config.PollInterval != 200*time.Millisecond {
			t.Errorf("PollInterval = %v, want 200ms", p.config.PollInterval)
		}
		if p.config.LockTTL != 600 {
			t.Errorf("LockTTL = %v, want 600", p.config.LockTTL)
		}
		if p.config.MaxConcurrent != 20 {
			t.Errorf("MaxConcurrent = %v, want 20", p.config.MaxConcurrent)
		}
	})

	t.Run("uses default config when no options provided", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}
		blockchainSvc := &svcmocks.MockBlockchainService{}

		processor := NewWithdrawalProcessor(withdrawalRepo, lockRepo, blockchainSvc)
		p := processor.(*withdrawalProcessor)

		defaultConfig := DefaultProcessorConfig()
		if p.config.ProcessorID != defaultConfig.ProcessorID {
			t.Errorf("ProcessorID = %v, want %v", p.config.ProcessorID, defaultConfig.ProcessorID)
		}
		if p.config.BatchSize != defaultConfig.BatchSize {
			t.Errorf("BatchSize = %v, want %v", p.config.BatchSize, defaultConfig.BatchSize)
		}
	})
}

func TestRetryService(t *testing.T) {
	ctx := context.Background()

	t.Run("should retry when under max retries", func(t *testing.T) {
		svc := NewRetryService()
		withdrawal := &entities.Withdrawal{
			RetryCount: 1,
			MaxRetries: 3,
			Status:     entities.StatusFailed,
			Amount:     big.NewInt(100),
		}

		if !svc.ShouldRetry(ctx, withdrawal, errors.New("test error")) {
			t.Error("should retry when under max retries")
		}
	})

	t.Run("should not retry when max retries reached", func(t *testing.T) {
		svc := NewRetryService()
		withdrawal := &entities.Withdrawal{
			RetryCount: 3,
			MaxRetries: 3,
			Status:     entities.StatusFailed,
			Amount:     big.NewInt(100),
		}

		if svc.ShouldRetry(ctx, withdrawal, errors.New("test error")) {
			t.Error("should not retry when max retries reached")
		}
	})

	t.Run("calculates exponential backoff delay", func(t *testing.T) {
		svc := NewRetryService(
			WithRetryBaseDelaySeconds(1),
			WithRetryMaxDelaySeconds(30),
			WithRetryMultiplier(2.0),
		)

		tests := []struct {
			retryCount int
			expected   int64
		}{
			{0, 1},
			{1, 2},
			{2, 4},
			{3, 8},
			{4, 16},
			{5, 30},
		}

		for _, tt := range tests {
			delay := svc.GetNextRetryDelay(ctx, tt.retryCount)
			if delay != tt.expected {
				t.Errorf("GetNextRetryDelay(%d) = %v, want %v", tt.retryCount, delay, tt.expected)
			}
		}
	})
}

func TestMockBlockchainService(t *testing.T) {
	ctx := context.Background()

	t.Run("broadcasts transaction successfully", func(t *testing.T) {
		svc := NewMockBlockchainService()
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
		}

		txHash, err := svc.BroadcastTransaction(ctx, withdrawal)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if txHash == "" {
			t.Error("txHash should not be empty")
		}
	})

	t.Run("validates addresses", func(t *testing.T) {
		svc := NewMockBlockchainService()

		valid, err := svc.ValidateAddress(ctx, "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh", "bitcoin")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("address should be valid")
		}

		valid, err = svc.ValidateAddress(ctx, "short", "bitcoin")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if valid {
			t.Error("short address should be invalid")
		}
	})

	t.Run("returns transaction status", func(t *testing.T) {
		svc := NewMockBlockchainService()

		status, err := svc.GetTransactionStatus(ctx, "0x123")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if status != "confirmed" {
			t.Errorf("status = %v, want 'confirmed'", status)
		}
	})

	t.Run("estimates fee", func(t *testing.T) {
		svc := NewMockBlockchainService()

		fee, err := svc.EstimateFee(ctx, "BTC", "bitcoin")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if fee == "" {
			t.Error("fee should not be empty")
		}
	})

	t.Run("simulates failures with fail mode", func(t *testing.T) {
		svc := NewMockBlockchainService(
			WithFailMode(true, 2),
		).(*MockBlockchainService)

		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
		}

		_, err := svc.BroadcastTransaction(ctx, withdrawal)
		if err == nil {
			t.Error("expected error on first failure")
		}

		_, err = svc.BroadcastTransaction(ctx, withdrawal)
		if err == nil {
			t.Error("expected error on second failure")
		}

		_, err = svc.BroadcastTransaction(ctx, withdrawal)
		if err != nil {
			t.Errorf("unexpected error after fail count reached: %v", err)
		}
	})
}
