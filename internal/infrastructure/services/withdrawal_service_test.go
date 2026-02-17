package services

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories/mocks"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	"go.uber.org/zap"
)

func TestWithdrawalService_CreateWithdrawal(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("creates new withdrawal successfully", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			CreateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				return nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return nil, errors.New("not found")
			},
			AcquireFunc: func(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
				return true, nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		req := &domain.CreateWithdrawalRequest{
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          "100000000",
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
			MaxRetries:      3,
		}

		withdrawal, alreadyExists, err := svc.CreateWithdrawal(ctx, req)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if alreadyExists {
			t.Error("should not indicate already exists")
		}
		if withdrawal == nil {
			t.Fatal("withdrawal should not be nil")
		}
		if withdrawal.IdempotencyKey != "test-key" {
			t.Errorf("IdempotencyKey = %v, want 'test-key'", withdrawal.IdempotencyKey)
		}
		if withdrawal.Status != entities.StatusPending {
			t.Errorf("Status = %v, want %v", withdrawal.Status, entities.StatusPending)
		}
	})

	t.Run("returns existing withdrawal for duplicate idempotency key", func(t *testing.T) {
		existingWithdrawal := &entities.Withdrawal{
			ID:              "existing-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusCompleted,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIdempotencyKeyFunc: func(ctx context.Context, key string) (*entities.Withdrawal, error) {
				return existingWithdrawal, nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return &entities.IdempotencyKey{
					Key:          key,
					WithdrawalID: "existing-id",
				}, nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		req := &domain.CreateWithdrawalRequest{
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          "100000000",
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
		}

		withdrawal, alreadyExists, err := svc.CreateWithdrawal(ctx, req)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !alreadyExists {
			t.Error("should indicate already exists")
		}
		if withdrawal.ID != "existing-id" {
			t.Errorf("ID = %v, want 'existing-id'", withdrawal.ID)
		}
	})

	t.Run("returns error for invalid amount", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{}
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return nil, errors.New("not found")
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		req := &domain.CreateWithdrawalRequest{
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          "invalid",
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
		}

		_, _, err := svc.CreateWithdrawal(ctx, req)

		if err != entities.ErrInvalidAmount {
			t.Errorf("error = %v, want %v", err, entities.ErrInvalidAmount)
		}
	})

	t.Run("returns error when acquire fails and cannot get existing", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIdempotencyKeyFunc: func(ctx context.Context, key string) (*entities.Withdrawal, error) {
				return nil, errors.New("not found")
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return nil, errors.New("not found")
			},
			AcquireFunc: func(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
				return false, nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		req := &domain.CreateWithdrawalRequest{
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          "100000000",
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
		}

		_, _, err := svc.CreateWithdrawal(ctx, req)

		if err == nil {
			t.Error("expected error when acquire fails and cannot get existing")
		}
	})

	t.Run("returns existing when acquire fails and existing found", func(t *testing.T) {
		existingWithdrawal := &entities.Withdrawal{
			ID:              "existing-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIdempotencyKeyFunc: func(ctx context.Context, key string) (*entities.Withdrawal, error) {
				return existingWithdrawal, nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return nil, errors.New("not found")
			},
			AcquireFunc: func(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
				return false, nil
			},
		}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		req := &domain.CreateWithdrawalRequest{
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          "100000000",
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
		}

		withdrawal, alreadyExists, err := svc.CreateWithdrawal(ctx, req)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !alreadyExists {
			t.Error("should indicate already exists")
		}
		if withdrawal.ID != "existing-id" {
			t.Errorf("ID = %v, want 'existing-id'", withdrawal.ID)
		}
	})
}

func TestWithdrawalService_GetWithdrawal(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("returns withdrawal successfully", func(t *testing.T) {
		expectedWithdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIDFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				if id == "test-id" {
					return expectedWithdrawal, nil
				}
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		withdrawal, err := svc.GetWithdrawal(ctx, "test-id")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if withdrawal.ID != "test-id" {
			t.Errorf("ID = %v, want 'test-id'", withdrawal.ID)
		}
	})

	t.Run("returns error when withdrawal not found", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIDFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		_, err := svc.GetWithdrawal(ctx, "non-existent")

		if err != entities.ErrWithdrawalNotFound {
			t.Errorf("error = %v, want %v", err, entities.ErrWithdrawalNotFound)
		}
	})
}

func TestWithdrawalService_GetWithdrawalByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("returns withdrawal by idempotency key", func(t *testing.T) {
		expectedWithdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusPending,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIdempotencyKeyFunc: func(ctx context.Context, key string) (*entities.Withdrawal, error) {
				if key == "test-key" {
					return expectedWithdrawal, nil
				}
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		withdrawal, err := svc.GetWithdrawalByIdempotencyKey(ctx, "test-key")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if withdrawal.IdempotencyKey != "test-key" {
			t.Errorf("IdempotencyKey = %v, want 'test-key'", withdrawal.IdempotencyKey)
		}
	})
}

func TestWithdrawalService_ListWithdrawals(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("lists withdrawals by user ID", func(t *testing.T) {
		expectedWithdrawals := []*entities.Withdrawal{
			{ID: "w1", UserID: "user-1", Amount: big.NewInt(100), IdempotencyKey: "k1", DestinationAddr: "addr"},
			{ID: "w2", UserID: "user-1", Amount: big.NewInt(200), IdempotencyKey: "k2", DestinationAddr: "addr"},
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			ListByUserIDFunc: func(ctx context.Context, userID string, limit, offset int) ([]*entities.Withdrawal, error) {
				return expectedWithdrawals, nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		withdrawals, err := svc.ListWithdrawals(ctx, "user-1", entities.StatusUnspecified, 10, 0)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(withdrawals) != 2 {
			t.Errorf("len(withdrawals) = %v, want 2", len(withdrawals))
		}
	})

	t.Run("lists withdrawals by status", func(t *testing.T) {
		expectedWithdrawals := []*entities.Withdrawal{
			{ID: "w1", UserID: "user-1", Status: entities.StatusPending, Amount: big.NewInt(100), IdempotencyKey: "k1", DestinationAddr: "addr"},
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			ListByStatusFunc: func(ctx context.Context, status entities.WithdrawalStatus, limit int) ([]*entities.Withdrawal, error) {
				if status == entities.StatusPending {
					return expectedWithdrawals, nil
				}
				return []*entities.Withdrawal{}, nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		withdrawals, err := svc.ListWithdrawals(ctx, "", entities.StatusPending, 10, 0)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(withdrawals) != 1 {
			t.Errorf("len(withdrawals) = %v, want 1", len(withdrawals))
		}
	})

	t.Run("lists pending withdrawals when no filter", func(t *testing.T) {
		expectedWithdrawals := []*entities.Withdrawal{
			{ID: "w1", Status: entities.StatusPending, Amount: big.NewInt(100), IdempotencyKey: "k1", DestinationAddr: "addr"},
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetPendingWithdrawalsFunc: func(ctx context.Context, limit int) ([]*entities.Withdrawal, error) {
				return expectedWithdrawals, nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		withdrawals, err := svc.ListWithdrawals(ctx, "", entities.StatusUnspecified, 10, 0)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(withdrawals) != 1 {
			t.Errorf("len(withdrawals) = %v, want 1", len(withdrawals))
		}
	})
}

func TestWithdrawalService_RetryWithdrawal(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("prepares withdrawal for retry successfully", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusFailed,
			RetryCount:      1,
			MaxRetries:      3,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIDFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return withdrawal, nil
			},
			UpdateFunc: func(ctx context.Context, w *entities.Withdrawal) error {
				return nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		updated, err := svc.RetryWithdrawal(ctx, "test-id")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if updated.Status != entities.StatusRetrying {
			t.Errorf("Status = %v, want %v", updated.Status, entities.StatusRetrying)
		}
	})

	t.Run("returns error when max retries exceeded", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-id",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Status:          entities.StatusFailed,
			RetryCount:      3,
			MaxRetries:      3,
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		}

		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIDFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return withdrawal, nil
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		_, err := svc.RetryWithdrawal(ctx, "test-id")

		if err != entities.ErrMaxRetriesExceeded {
			t.Errorf("error = %v, want %v", err, entities.ErrMaxRetriesExceeded)
		}
	})

	t.Run("returns error when withdrawal not found", func(t *testing.T) {
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIDFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		idempotencyRepo := &mocks.MockIdempotencyRepository{}
		lockRepo := &mocks.MockProcessingLockRepository{}

		svc := NewWithdrawalService(withdrawalRepo, idempotencyRepo, lockRepo, WithWithdrawalServiceLogger(logger))

		_, err := svc.RetryWithdrawal(ctx, "non-existent")

		if err != entities.ErrWithdrawalNotFound {
			t.Errorf("error = %v, want %v", err, entities.ErrWithdrawalNotFound)
		}
	})
}

func TestIdempotencyService(t *testing.T) {
	ctx := context.Background()

	t.Run("acquires key successfully", func(t *testing.T) {
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			AcquireFunc: func(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
				return true, nil
			},
		}
		withdrawalRepo := &mocks.MockWithdrawalRepository{}

		svc := NewIdempotencyService(idempotencyRepo, withdrawalRepo, 24*60*60*time.Second)

		acquired, err := svc.AcquireKey(ctx, "test-key", "withdrawal-1")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !acquired {
			t.Error("should acquire key")
		}
	})

	t.Run("releases key successfully", func(t *testing.T) {
		idempotencyRepo := &mocks.MockIdempotencyRepository{
			ReleaseFunc: func(ctx context.Context, key string) error {
				return nil
			},
		}
		withdrawalRepo := &mocks.MockWithdrawalRepository{}

		svc := NewIdempotencyService(idempotencyRepo, withdrawalRepo, 24*60*60*time.Second)

		err := svc.ReleaseKey(ctx, "test-key")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("checks key successfully", func(t *testing.T) {
		expectedKey := &entities.IdempotencyKey{
			Key:          "test-key",
			WithdrawalID: "withdrawal-1",
		}

		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return expectedKey, nil
			},
		}
		withdrawalRepo := &mocks.MockWithdrawalRepository{}

		svc := NewIdempotencyService(idempotencyRepo, withdrawalRepo, 24*60*60*time.Second)

		key, err := svc.CheckKey(ctx, "test-key")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if key.Key != "test-key" {
			t.Errorf("Key = %v, want 'test-key'", key.Key)
		}
	})

	t.Run("is processed returns true for valid key", func(t *testing.T) {
		expectedKey := &entities.IdempotencyKey{
			Key:          "test-key",
			WithdrawalID: "withdrawal-1",
		}
		expectedWithdrawal := &entities.Withdrawal{
			ID:              "withdrawal-1",
			IdempotencyKey:  "test-key",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
		}

		idempotencyRepo := &mocks.MockIdempotencyRepository{
			GetFunc: func(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
				return expectedKey, nil
			},
		}
		withdrawalRepo := &mocks.MockWithdrawalRepository{
			GetByIDFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return expectedWithdrawal, nil
			},
		}

		svc := NewIdempotencyService(idempotencyRepo, withdrawalRepo, 24*60*60*time.Second)

		processed, withdrawal, err := svc.IsProcessed(ctx, "test-key")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !processed {
			t.Error("should be processed")
		}
		if withdrawal == nil {
			t.Error("withdrawal should not be nil")
		}
	})
}
