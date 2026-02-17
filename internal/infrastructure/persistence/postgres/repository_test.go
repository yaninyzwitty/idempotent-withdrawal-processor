package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/persistence/postgres/db"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	schema := `
		CREATE TABLE withdrawals (
			id VARCHAR(36) PRIMARY KEY,
			idempotency_key VARCHAR(255) NOT NULL UNIQUE,
			user_id VARCHAR(36) NOT NULL,
			asset VARCHAR(50) NOT NULL,
			amount VARCHAR(78) NOT NULL,
			destination_addr VARCHAR(255) NOT NULL,
			network VARCHAR(50) NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			error_message TEXT,
			tx_hash VARCHAR(255),
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			processed_at TIMESTAMP WITH TIME ZONE,
			processing_version BIGINT NOT NULL DEFAULT 1
		);

		CREATE INDEX idx_withdrawals_idempotency_key ON withdrawals(idempotency_key);
		CREATE INDEX idx_withdrawals_user_id ON withdrawals(user_id);
		CREATE INDEX idx_withdrawals_status ON withdrawals(status);

		CREATE TABLE idempotency_keys (
			key VARCHAR(255) PRIMARY KEY,
			withdrawal_id VARCHAR(36),
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		);

		CREATE TABLE processing_locks (
			withdrawal_id VARCHAR(36) PRIMARY KEY,
			locked_by VARCHAR(36),
			locked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		);
	`

	_, err = pool.Exec(ctx, schema)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		_ = testcontainers.TerminateContainer(pgContainer)
	}

	return pool, cleanup
}

func TestPostgresWithdrawalRepository_Create(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	t.Run("creates withdrawal successfully", func(t *testing.T) {
		w := &entities.Withdrawal{
			ID:              "w1",
			IdempotencyKey:  "k1",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}

		err := repo.Create(ctx, w)
		require.NoError(t, err)
	})

	t.Run("returns error for duplicate ID", func(t *testing.T) {
		w1 := &entities.Withdrawal{
			ID:              "w2",
			IdempotencyKey:  "k2",
			Asset:           "BTC",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}
		w2 := &entities.Withdrawal{
			ID:              "w2",
			IdempotencyKey:  "k3",
			Asset:           "BTC",
			Amount:          big.NewInt(200),
			DestinationAddr: "addr2",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}

		err := repo.Create(ctx, w1)
		require.NoError(t, err)

		err = repo.Create(ctx, w2)
		require.ErrorIs(t, err, entities.ErrWithdrawalAlreadyExist)
	})

	t.Run("returns error for duplicate idempotency key", func(t *testing.T) {
		w1 := &entities.Withdrawal{
			ID:              "w3",
			IdempotencyKey:  "k4",
			Asset:           "BTC",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}
		w2 := &entities.Withdrawal{
			ID:              "w4",
			IdempotencyKey:  "k4",
			Asset:           "BTC",
			Amount:          big.NewInt(200),
			DestinationAddr: "addr2",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}

		err := repo.Create(ctx, w1)
		require.NoError(t, err)

		err = repo.Create(ctx, w2)
		require.ErrorIs(t, err, entities.ErrWithdrawalAlreadyExist)
	})
}

func TestPostgresWithdrawalRepository_GetByID(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	w := &entities.Withdrawal{
		ID:              "w1",
		IdempotencyKey:  "k1",
		UserID:          "user-1",
		Asset:           "BTC",
		Amount:          big.NewInt(100),
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          entities.StatusPending,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	err := repo.Create(ctx, w)
	require.NoError(t, err)

	t.Run("returns withdrawal by ID", func(t *testing.T) {
		got, err := repo.GetByID(ctx, "w1")
		require.NoError(t, err)
		require.Equal(t, "w1", got.ID)
	})

	t.Run("returns error for non-existent ID", func(t *testing.T) {
		_, err := repo.GetByID(ctx, "non-existent")
		require.ErrorIs(t, err, entities.ErrWithdrawalNotFound)
	})
}

func TestPostgresWithdrawalRepository_GetByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	w := &entities.Withdrawal{
		ID:              "w1",
		IdempotencyKey:  "k1",
		UserID:          "user-1",
		Asset:           "BTC",
		Amount:          big.NewInt(100),
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          entities.StatusPending,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	err := repo.Create(ctx, w)
	require.NoError(t, err)

	t.Run("returns withdrawal by idempotency key", func(t *testing.T) {
		got, err := repo.GetByIdempotencyKey(ctx, "k1")
		require.NoError(t, err)
		require.Equal(t, "k1", got.IdempotencyKey)
	})

	t.Run("returns error for non-existent key", func(t *testing.T) {
		_, err := repo.GetByIdempotencyKey(ctx, "non-existent")
		require.ErrorIs(t, err, entities.ErrWithdrawalNotFound)
	})
}

func TestPostgresWithdrawalRepository_Update(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	w := &entities.Withdrawal{
		ID:              "w1",
		IdempotencyKey:  "k1",
		UserID:          "user-1",
		Asset:           "BTC",
		Amount:          big.NewInt(100),
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          entities.StatusPending,
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	err := repo.Create(ctx, w)
	require.NoError(t, err)

	t.Run("updates withdrawal successfully", func(t *testing.T) {
		w.Status = entities.StatusProcessing
		err := repo.Update(ctx, w)
		require.NoError(t, err)

		got, err := repo.GetByID(ctx, "w1")
		require.NoError(t, err)
		require.Equal(t, entities.StatusProcessing, got.Status)
	})

	t.Run("returns error for non-existent withdrawal", func(t *testing.T) {
		nonExistent := &entities.Withdrawal{
			ID:              "non-existent",
			IdempotencyKey:  "k2",
			Asset:           "BTC",
			Amount:          big.NewInt(100),
			DestinationAddr: "addr",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}
		err := repo.Update(ctx, nonExistent)
		require.ErrorIs(t, err, entities.ErrWithdrawalNotFound)
	})
}

func TestPostgresWithdrawalRepository_UpdateWithVersion(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	w := &entities.Withdrawal{
		ID:                "w1",
		IdempotencyKey:    "k1",
		Asset:             "BTC",
		Amount:            big.NewInt(100),
		DestinationAddr:   "addr",
		Network:           "bitcoin",
		Status:            entities.StatusPending,
		ProcessingVersion: 1,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
	err := repo.Create(ctx, w)
	require.NoError(t, err)

	t.Run("updates with correct version", func(t *testing.T) {
		w.Status = entities.StatusProcessing
		err := repo.UpdateWithVersion(ctx, w, 1)
		require.NoError(t, err)

		got, err := repo.GetByID(ctx, "w1")
		require.NoError(t, err)
		require.Equal(t, int64(2), got.ProcessingVersion)
	})

	t.Run("returns error for version conflict", func(t *testing.T) {
		got, _ := repo.GetByID(ctx, "w1")
		got.Status = entities.StatusCompleted
		err := repo.UpdateWithVersion(ctx, got, 1)
		require.Error(t, err)
	})
}

func TestPostgresWithdrawalRepository_ListByUserID(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	now := time.Now().UTC()
	err := repo.Create(ctx, &entities.Withdrawal{ID: "w1", IdempotencyKey: "k1", UserID: "user-1", Asset: "BTC", Amount: big.NewInt(100), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusPending, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = repo.Create(ctx, &entities.Withdrawal{ID: "w2", IdempotencyKey: "k2", UserID: "user-1", Asset: "BTC", Amount: big.NewInt(200), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusPending, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = repo.Create(ctx, &entities.Withdrawal{ID: "w3", IdempotencyKey: "k3", UserID: "user-2", Asset: "BTC", Amount: big.NewInt(300), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusPending, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)

	t.Run("lists withdrawals by user ID", func(t *testing.T) {
		withdrawals, err := repo.ListByUserID(ctx, "user-1", 10, 0)
		require.NoError(t, err)
		require.Len(t, withdrawals, 2)
	})

	t.Run("applies limit and offset", func(t *testing.T) {
		withdrawals, err := repo.ListByUserID(ctx, "user-1", 1, 0)
		require.NoError(t, err)
		require.Len(t, withdrawals, 1)
	})

	t.Run("returns empty for non-existent user", func(t *testing.T) {
		withdrawals, err := repo.ListByUserID(ctx, "non-existent", 10, 0)
		require.NoError(t, err)
		require.Empty(t, withdrawals)
	})
}

func TestPostgresWithdrawalRepository_ListByStatus(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	now := time.Now().UTC()
	err := repo.Create(ctx, &entities.Withdrawal{ID: "w1", IdempotencyKey: "k1", Asset: "BTC", Amount: big.NewInt(100), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusPending, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = repo.Create(ctx, &entities.Withdrawal{ID: "w2", IdempotencyKey: "k2", Asset: "BTC", Amount: big.NewInt(200), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusPending, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = repo.Create(ctx, &entities.Withdrawal{ID: "w3", IdempotencyKey: "k3", Asset: "BTC", Amount: big.NewInt(300), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusCompleted, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)

	t.Run("lists withdrawals by status", func(t *testing.T) {
		withdrawals, err := repo.ListByStatus(ctx, entities.StatusPending, 10)
		require.NoError(t, err)
		require.Len(t, withdrawals, 2)
	})

	t.Run("applies limit", func(t *testing.T) {
		withdrawals, err := repo.ListByStatus(ctx, entities.StatusPending, 1)
		require.NoError(t, err)
		require.Len(t, withdrawals, 1)
	})
}

func TestPostgresWithdrawalRepository_GetPendingWithdrawals(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresWithdrawalRepository(pool)

	now := time.Now().UTC()
	err := repo.Create(ctx, &entities.Withdrawal{ID: "w1", IdempotencyKey: "k1", Asset: "BTC", Amount: big.NewInt(100), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusPending, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = repo.Create(ctx, &entities.Withdrawal{ID: "w2", IdempotencyKey: "k2", Asset: "BTC", Amount: big.NewInt(200), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusRetrying, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = repo.Create(ctx, &entities.Withdrawal{ID: "w3", IdempotencyKey: "k3", Asset: "BTC", Amount: big.NewInt(300), DestinationAddr: "addr", Network: "bitcoin", Status: entities.StatusCompleted, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)

	t.Run("returns pending and retrying withdrawals", func(t *testing.T) {
		withdrawals, err := repo.GetPendingWithdrawals(ctx, 10)
		require.NoError(t, err)
		require.Len(t, withdrawals, 2)
	})
}

func TestPostgresIdempotencyRepository(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewPostgresIdempotencyRepository(pool, 24*time.Hour)

	t.Run("stores and retrieves key", func(t *testing.T) {
		key := entities.NewIdempotencyKey("test-key", "withdrawal-1", 24*time.Hour)
		err := repo.Store(ctx, key)
		require.NoError(t, err)

		got, err := repo.Get(ctx, "test-key")
		require.NoError(t, err)
		require.Equal(t, "test-key", got.Key)
	})

	t.Run("checks key existence", func(t *testing.T) {
		exists, err := repo.Exists(ctx, "test-key")
		require.NoError(t, err)
		require.True(t, exists)

		exists, _ = repo.Exists(ctx, "non-existent")
		require.False(t, exists)
	})

	t.Run("acquires and releases key", func(t *testing.T) {
		acquired, err := repo.Acquire(ctx, "acquire-key", "withdrawal-1", 300)
		require.NoError(t, err)
		require.True(t, acquired)

		acquired, _ = repo.Acquire(ctx, "acquire-key", "withdrawal-2", 300)
		require.False(t, acquired)

		err = repo.Release(ctx, "acquire-key")
		require.NoError(t, err)

		acquired, _ = repo.Acquire(ctx, "acquire-key", "withdrawal-2", 300)
		require.True(t, acquired)
	})

	t.Run("deletes key", func(t *testing.T) {
		key := entities.NewIdempotencyKey("delete-key", "withdrawal-1", 24*time.Hour)
		err := repo.Store(ctx, key)
		require.NoError(t, err)

		err = repo.Delete(ctx, "delete-key")
		require.NoError(t, err)

		_, err = repo.Get(ctx, "delete-key")
		require.Error(t, err)
	})
}

func TestPostgresProcessingLockRepository(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	queries := db.New(pool)
	repo := NewPostgresProcessingLockRepository(pool)

	now := time.Now().UTC()
	nowTs := pgtype.Timestamptz{Time: now, Valid: true}
	_, err := queries.CreateWithdrawal(ctx, db.CreateWithdrawalParams{
		ID:              "withdrawal-1",
		IdempotencyKey:  "k1",
		Asset:           "BTC",
		Amount:          "100",
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          "PENDING",
		CreatedAt:       nowTs,
		UpdatedAt:       nowTs,
	})
	require.NoError(t, err)
	_, err = queries.CreateWithdrawal(ctx, db.CreateWithdrawalParams{
		ID:              "withdrawal-2",
		IdempotencyKey:  "k2",
		Asset:           "BTC",
		Amount:          "100",
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          "PENDING",
		CreatedAt:       nowTs,
		UpdatedAt:       nowTs,
	})
	require.NoError(t, err)
	_, err = queries.CreateWithdrawal(ctx, db.CreateWithdrawalParams{
		ID:              "withdrawal-3",
		IdempotencyKey:  "k3",
		Asset:           "BTC",
		Amount:          "100",
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          "PENDING",
		CreatedAt:       nowTs,
		UpdatedAt:       nowTs,
	})
	require.NoError(t, err)
	_, err = queries.CreateWithdrawal(ctx, db.CreateWithdrawalParams{
		ID:              "withdrawal-4",
		IdempotencyKey:  "k4",
		Asset:           "BTC",
		Amount:          "100",
		DestinationAddr: "addr",
		Network:         "bitcoin",
		Status:          "PENDING",
		CreatedAt:       nowTs,
		UpdatedAt:       nowTs,
	})
	require.NoError(t, err)

	t.Run("acquires and releases lock", func(t *testing.T) {
		acquired, err := repo.Acquire(ctx, "withdrawal-1", "processor-1", 300)
		require.NoError(t, err)
		require.True(t, acquired)

		isLocked, _ := repo.IsLocked(ctx, "withdrawal-1")
		require.True(t, isLocked)

		err = repo.Release(ctx, "withdrawal-1", "processor-1")
		require.NoError(t, err)

		isLocked, _ = repo.IsLocked(ctx, "withdrawal-1")
		require.False(t, isLocked)
	})

	t.Run("cannot acquire already locked withdrawal", func(t *testing.T) {
		_, _ = repo.Acquire(ctx, "withdrawal-2", "processor-1", 300)

		acquired, _ := repo.Acquire(ctx, "withdrawal-2", "processor-2", 300)
		require.False(t, acquired)
	})

	t.Run("extends lock", func(t *testing.T) {
		_, _ = repo.Acquire(ctx, "withdrawal-3", "processor-1", 300)

		err := repo.Extend(ctx, "withdrawal-3", "processor-1", 60)
		require.NoError(t, err)
	})

	t.Run("cannot extend lock owned by different processor", func(t *testing.T) {
		_, _ = repo.Acquire(ctx, "withdrawal-4", "processor-1", 300)

		err := repo.Extend(ctx, "withdrawal-4", "processor-2", 60)
		require.Error(t, err)
	})
}
