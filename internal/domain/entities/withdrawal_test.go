package entities

import (
	"math/big"
	"testing"
	"time"
)

func TestWithdrawalStatus_String(t *testing.T) {
	tests := []struct {
		status   WithdrawalStatus
		expected string
	}{
		{StatusUnspecified, "UNSPECIFIED"},
		{StatusPending, "PENDING"},
		{StatusProcessing, "PROCESSING"},
		{StatusCompleted, "COMPLETED"},
		{StatusFailed, "FAILED"},
		{StatusRetrying, "RETRYING"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.status.String(); got != tt.expected {
				t.Errorf("WithdrawalStatus.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestWithdrawalStatus_CanTransitionTo(t *testing.T) {
	tests := []struct {
		name       string
		from       WithdrawalStatus
		to         WithdrawalStatus
		canTransit bool
	}{
		{"pending to processing", StatusPending, StatusProcessing, true},
		{"pending to failed", StatusPending, StatusFailed, true},
		{"pending to completed", StatusPending, StatusCompleted, false},
		{"processing to completed", StatusProcessing, StatusCompleted, true},
		{"processing to failed", StatusProcessing, StatusFailed, true},
		{"processing to retrying", StatusProcessing, StatusRetrying, true},
		{"processing to pending", StatusProcessing, StatusPending, false},
		{"completed to pending", StatusCompleted, StatusPending, false},
		{"completed to failed", StatusCompleted, StatusFailed, false},
		{"failed to retrying", StatusFailed, StatusRetrying, true},
		{"failed to pending", StatusFailed, StatusPending, false},
		{"retrying to processing", StatusRetrying, StatusProcessing, true},
		{"retrying to failed", StatusRetrying, StatusFailed, true},
		{"unspecified to pending", StatusUnspecified, StatusPending, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.from.CanTransitionTo(tt.to); got != tt.canTransit {
				t.Errorf("CanTransitionTo(%v -> %v) = %v, want %v", tt.from, tt.to, got, tt.canTransit)
			}
		})
	}
}

func TestNewWithdrawal(t *testing.T) {
	validAmount := big.NewInt(1000000)

	tests := []struct {
		name           string
		id             string
		idempotencyKey string
		userID         string
		asset          string
		amount         *big.Int
		destAddr       string
		network        string
		opts           []WithdrawalOption
		wantErr        error
	}{
		{
			name:           "valid withdrawal",
			id:             "test-id",
			idempotencyKey: "test-key",
			userID:         "user-1",
			asset:          "BTC",
			amount:         validAmount,
			destAddr:       "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			network:        "bitcoin",
			wantErr:        nil,
		},
		{
			name:           "empty idempotency key",
			id:             "test-id",
			idempotencyKey: "",
			userID:         "user-1",
			asset:          "BTC",
			amount:         validAmount,
			destAddr:       "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			network:        "bitcoin",
			wantErr:        ErrInvalidIdempotencyKey,
		},
		{
			name:           "nil amount",
			id:             "test-id",
			idempotencyKey: "test-key",
			userID:         "user-1",
			asset:          "BTC",
			amount:         nil,
			destAddr:       "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			network:        "bitcoin",
			wantErr:        ErrInvalidAmount,
		},
		{
			name:           "negative amount",
			id:             "test-id",
			idempotencyKey: "test-key",
			userID:         "user-1",
			asset:          "BTC",
			amount:         big.NewInt(-100),
			destAddr:       "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			network:        "bitcoin",
			wantErr:        ErrInvalidAmount,
		},
		{
			name:           "zero amount",
			id:             "test-id",
			idempotencyKey: "test-key",
			userID:         "user-1",
			asset:          "BTC",
			amount:         big.NewInt(0),
			destAddr:       "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			network:        "bitcoin",
			wantErr:        ErrInvalidAmount,
		},
		{
			name:           "empty destination address",
			id:             "test-id",
			idempotencyKey: "test-key",
			userID:         "user-1",
			asset:          "BTC",
			amount:         validAmount,
			destAddr:       "",
			network:        "bitcoin",
			wantErr:        ErrInvalidAddress,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWithdrawal(
				tt.id,
				tt.idempotencyKey,
				tt.userID,
				tt.asset,
				tt.amount,
				tt.destAddr,
				tt.network,
				tt.opts...,
			)

			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("NewWithdrawal() error = %v, want %v", err, tt.wantErr)
				}
				if w != nil {
					t.Errorf("NewWithdrawal() should return nil withdrawal on error")
				}
				return
			}

			if err != nil {
				t.Errorf("NewWithdrawal() unexpected error = %v", err)
				return
			}

			if w.ID != tt.id {
				t.Errorf("Withdrawal.ID = %v, want %v", w.ID, tt.id)
			}
			if w.IdempotencyKey != tt.idempotencyKey {
				t.Errorf("Withdrawal.IdempotencyKey = %v, want %v", w.IdempotencyKey, tt.idempotencyKey)
			}
			if w.Status != StatusPending {
				t.Errorf("Withdrawal.Status = %v, want %v", w.Status, StatusPending)
			}
			if w.MaxRetries != 3 {
				t.Errorf("Withdrawal.MaxRetries = %v, want 3", w.MaxRetries)
			}
		})
	}
}

func TestNewWithdrawal_WithOptions(t *testing.T) {
	validAmount := big.NewInt(1000000)

	t.Run("with max retries", func(t *testing.T) {
		w, err := NewWithdrawal(
			"test-id",
			"test-key",
			"user-1",
			"BTC",
			validAmount,
			"bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			"bitcoin",
			WithMaxRetries(5),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if w.MaxRetries != 5 {
			t.Errorf("MaxRetries = %v, want 5", w.MaxRetries)
		}
	})

	t.Run("with negative max retries", func(t *testing.T) {
		_, err := NewWithdrawal(
			"test-id",
			"test-key",
			"user-1",
			"BTC",
			validAmount,
			"bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			"bitcoin",
			WithMaxRetries(-1),
		)
		if err == nil {
			t.Error("expected error for negative max retries")
		}
	})

	t.Run("with status", func(t *testing.T) {
		w, err := NewWithdrawal(
			"test-id",
			"test-key",
			"user-1",
			"BTC",
			validAmount,
			"bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			"bitcoin",
			WithStatus(StatusProcessing),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if w.Status != StatusProcessing {
			t.Errorf("Status = %v, want %v", w.Status, StatusProcessing)
		}
	})

	t.Run("with processing version", func(t *testing.T) {
		w, err := NewWithdrawal(
			"test-id",
			"test-key",
			"user-1",
			"BTC",
			validAmount,
			"bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			"bitcoin",
			WithProcessingVersion(10),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if w.ProcessingVersion != 10 {
			t.Errorf("ProcessingVersion = %v, want 10", w.ProcessingVersion)
		}
	})
}

func TestWithdrawal_CanRetry(t *testing.T) {
	validAmount := big.NewInt(1000000)

	tests := []struct {
		name       string
		retryCount int
		maxRetries int
		status     WithdrawalStatus
		canRetry   bool
	}{
		{"can retry failed", 0, 3, StatusFailed, true},
		{"can retry retrying", 1, 3, StatusRetrying, true},
		{"cannot retry pending", 0, 3, StatusPending, false},
		{"cannot retry processing", 0, 3, StatusProcessing, false},
		{"cannot retry completed", 0, 3, StatusCompleted, false},
		{"max retries reached", 3, 3, StatusFailed, false},
		{"over max retries", 4, 3, StatusFailed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Withdrawal{
				ID:              "test-id",
				RetryCount:      tt.retryCount,
				MaxRetries:      tt.maxRetries,
				Status:          tt.status,
				Amount:          validAmount,
				IdempotencyKey:  "test-key",
				DestinationAddr: "addr",
			}
			if got := w.CanRetry(); got != tt.canRetry {
				t.Errorf("CanRetry() = %v, want %v", got, tt.canRetry)
			}
		})
	}
}

func TestWithdrawal_IncrementRetry(t *testing.T) {
	validAmount := big.NewInt(1000000)

	t.Run("successful increment", func(t *testing.T) {
		w := &Withdrawal{
			ID:              "test-id",
			RetryCount:      0,
			MaxRetries:      3,
			Status:          StatusFailed,
			Amount:          validAmount,
			IdempotencyKey:  "test-key",
			DestinationAddr: "addr",
		}
		err := w.IncrementRetry()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if w.RetryCount != 1 {
			t.Errorf("RetryCount = %v, want 1", w.RetryCount)
		}
		if w.Status != StatusRetrying {
			t.Errorf("Status = %v, want %v", w.Status, StatusRetrying)
		}
	})

	t.Run("max retries exceeded", func(t *testing.T) {
		w := &Withdrawal{
			ID:              "test-id",
			RetryCount:      3,
			MaxRetries:      3,
			Status:          StatusFailed,
			Amount:          validAmount,
			IdempotencyKey:  "test-key",
			DestinationAddr: "addr",
		}
		err := w.IncrementRetry()
		if err != ErrMaxRetriesExceeded {
			t.Errorf("error = %v, want %v", err, ErrMaxRetriesExceeded)
		}
	})
}

func TestWithdrawal_MarkProcessing(t *testing.T) {
	validAmount := big.NewInt(1000000)

	tests := []struct {
		name    string
		status  WithdrawalStatus
		wantErr bool
	}{
		{"from pending", StatusPending, false},
		{"from retrying", StatusRetrying, false},
		{"from processing", StatusProcessing, true},
		{"from completed", StatusCompleted, true},
		{"from failed", StatusFailed, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Withdrawal{
				ID:              "test-id",
				Status:          tt.status,
				Amount:          validAmount,
				IdempotencyKey:  "test-key",
				DestinationAddr: "addr",
			}
			err := w.MarkProcessing()
			if (err != nil) != tt.wantErr {
				t.Errorf("MarkProcessing() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && w.Status != StatusProcessing {
				t.Errorf("Status = %v, want %v", w.Status, StatusProcessing)
			}
		})
	}
}

func TestWithdrawal_MarkCompleted(t *testing.T) {
	validAmount := big.NewInt(1000000)

	tests := []struct {
		name    string
		status  WithdrawalStatus
		wantErr bool
	}{
		{"from processing", StatusProcessing, false},
		{"from pending", StatusPending, true},
		{"from completed", StatusCompleted, true},
		{"from failed", StatusFailed, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Withdrawal{
				ID:              "test-id",
				Status:          tt.status,
				Amount:          validAmount,
				IdempotencyKey:  "test-key",
				DestinationAddr: "addr",
			}
			err := w.MarkCompleted("0x123abc")
			if (err != nil) != tt.wantErr {
				t.Errorf("MarkCompleted() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if w.Status != StatusCompleted {
					t.Errorf("Status = %v, want %v", w.Status, StatusCompleted)
				}
				if w.TxHash != "0x123abc" {
					t.Errorf("TxHash = %v, want 0x123abc", w.TxHash)
				}
				if w.ProcessedAt == nil {
					t.Error("ProcessedAt should not be nil")
				}
			}
		})
	}
}

func TestWithdrawal_MarkFailed(t *testing.T) {
	validAmount := big.NewInt(1000000)

	tests := []struct {
		name    string
		status  WithdrawalStatus
		wantErr bool
	}{
		{"from pending", StatusPending, false},
		{"from processing", StatusProcessing, false},
		{"from retrying", StatusRetrying, false},
		{"from completed", StatusCompleted, true},
		{"from failed", StatusFailed, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Withdrawal{
				ID:              "test-id",
				Status:          tt.status,
				Amount:          validAmount,
				IdempotencyKey:  "test-key",
				DestinationAddr: "addr",
			}
			err := w.MarkFailed("test error")
			if (err != nil) != tt.wantErr {
				t.Errorf("MarkFailed() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if w.Status != StatusFailed {
					t.Errorf("Status = %v, want %v", w.Status, StatusFailed)
				}
				if w.ErrorMessage != "test error" {
					t.Errorf("ErrorMessage = %v, want 'test error'", w.ErrorMessage)
				}
			}
		})
	}
}

func TestWithdrawal_PrepareForRetry(t *testing.T) {
	validAmount := big.NewInt(1000000)

	t.Run("successful prepare", func(t *testing.T) {
		w := &Withdrawal{
			ID:              "test-id",
			RetryCount:      0,
			MaxRetries:      3,
			Status:          StatusFailed,
			Amount:          validAmount,
			IdempotencyKey:  "test-key",
			DestinationAddr: "addr",
		}
		err := w.PrepareForRetry()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if w.Status != StatusRetrying {
			t.Errorf("Status = %v, want %v", w.Status, StatusRetrying)
		}
	})

	t.Run("max retries exceeded", func(t *testing.T) {
		w := &Withdrawal{
			ID:              "test-id",
			RetryCount:      3,
			MaxRetries:      3,
			Status:          StatusFailed,
			Amount:          validAmount,
			IdempotencyKey:  "test-key",
			DestinationAddr: "addr",
		}
		err := w.PrepareForRetry()
		if err != ErrMaxRetriesExceeded {
			t.Errorf("error = %v, want %v", err, ErrMaxRetriesExceeded)
		}
	})
}

func TestIdempotencyKey(t *testing.T) {
	key := NewIdempotencyKey("test-key", "withdrawal-1", 24*time.Hour)

	if key.Key != "test-key" {
		t.Errorf("Key = %v, want 'test-key'", key.Key)
	}
	if key.WithdrawalID != "withdrawal-1" {
		t.Errorf("WithdrawalID = %v, want 'withdrawal-1'", key.WithdrawalID)
	}
	if key.IsExpired() {
		t.Error("new key should not be expired")
	}
}

func TestIdempotencyKey_IsExpired(t *testing.T) {
	t.Run("not expired", func(t *testing.T) {
		key := NewIdempotencyKey("test-key", "withdrawal-1", 24*time.Hour)
		if key.IsExpired() {
			t.Error("key should not be expired")
		}
	})

	t.Run("expired", func(t *testing.T) {
		key := &IdempotencyKey{
			Key:          "test-key",
			WithdrawalID: "withdrawal-1",
			CreatedAt:    time.Now().UTC().Add(-2 * time.Hour),
			ExpiresAt:    time.Now().UTC().Add(-1 * time.Hour),
		}
		if !key.IsExpired() {
			t.Error("key should be expired")
		}
	})
}

func TestProcessingLock(t *testing.T) {
	lock := NewProcessingLock("withdrawal-1", "processor-1", 5*time.Minute)

	if lock.WithdrawalID != "withdrawal-1" {
		t.Errorf("WithdrawalID = %v, want 'withdrawal-1'", lock.WithdrawalID)
	}
	if lock.LockedBy != "processor-1" {
		t.Errorf("LockedBy = %v, want 'processor-1'", lock.LockedBy)
	}
	if lock.IsExpired() {
		t.Error("new lock should not be expired")
	}
}

func TestProcessingLock_IsExpired(t *testing.T) {
	t.Run("not expired", func(t *testing.T) {
		lock := NewProcessingLock("withdrawal-1", "processor-1", 5*time.Minute)
		if lock.IsExpired() {
			t.Error("lock should not be expired")
		}
	})

	t.Run("expired", func(t *testing.T) {
		lock := &ProcessingLock{
			WithdrawalID: "withdrawal-1",
			LockedBy:     "processor-1",
			LockedAt:     time.Now().UTC().Add(-10 * time.Minute),
			ExpiresAt:    time.Now().UTC().Add(-5 * time.Minute),
		}
		if !lock.IsExpired() {
			t.Error("lock should be expired")
		}
	})
}

func TestWithdrawalEvent(t *testing.T) {
	now := time.Now().UTC()
	event := WithdrawalEvent{
		WithdrawalID: "withdrawal-1",
		EventType:    EventTypeCreated,
		Status:       StatusPending,
		Timestamp:    now,
		Metadata:     map[string]string{"key": "value"},
	}

	if event.WithdrawalID != "withdrawal-1" {
		t.Errorf("WithdrawalID = %v, want 'withdrawal-1'", event.WithdrawalID)
	}
	if event.EventType != EventTypeCreated {
		t.Errorf("EventType = %v, want %v", event.EventType, EventTypeCreated)
	}
}

func TestEventTypes(t *testing.T) {
	tests := []struct {
		constant string
		expected string
	}{
		{EventTypeCreated, "withdrawal.created"},
		{EventTypeProcessing, "withdrawal.processing"},
		{EventTypeCompleted, "withdrawal.completed"},
		{EventTypeFailed, "withdrawal.failed"},
		{EventTypeRetrying, "withdrawal.retrying"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("event type = %v, want %v", tt.constant, tt.expected)
			}
		})
	}
}
