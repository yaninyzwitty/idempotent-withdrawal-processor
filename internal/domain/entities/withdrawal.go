package entities

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"
)

var (
	ErrInvalidAmount          = errors.New("invalid amount")
	ErrInvalidAddress         = errors.New("invalid destination address")
	ErrInvalidIdempotencyKey  = errors.New("invalid idempotency key")
	ErrMaxRetriesExceeded     = errors.New("max retries exceeded")
	ErrInvalidStatus          = errors.New("invalid status transition")
	ErrWithdrawalNotFound     = errors.New("withdrawal not found")
	ErrWithdrawalAlreadyExist = errors.New("withdrawal already exists")
)

type WithdrawalStatus int

const (
	StatusUnspecified WithdrawalStatus = iota
	StatusPending
	StatusProcessing
	StatusCompleted
	StatusFailed
	StatusRetrying
)

func (s WithdrawalStatus) String() string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusProcessing:
		return "PROCESSING"
	case StatusCompleted:
		return "COMPLETED"
	case StatusFailed:
		return "FAILED"
	case StatusRetrying:
		return "RETRYING"
	default:
		return "UNSPECIFIED"
	}
}

func (s WithdrawalStatus) CanTransitionTo(newStatus WithdrawalStatus) bool {
	transitions := map[WithdrawalStatus][]WithdrawalStatus{
		StatusPending:    {StatusProcessing, StatusFailed},
		StatusProcessing: {StatusCompleted, StatusFailed, StatusRetrying},
		StatusRetrying:   {StatusProcessing, StatusFailed},
		StatusFailed:     {StatusRetrying},
		StatusCompleted:  {},
	}

	allowed, exists := transitions[s]
	if !exists {
		return false
	}

	return slices.Contains(allowed, newStatus)
}

type Withdrawal struct {
	ID                string
	IdempotencyKey    string
	UserID            string
	Asset             string
	Amount            *big.Int
	DestinationAddr   string
	Network           string
	Status            WithdrawalStatus
	RetryCount        int
	MaxRetries        int
	ErrorMessage      string
	TxHash            string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	ProcessedAt       *time.Time
	ProcessingVersion int64
}

type WithdrawalOption func(*Withdrawal) error

func NewWithdrawal(
	id string,
	idempotencyKey string,
	userID string,
	asset string,
	amount *big.Int,
	destinationAddr string,
	network string,
	opts ...WithdrawalOption,
) (*Withdrawal, error) {
	if idempotencyKey == "" {
		return nil, ErrInvalidIdempotencyKey
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, ErrInvalidAmount
	}
	if destinationAddr == "" {
		return nil, ErrInvalidAddress
	}

	now := time.Now().UTC()
	w := &Withdrawal{
		ID:              id,
		IdempotencyKey:  idempotencyKey,
		UserID:          userID,
		Asset:           asset,
		Amount:          amount,
		DestinationAddr: destinationAddr,
		Network:         network,
		Status:          StatusPending,
		RetryCount:      0,
		MaxRetries:      3,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, err
		}
	}

	return w, nil
}

func WithMaxRetries(maxRetries int) WithdrawalOption {
	return func(w *Withdrawal) error {
		if maxRetries < 0 {
			return fmt.Errorf("max retries cannot be negative: %d", maxRetries)
		}
		w.MaxRetries = maxRetries
		return nil
	}
}

func WithStatus(status WithdrawalStatus) WithdrawalOption {
	return func(w *Withdrawal) error {
		w.Status = status
		return nil
	}
}

func WithProcessingVersion(version int64) WithdrawalOption {
	return func(w *Withdrawal) error {
		w.ProcessingVersion = version
		return nil
	}
}

func (w *Withdrawal) CanRetry() bool {
	return w.RetryCount < w.MaxRetries && (w.Status == StatusFailed || w.Status == StatusRetrying)
}

func (w *Withdrawal) IncrementRetry() error {
	if !w.CanRetry() {
		return ErrMaxRetriesExceeded
	}
	w.RetryCount++
	w.Status = StatusRetrying
	w.UpdatedAt = time.Now().UTC()
	return nil
}

func (w *Withdrawal) MarkProcessing() error {
	if !w.Status.CanTransitionTo(StatusProcessing) {
		return fmt.Errorf("%w: cannot transition from %s to PROCESSING", ErrInvalidStatus, w.Status)
	}
	w.Status = StatusProcessing
	w.UpdatedAt = time.Now().UTC()
	return nil
}

func (w *Withdrawal) MarkCompleted(txHash string) error {
	if !w.Status.CanTransitionTo(StatusCompleted) {
		return fmt.Errorf("%w: cannot transition from %s to COMPLETED", ErrInvalidStatus, w.Status)
	}
	now := time.Now().UTC()
	w.Status = StatusCompleted
	w.TxHash = txHash
	w.UpdatedAt = now
	w.ProcessedAt = &now
	return nil
}

func (w *Withdrawal) MarkFailed(errMsg string) error {
	if !w.Status.CanTransitionTo(StatusFailed) {
		return fmt.Errorf("%w: cannot transition from %s to FAILED", ErrInvalidStatus, w.Status)
	}
	w.Status = StatusFailed
	w.ErrorMessage = errMsg
	w.UpdatedAt = time.Now().UTC()
	return nil
}

func (w *Withdrawal) PrepareForRetry() error {
	if !w.CanRetry() {
		return ErrMaxRetriesExceeded
	}
	w.Status = StatusRetrying
	w.UpdatedAt = time.Now().UTC()
	return nil
}

type IdempotencyKey struct {
	Key          string
	WithdrawalID string
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

func NewIdempotencyKey(key string, withdrawalID string, ttl time.Duration) *IdempotencyKey {
	now := time.Now().UTC()
	return &IdempotencyKey{
		Key:          key,
		WithdrawalID: withdrawalID,
		CreatedAt:    now,
		ExpiresAt:    now.Add(ttl),
	}
}

func (ik *IdempotencyKey) IsExpired() bool {
	return time.Now().UTC().After(ik.ExpiresAt)
}

type ProcessingLock struct {
	WithdrawalID string
	LockedBy     string
	LockedAt     time.Time
	ExpiresAt    time.Time
}

func NewProcessingLock(withdrawalID string, lockedBy string, ttl time.Duration) *ProcessingLock {
	now := time.Now().UTC()
	return &ProcessingLock{
		WithdrawalID: withdrawalID,
		LockedBy:     lockedBy,
		LockedAt:     now,
		ExpiresAt:    now.Add(ttl),
	}
}

func (pl *ProcessingLock) IsExpired() bool {
	return time.Now().UTC().After(pl.ExpiresAt)
}

type WithdrawalEvent struct {
	WithdrawalID string
	EventType    string
	Status       WithdrawalStatus
	Timestamp    time.Time
	Metadata     map[string]string
}

const (
	EventTypeCreated    = "withdrawal.created"
	EventTypeProcessing = "withdrawal.processing"
	EventTypeCompleted  = "withdrawal.completed"
	EventTypeFailed     = "withdrawal.failed"
	EventTypeRetrying   = "withdrawal.retrying"
)
