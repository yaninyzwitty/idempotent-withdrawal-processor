package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	"go.uber.org/zap"
)

type InMemoryWithdrawalRepository struct {
	byID             map[string]*entities.Withdrawal
	byIdempotencyKey map[string]string
	byUserID         map[string][]string
	byStatus         map[entities.WithdrawalStatus][]string
	mu               sync.RWMutex
	logger           *zap.Logger
}

type InMemoryWithdrawalRepositoryOption func(*InMemoryWithdrawalRepository)

func WithMemoryLogger(logger *zap.Logger) InMemoryWithdrawalRepositoryOption {
	return func(r *InMemoryWithdrawalRepository) {
		r.logger = logger
	}
}

func NewInMemoryWithdrawalRepository(opts ...InMemoryWithdrawalRepositoryOption) *InMemoryWithdrawalRepository {
	r := &InMemoryWithdrawalRepository{
		byID:             make(map[string]*entities.Withdrawal),
		byIdempotencyKey: make(map[string]string),
		byUserID:         make(map[string][]string),
		byStatus:         make(map[entities.WithdrawalStatus][]string),
		logger:           zap.NewNop(),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *InMemoryWithdrawalRepository) Create(ctx context.Context, withdrawal *entities.Withdrawal) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byIdempotencyKey[withdrawal.IdempotencyKey]; exists {
		return entities.ErrWithdrawalAlreadyExist
	}

	if _, exists := r.byID[withdrawal.ID]; exists {
		return entities.ErrWithdrawalAlreadyExist
	}

	r.byID[withdrawal.ID] = withdrawal
	r.byIdempotencyKey[withdrawal.IdempotencyKey] = withdrawal.ID
	r.byUserID[withdrawal.UserID] = append(r.byUserID[withdrawal.UserID], withdrawal.ID)
	r.byStatus[withdrawal.Status] = append(r.byStatus[withdrawal.Status], withdrawal.ID)

	r.logger.Debug("created withdrawal",
		zap.String("id", withdrawal.ID),
		zap.String("idempotency_key", withdrawal.IdempotencyKey),
	)

	return nil
}

func (r *InMemoryWithdrawalRepository) GetByID(ctx context.Context, id string) (*entities.Withdrawal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	withdrawal, exists := r.byID[id]
	if !exists {
		return nil, entities.ErrWithdrawalNotFound
	}

	return withdrawal, nil
}

func (r *InMemoryWithdrawalRepository) GetByIdempotencyKey(ctx context.Context, key string) (*entities.Withdrawal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	id, exists := r.byIdempotencyKey[key]
	if !exists {
		return nil, entities.ErrWithdrawalNotFound
	}

	return r.byID[id], nil
}

func (r *InMemoryWithdrawalRepository) Update(ctx context.Context, withdrawal *entities.Withdrawal) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, exists := r.byID[withdrawal.ID]
	if !exists {
		return entities.ErrWithdrawalNotFound
	}

	oldStatus := existing.Status
	newStatus := withdrawal.Status

	if oldStatus != newStatus {
		r.removeFromStatusSlice(oldStatus, withdrawal.ID)
		r.byStatus[newStatus] = append(r.byStatus[newStatus], withdrawal.ID)
	}

	withdrawal.UpdatedAt = time.Now().UTC()
	r.byID[withdrawal.ID] = withdrawal

	r.logger.Debug("updated withdrawal",
		zap.String("id", withdrawal.ID),
		zap.String("old_status", oldStatus.String()),
		zap.String("new_status", newStatus.String()),
	)

	return nil
}

func (r *InMemoryWithdrawalRepository) UpdateWithVersion(ctx context.Context, withdrawal *entities.Withdrawal, expectedVersion int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, exists := r.byID[withdrawal.ID]
	if !exists {
		return entities.ErrWithdrawalNotFound
	}

	if existing.ProcessingVersion != expectedVersion {
		return fmt.Errorf("version conflict: expected %d, got %d", expectedVersion, existing.ProcessingVersion)
	}

	withdrawal.ProcessingVersion = existing.ProcessingVersion + 1
	withdrawal.UpdatedAt = time.Now().UTC()

	oldStatus := existing.Status
	newStatus := withdrawal.Status

	if oldStatus != newStatus {
		r.removeFromStatusSlice(oldStatus, withdrawal.ID)
		r.byStatus[newStatus] = append(r.byStatus[newStatus], withdrawal.ID)
	}

	r.byID[withdrawal.ID] = withdrawal

	return nil
}

func (r *InMemoryWithdrawalRepository) ListByUserID(ctx context.Context, userID string, limit, offset int) ([]*entities.Withdrawal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids, exists := r.byUserID[userID]
	if !exists {
		return []*entities.Withdrawal{}, nil
	}

	start := offset
	if start > len(ids) {
		return []*entities.Withdrawal{}, nil
	}

	end := start + limit
	if end > len(ids) {
		end = len(ids)
	}

	withdrawals := make([]*entities.Withdrawal, 0, end-start)
	for i := start; i < end; i++ {
		if w, ok := r.byID[ids[i]]; ok {
			withdrawals = append(withdrawals, w)
		}
	}

	return withdrawals, nil
}

func (r *InMemoryWithdrawalRepository) ListByStatus(ctx context.Context, status entities.WithdrawalStatus, limit int) ([]*entities.Withdrawal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids, exists := r.byStatus[status]
	if !exists {
		return []*entities.Withdrawal{}, nil
	}

	count := limit
	if count > len(ids) {
		count = len(ids)
	}

	withdrawals := make([]*entities.Withdrawal, 0, count)
	for i := 0; i < count; i++ {
		if w, ok := r.byID[ids[i]]; ok {
			withdrawals = append(withdrawals, w)
		}
	}

	return withdrawals, nil
}

func (r *InMemoryWithdrawalRepository) GetPendingWithdrawals(ctx context.Context, limit int) ([]*entities.Withdrawal, error) {
	pending, err := r.ListByStatus(ctx, entities.StatusPending, limit)
	if err != nil {
		return nil, err
	}

	retrying, err := r.ListByStatus(ctx, entities.StatusRetrying, limit-len(pending))
	if err != nil {
		return nil, err
	}

	return append(pending, retrying...), nil
}

func (r *InMemoryWithdrawalRepository) removeFromStatusSlice(status entities.WithdrawalStatus, id string) {
	ids := r.byStatus[status]
	for i, existingID := range ids {
		if existingID == id {
			r.byStatus[status] = append(ids[:i], ids[i+1:]...)
			break
		}
	}
}

var _ repositories.WithdrawalRepository = (*InMemoryWithdrawalRepository)(nil)

type InMemoryIdempotencyRepository struct {
	keys map[string]*entities.IdempotencyKey
	mu   sync.RWMutex
	ttl  time.Duration
}

func NewInMemoryIdempotencyRepository(ttl time.Duration) *InMemoryIdempotencyRepository {
	r := &InMemoryIdempotencyRepository{
		keys: make(map[string]*entities.IdempotencyKey),
		ttl:  ttl,
	}

	go r.cleanup()

	return r
}

func (r *InMemoryIdempotencyRepository) cleanup() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		r.mu.Lock()
		for key, ik := range r.keys {
			if ik.IsExpired() {
				delete(r.keys, key)
			}
		}
		r.mu.Unlock()
	}
}

func (r *InMemoryIdempotencyRepository) Store(ctx context.Context, key *entities.IdempotencyKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.keys[key.Key] = key
	return nil
}

func (r *InMemoryIdempotencyRepository) Get(ctx context.Context, key string) (*entities.IdempotencyKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ik, exists := r.keys[key]
	if !exists {
		return nil, fmt.Errorf("idempotency key not found: %s", key)
	}

	return ik, nil
}

func (r *InMemoryIdempotencyRepository) Delete(ctx context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.keys, key)
	return nil
}

func (r *InMemoryIdempotencyRepository) Exists(ctx context.Context, key string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ik, exists := r.keys[key]
	if !exists {
		return false, nil
	}

	return !ik.IsExpired(), nil
}

func (r *InMemoryIdempotencyRepository) Acquire(ctx context.Context, key string, withdrawalID string, ttl int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ik, exists := r.keys[key]; exists && !ik.IsExpired() {
		return false, nil
	}

	r.keys[key] = entities.NewIdempotencyKey(key, withdrawalID, time.Duration(ttl)*time.Second)
	return true, nil
}

func (r *InMemoryIdempotencyRepository) Release(ctx context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.keys, key)
	return nil
}

var _ repositories.IdempotencyRepository = (*InMemoryIdempotencyRepository)(nil)

type InMemoryProcessingLockRepository struct {
	locks map[string]*entities.ProcessingLock
	mu    sync.RWMutex
}

func NewInMemoryProcessingLockRepository() *InMemoryProcessingLockRepository {
	r := &InMemoryProcessingLockRepository{
		locks: make(map[string]*entities.ProcessingLock),
	}

	go r.cleanup()

	return r
}

func (r *InMemoryProcessingLockRepository) cleanup() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		r.mu.Lock()
		for id, lock := range r.locks {
			if lock.IsExpired() {
				delete(r.locks, id)
			}
		}
		r.mu.Unlock()
	}
}

func (r *InMemoryProcessingLockRepository) Acquire(ctx context.Context, withdrawalID string, processorID string, ttl int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if lock, exists := r.locks[withdrawalID]; exists && !lock.IsExpired() {
		return false, nil
	}

	r.locks[withdrawalID] = entities.NewProcessingLock(withdrawalID, processorID, time.Duration(ttl)*time.Second)
	return true, nil
}

func (r *InMemoryProcessingLockRepository) Release(ctx context.Context, withdrawalID string, processorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if lock, exists := r.locks[withdrawalID]; exists {
		if lock.LockedBy == processorID {
			delete(r.locks, withdrawalID)
		}
	}

	return nil
}

func (r *InMemoryProcessingLockRepository) IsLocked(ctx context.Context, withdrawalID string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	lock, exists := r.locks[withdrawalID]
	if !exists {
		return false, nil
	}

	return !lock.IsExpired(), nil
}

func (r *InMemoryProcessingLockRepository) Extend(ctx context.Context, withdrawalID string, processorID string, additionalTTL int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	lock, exists := r.locks[withdrawalID]
	if !exists || lock.IsExpired() {
		return fmt.Errorf("lock not found or expired")
	}

	if lock.LockedBy != processorID {
		return fmt.Errorf("lock owned by different processor")
	}

	lock.ExpiresAt = lock.ExpiresAt.Add(time.Duration(additionalTTL) * time.Second)
	return nil
}

var _ repositories.ProcessingLockRepository = (*InMemoryProcessingLockRepository)(nil)

func GenerateID() string {
	return uuid.New().String()
}

func HashIdempotencyKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}
