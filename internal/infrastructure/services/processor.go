package services

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	kafkainfra "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/kafka"
	"go.uber.org/zap"
)

type ProcessorConfig struct {
	ProcessorID     string
	BatchSize       int
	PollInterval    time.Duration
	LockTTL         int64
	MaxConcurrent   int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	RetryMultiplier float64
}

type ProcessorOption func(*ProcessorConfig)

func WithProcessorID(id string) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.ProcessorID = id
	}
}

func WithBatchSize(size int) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.BatchSize = size
	}
}

func WithPollInterval(interval time.Duration) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.PollInterval = interval
	}
}

func WithLockTTL(ttl int64) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.LockTTL = ttl
	}
}

func WithMaxConcurrent(max int) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.MaxConcurrent = max
	}
}

func WithRetryBaseDelay(delay time.Duration) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.RetryBaseDelay = delay
	}
}

func WithRetryMaxDelay(delay time.Duration) ProcessorOption {
	return func(c *ProcessorConfig) {
		c.RetryMaxDelay = delay
	}
}

func DefaultProcessorConfig() *ProcessorConfig {
	return &ProcessorConfig{
		ProcessorID:     "processor-1",
		BatchSize:       100,
		PollInterval:    100 * time.Millisecond,
		LockTTL:         300,
		MaxConcurrent:   10,
		RetryBaseDelay:  1 * time.Second,
		RetryMaxDelay:   30 * time.Second,
		RetryMultiplier: 2.0,
	}
}

type ProcessorStats struct {
	TotalProcessed   int64
	TotalSucceeded   int64
	TotalFailed      int64
	TotalRetries     int64
	CurrentQueueSize int64
}

type withdrawalProcessor struct {
	config         *ProcessorConfig
	withdrawalRepo repositories.WithdrawalRepository
	lockRepo       repositories.ProcessingLockRepository
	blockchainSvc  domain.BlockchainService
	logger         *zap.Logger
	stats          ProcessorStats
	running        atomic.Bool
	stopCh         chan struct{}
	wg             sync.WaitGroup
	cancel         context.CancelFunc
}

func NewWithdrawalProcessor(
	withdrawalRepo repositories.WithdrawalRepository,
	lockRepo repositories.ProcessingLockRepository,
	blockchainSvc domain.BlockchainService,
	opts ...ProcessorOption,
) domain.WithdrawalProcessor {
	config := DefaultProcessorConfig()
	for _, opt := range opts {
		opt(config)
	}

	return &withdrawalProcessor{
		config:         config,
		withdrawalRepo: withdrawalRepo,
		lockRepo:       lockRepo,
		blockchainSvc:  blockchainSvc,
		logger:         zap.NewNop(),
		stopCh:         make(chan struct{}),
	}
}

func (p *withdrawalProcessor) Process(ctx context.Context, withdrawal *entities.Withdrawal) error {
	acquired, err := p.lockRepo.Acquire(ctx, withdrawal.ID, p.config.ProcessorID, p.config.LockTTL)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		p.logger.Debug("withdrawal already being processed",
			zap.String("withdrawal_id", withdrawal.ID),
		)
		return nil
	}

	defer func() {
		_ = p.lockRepo.Release(ctx, withdrawal.ID, p.config.ProcessorID)
	}()

	if err := withdrawal.MarkProcessing(); err != nil {
		return fmt.Errorf("failed to mark withdrawal as processing: %w", err)
	}

	if err := p.withdrawalRepo.Update(ctx, withdrawal); err != nil {
		return fmt.Errorf("failed to update withdrawal status: %w", err)
	}

	txHash, err := p.blockchainSvc.BroadcastTransaction(ctx, withdrawal)
	if err != nil {
		atomic.AddInt64(&p.stats.TotalRetries, 1)

		if withdrawal.CanRetry() {
			if err := withdrawal.IncrementRetry(); err != nil {
				p.logger.Error("failed to increment retry count",
					zap.Error(err),
					zap.String("withdrawal_id", withdrawal.ID),
				)
			}

			if err := p.withdrawalRepo.Update(ctx, withdrawal); err != nil {
				p.logger.Error("failed to update withdrawal for retry",
					zap.Error(err),
					zap.String("withdrawal_id", withdrawal.ID),
				)
			}

			return fmt.Errorf("transaction broadcast failed, scheduled for retry: %w", err)
		}

		if err := withdrawal.MarkFailed(err.Error()); err != nil {
			p.logger.Error("failed to mark withdrawal as failed",
				zap.Error(err),
				zap.String("withdrawal_id", withdrawal.ID),
			)
		}

		if err := p.withdrawalRepo.Update(ctx, withdrawal); err != nil {
			p.logger.Error("failed to update failed withdrawal",
				zap.Error(err),
				zap.String("withdrawal_id", withdrawal.ID),
			)
		}

		atomic.AddInt64(&p.stats.TotalFailed, 1)
		return fmt.Errorf("transaction broadcast failed, max retries exceeded: %w", err)
	}

	if err := withdrawal.MarkCompleted(txHash); err != nil {
		return fmt.Errorf("failed to mark withdrawal as completed: %w", err)
	}

	if err := p.withdrawalRepo.Update(ctx, withdrawal); err != nil {
		p.logger.Error("failed to update completed withdrawal",
			zap.Error(err),
			zap.String("withdrawal_id", withdrawal.ID),
		)
		return fmt.Errorf("failed to update completed withdrawal: %w", err)
	}

	atomic.AddInt64(&p.stats.TotalSucceeded, 1)
	p.logger.Info("withdrawal completed successfully",
		zap.String("withdrawal_id", withdrawal.ID),
		zap.String("tx_hash", txHash),
	)

	return nil
}

func (p *withdrawalProcessor) ProcessBatch(ctx context.Context, withdrawals []*entities.Withdrawal) error {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, p.config.MaxConcurrent)
	errCh := make(chan error, len(withdrawals))

	for _, w := range withdrawals {
		wg.Add(1)
		go func(withdrawal *entities.Withdrawal) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			atomic.AddInt64(&p.stats.TotalProcessed, 1)

			if err := p.Process(ctx, withdrawal); err != nil {
				errCh <- err
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch processing completed with %d errors", len(errors))
	}

	return nil
}

func (p *withdrawalProcessor) Start(ctx context.Context) error {
	if p.running.Swap(true) {
		return fmt.Errorf("processor already running")
	}

	p.logger.Info("starting withdrawal processor",
		zap.String("processor_id", p.config.ProcessorID),
	)

	runCtx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	p.wg.Add(1)
	go p.run(runCtx)

	return nil
}

func (p *withdrawalProcessor) run(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("processor stopped by context")
			return
		case <-p.stopCh:
			p.logger.Info("processor stopped by signal")
			return
		case <-ticker.C:
			p.processPending(ctx)
		}
	}
}

func (p *withdrawalProcessor) processPending(ctx context.Context) {
	withdrawals, err := p.withdrawalRepo.GetPendingWithdrawals(ctx, p.config.BatchSize)
	if err != nil {
		p.logger.Error("failed to get pending withdrawals", zap.Error(err))
		return
	}

	atomic.StoreInt64(&p.stats.CurrentQueueSize, int64(len(withdrawals)))

	if len(withdrawals) == 0 {
		return
	}

	p.logger.Debug("processing pending withdrawals",
		zap.Int("count", len(withdrawals)),
	)

	if err := p.ProcessBatch(ctx, withdrawals); err != nil {
		p.logger.Error("batch processing error", zap.Error(err))
	}
}

func (p *withdrawalProcessor) Stop(ctx context.Context) error {
	if !p.running.Swap(false) {
		return fmt.Errorf("processor not running")
	}

	if p.cancel != nil {
		p.cancel()
	}
	close(p.stopCh)
	p.wg.Wait()

	p.logger.Info("withdrawal processor stopped",
		zap.String("processor_id", p.config.ProcessorID),
	)

	return nil
}

func (p *withdrawalProcessor) GetStats() ProcessorStats {
	return ProcessorStats{
		TotalProcessed:   atomic.LoadInt64(&p.stats.TotalProcessed),
		TotalSucceeded:   atomic.LoadInt64(&p.stats.TotalSucceeded),
		TotalFailed:      atomic.LoadInt64(&p.stats.TotalFailed),
		TotalRetries:     atomic.LoadInt64(&p.stats.TotalRetries),
		CurrentQueueSize: atomic.LoadInt64(&p.stats.CurrentQueueSize),
	}
}

var _ domain.WithdrawalProcessor = (*withdrawalProcessor)(nil)

type KafkaProcessor struct {
	consumer  *kafkainfra.Consumer
	processor domain.WithdrawalProcessor
	svc       domain.WithdrawalService
	logger    *zap.Logger
	running   atomic.Bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

type KafkaProcessorOption func(*KafkaProcessor)

func WithKafkaProcessorLogger(logger *zap.Logger) KafkaProcessorOption {
	return func(kp *KafkaProcessor) {
		kp.logger = logger
	}
}

func NewKafkaProcessor(
	consumer *kafkainfra.Consumer,
	processor domain.WithdrawalProcessor,
	svc domain.WithdrawalService,
	opts ...KafkaProcessorOption,
) *KafkaProcessor {
	kp := &KafkaProcessor{
		consumer:  consumer,
		processor: processor,
		svc:       svc,
		logger:    zap.NewNop(),
		stopCh:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(kp)
	}

	kp.consumer.SetHandler(kp.handleMessage)

	return kp
}

func (kp *KafkaProcessor) handleMessage(ctx context.Context, msg *kafkainfra.WithdrawalMessage) error {
	withdrawal, err := kp.svc.GetWithdrawal(ctx, msg.ID)
	if err != nil {
		kp.logger.Error("failed to get withdrawal",
			zap.Error(err),
			zap.String("withdrawal_id", msg.ID),
		)
		return err
	}

	return kp.processor.Process(ctx, withdrawal)
}

func (kp *KafkaProcessor) Start(ctx context.Context) error {
	if kp.running.Swap(true) {
		return fmt.Errorf("kafka processor already running")
	}

	kp.logger.Info("starting kafka processor")

	kp.wg.Add(1)
	go func() {
		defer kp.wg.Done()
		if err := kp.consumer.Consume(ctx); err != nil {
			kp.logger.Error("consumer error", zap.Error(err))
		}
	}()

	return nil
}

func (kp *KafkaProcessor) Stop(ctx context.Context) error {
	if !kp.running.Swap(false) {
		return fmt.Errorf("kafka processor not running")
	}

	close(kp.stopCh)
	kp.wg.Wait()

	if err := kp.consumer.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}

	return nil
}

func (kp *KafkaProcessor) Process(ctx context.Context, withdrawal *entities.Withdrawal) error {
	return kp.processor.Process(ctx, withdrawal)
}

func (kp *KafkaProcessor) ProcessBatch(ctx context.Context, withdrawals []*entities.Withdrawal) error {
	return kp.processor.ProcessBatch(ctx, withdrawals)
}

var _ domain.WithdrawalProcessor = (*KafkaProcessor)(nil)
