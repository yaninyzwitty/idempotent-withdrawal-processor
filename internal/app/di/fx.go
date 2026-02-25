package di

import (
	"context"
	"fmt"
	"net"
	"time"

	withdrawalv1 "github.com/yaninyzwitty/idempotent-widthrawal-processor/gen/withdrawal/v1"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/app/config"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/repositories"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/kafka"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/persistence/postgres"
	infraServices "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/services"
	grpchandler "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/transport/grpc"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewFxApp(opts ...fx.Option) *fx.App {
	options := append([]fx.Option{
		fx.Provide(
			NewLogger,
			NewConfig,
			NewPostgresPool,
			NewWithdrawalRepository,
			NewIdempotencyRepository,
			NewProcessingLockRepository,
			NewWithdrawalService,
			NewIdempotencyService,
			NewBlockchainService,
			NewRetryService,
			NewWithdrawalProcessor,
			NewKafkaConsumer,
			NewKafkaProducer,
			NewKafkaProcessor,
			NewGRPCServer,
			NewGRPCListener,
		),
		fx.Invoke(StartGRPCServer, StartProcessor, RegisterLifecycle),
	}, opts...)

	return fx.New(options...)
}

func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	var logger *zap.Logger
	var err error

	if cfg.Logging.Format == "json" {
		logger, err = zap.NewProduction()
	} else {
		logger, err = zap.NewDevelopment()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	switch cfg.Logging.Level {
	case "debug":
		logger = logger.WithOptions(zap.IncreaseLevel(zap.DebugLevel))
	case "info":
		logger = logger.WithOptions(zap.IncreaseLevel(zap.InfoLevel))
	case "warn":
		logger = logger.WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	case "error":
		logger = logger.WithOptions(zap.IncreaseLevel(zap.ErrorLevel))
	}

	return logger, nil
}

func NewConfig() *config.Config {
	configPath := config.GetConfigPath()

	cfg, err := config.Load(configPath)
	if err != nil {
		panic(fmt.Sprintf("failed to load config: %v", err))
	}
	return cfg
}

func NewPostgresPool(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*postgres.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), cfg.Postgres.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("closing postgres connection pool")
			pool.Close()
			return nil
		},
	})

	return pool, nil
}

func NewWithdrawalRepository(pool *postgres.Pool, logger *zap.Logger) repositories.WithdrawalRepository {
	return postgres.NewPostgresWithdrawalRepository(pool, postgres.WithPostgresLogger(logger))
}

func NewIdempotencyRepository(pool *postgres.Pool, cfg *config.Config) repositories.IdempotencyRepository {
	return postgres.NewPostgresIdempotencyRepository(pool, cfg.Idempotency.KeyTTL)
}

func NewProcessingLockRepository(pool *postgres.Pool) repositories.ProcessingLockRepository {
	return postgres.NewPostgresProcessingLockRepository(pool)
}

func NewWithdrawalService(
	withdrawalRepo repositories.WithdrawalRepository,
	idempotencyRepo repositories.IdempotencyRepository,
	lockRepo repositories.ProcessingLockRepository,
	logger *zap.Logger,
) domain.WithdrawalService {
	return infraServices.NewWithdrawalService(
		withdrawalRepo,
		idempotencyRepo,
		lockRepo,
		infraServices.WithWithdrawalServiceLogger(logger),
	)
}

func NewIdempotencyService(
	idempotencyRepo repositories.IdempotencyRepository,
	withdrawalRepo repositories.WithdrawalRepository,
	cfg *config.Config,
) domain.IdempotencyService {
	return infraServices.NewIdempotencyService(
		idempotencyRepo,
		withdrawalRepo,
		cfg.Idempotency.KeyTTL,
	)
}

func NewBlockchainService(logger *zap.Logger) domain.BlockchainService {
	return infraServices.NewMockBlockchainService(
		infraServices.WithBlockchainLogger(logger),
	)
}

func NewRetryService() domain.RetryService {
	return infraServices.NewRetryService()
}

func NewWithdrawalProcessor(
	withdrawalRepo repositories.WithdrawalRepository,
	lockRepo repositories.ProcessingLockRepository,
	blockchainSvc domain.BlockchainService,
	cfg *config.Config,
	logger *zap.Logger,
) domain.WithdrawalProcessor {
	return infraServices.NewWithdrawalProcessor(
		withdrawalRepo,
		lockRepo,
		blockchainSvc,
		infraServices.WithProcessorID(cfg.Processor.ID),
		infraServices.WithBatchSize(cfg.Processor.BatchSize),
		infraServices.WithPollInterval(cfg.Processor.PollInterval),
		infraServices.WithLockTTL(int64(cfg.Processor.LockTTL/time.Second)),
		infraServices.WithMaxConcurrent(cfg.Processor.MaxConcurrent),
		infraServices.WithRetryBaseDelay(cfg.Processor.RetryBaseDelay),
		infraServices.WithRetryMaxDelay(cfg.Processor.RetryMaxDelay),
	)
}

func NewKafkaConsumer(cfg *config.Config) (*kafka.Consumer, error) {
	return kafka.NewConsumer(
		kafka.WithBrokers(cfg.Kafka.Brokers),
		kafka.WithTopic(cfg.Kafka.Topic),
		kafka.WithGroupID(cfg.Kafka.ConsumerGroup),
		kafka.WithMinBytes(cfg.Kafka.MinBytes),
		kafka.WithMaxBytes(cfg.Kafka.MaxBytes),
		kafka.WithMaxWait(cfg.Kafka.MaxWait),
		kafka.WithSASL(cfg.Kafka.Username, cfg.Kafka.Password, cfg.Kafka.SASLMechanism),
	)
}

func NewKafkaProducer(cfg *config.Config) (*kafka.Producer, error) {
	return kafka.NewProducer(
		kafka.WithProducerBrokers(cfg.Kafka.Brokers),
		kafka.WithProducerTopic(cfg.Kafka.Topic),
		kafka.WithProducerSASL(cfg.Kafka.Username, cfg.Kafka.Password, cfg.Kafka.SASLMechanism),
	)
}

func NewKafkaProcessor(
	consumer *kafka.Consumer,
	processor domain.WithdrawalProcessor,
	svc domain.WithdrawalService,
	logger *zap.Logger,
) *infraServices.KafkaProcessor {
	return infraServices.NewKafkaProcessor(
		consumer,
		processor,
		svc,
		infraServices.WithKafkaProcessorLogger(logger),
	)
}

func NewGRPCServer(
	svc domain.WithdrawalService,
	processor domain.WithdrawalProcessor,
	logger *zap.Logger,
) *grpchandler.WithdrawalGRPCServer {
	return grpchandler.NewWithdrawalGRPCServer(
		svc,
		processor,
		grpchandler.WithGRPCLogger(logger),
	)
}

func NewGRPCListener(cfg *config.Config) (net.Listener, error) {
	addr := fmt.Sprintf(":%d", cfg.GRPC.Port)
	return net.Listen(cfg.GRPC.Network, addr)
}

func StartGRPCServer(
	grpcSrv *grpchandler.WithdrawalGRPCServer,
	listener net.Listener,
	logger *zap.Logger,
	lifecycle fx.Lifecycle,
) {
	server := grpc.NewServer()

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			withdrawalv1.RegisterWithdrawalServiceServer(server, grpcSrv)
			withdrawalv1.RegisterWithdrawalProcessorServiceServer(server, grpcSrv)
			reflection.Register(server)

			go func() {
				logger.Info("starting gRPC server",
					zap.String("address", listener.Addr().String()),
				)
				if err := server.Serve(listener); err != nil {
					logger.Error("gRPC server error", zap.Error(err))
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("stopping gRPC server")
			server.GracefulStop()
			return nil
		},
	})
}

func StartProcessor(
	processor domain.WithdrawalProcessor,
	logger *zap.Logger,
	lifecycle fx.Lifecycle,
) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("starting withdrawal processor")
			return processor.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("stopping withdrawal processor")
			return processor.Stop(ctx)
		},
	})
}

func RegisterLifecycle(
	consumer *kafka.Consumer,
	producer *kafka.Producer,
	logger *zap.Logger,
	lifecycle fx.Lifecycle,
) {
	lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("closing kafka connections")
			if err := consumer.Close(); err != nil {
				logger.Error("failed to close consumer", zap.Error(err))
			}
			if err := producer.Close(); err != nil {
				logger.Error("failed to close producer", zap.Error(err))
			}
			return nil
		},
	})
}
