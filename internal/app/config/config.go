package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPC        GRPCConfig
	Kafka       KafkaConfig
	Processor   ProcessorConfig
	Idempotency IdempotencyConfig
	Logging     LoggingConfig
	Postgres    PostgresConfig
}

type PostgresConfig struct {
	URL      string
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

type GRPCConfig struct {
	Port    int
	Network string
}

type KafkaConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	MinBytes      int
	MaxBytes      int
	MaxWait       time.Duration
}

type ProcessorConfig struct {
	ID             string
	BatchSize      int
	PollInterval   time.Duration
	LockTTL        int64
	MaxConcurrent  int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

type IdempotencyConfig struct {
	KeyTTL time.Duration
}

type LoggingConfig struct {
	Level  string
	Format string
}

type ConfigOption func(*Config)

func WithGRPCPort(port int) ConfigOption {
	return func(c *Config) {
		c.GRPC.Port = port
	}
}

func WithKafkaBrokers(brokers []string) ConfigOption {
	return func(c *Config) {
		c.Kafka.Brokers = brokers
	}
}

func WithKafkaTopic(topic string) ConfigOption {
	return func(c *Config) {
		c.Kafka.Topic = topic
	}
}

func WithProcessorID(id string) ConfigOption {
	return func(c *Config) {
		c.Processor.ID = id
	}
}

func WithLogLevel(level string) ConfigOption {
	return func(c *Config) {
		c.Logging.Level = level
	}
}

func DefaultConfig() *Config {
	return &Config{
		GRPC: GRPCConfig{
			Port:    50051,
			Network: "tcp",
		},
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			Topic:         "withdrawals",
			ConsumerGroup: "withdrawal-processor",
			MinBytes:      1,
			MaxBytes:      10e6,
			MaxWait:       100 * time.Millisecond,
		},
		Processor: ProcessorConfig{
			ID:             "processor-1",
			BatchSize:      100,
			PollInterval:   100 * time.Millisecond,
			LockTTL:        300,
			MaxConcurrent:  10,
			RetryBaseDelay: 1 * time.Second,
			RetryMaxDelay:  30 * time.Second,
		},
		Idempotency: IdempotencyConfig{
			KeyTTL: 24 * time.Hour,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		Postgres: PostgresConfig{
			URL:      "",
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			Database: "withdrawals",
			SSLMode:  "disable",
		},
	}
}

func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if port := os.Getenv("GRPC_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.GRPC.Port = p
		}
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = []string{brokers}
	}

	if topic := os.Getenv("KAFKA_TOPIC"); topic != "" {
		cfg.Kafka.Topic = topic
	}

	if group := os.Getenv("KAFKA_CONSUMER_GROUP"); group != "" {
		cfg.Kafka.ConsumerGroup = group
	}

	if procID := os.Getenv("PROCESSOR_ID"); procID != "" {
		cfg.Processor.ID = procID
	}

	if batchSize := os.Getenv("PROCESSOR_BATCH_SIZE"); batchSize != "" {
		if b, err := strconv.Atoi(batchSize); err == nil {
			cfg.Processor.BatchSize = b
		}
	}

	if maxConcurrent := os.Getenv("PROCESSOR_MAX_CONCURRENT"); maxConcurrent != "" {
		if m, err := strconv.Atoi(maxConcurrent); err == nil {
			cfg.Processor.MaxConcurrent = m
		}
	}

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		cfg.Logging.Level = logLevel
	}

	if url := os.Getenv("POSTGRES_URL"); url != "" {
		cfg.Postgres.URL = url
	}
	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		cfg.Postgres.Host = host
	}
	if port := os.Getenv("POSTGRES_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.Postgres.Port = p
		}
	}
	if user := os.Getenv("POSTGRES_USER"); user != "" {
		cfg.Postgres.User = user
	}
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		cfg.Postgres.Password = password
	}
	if database := os.Getenv("POSTGRES_DATABASE"); database != "" {
		cfg.Postgres.Database = database
	}
	if sslMode := os.Getenv("POSTGRES_SSL_MODE"); sslMode != "" {
		cfg.Postgres.SSLMode = sslMode
	}

	return cfg
}

func Validate(c *Config) error {
	if c.GRPC.Port <= 0 || c.GRPC.Port > 65535 {
		return fmt.Errorf("invalid grpc port: %d", c.GRPC.Port)
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers are required")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is required")
	}
	if c.Processor.BatchSize <= 0 {
		return fmt.Errorf("processor batch size must be positive")
	}
	if c.Processor.MaxConcurrent <= 0 {
		return fmt.Errorf("processor max concurrent must be positive")
	}
	return nil
}

func (c *PostgresConfig) ConnectionString() string {
	if c.URL != "" {
		return c.URL
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}
