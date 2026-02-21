package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	GRPC        GRPCConfig        `yaml:"grpc"`
	Kafka       KafkaConfig       `yaml:"kafka"`
	Processor   ProcessorConfig   `yaml:"processor"`
	Idempotency IdempotencyConfig `yaml:"idempotency"`
	Logging     LoggingConfig     `yaml:"logging"`
	Postgres    PostgresConfig    `yaml:"postgres"`
}

type PostgresConfig struct {
	URL      string `yaml:"-"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"-"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"ssl_mode"`
}

type GRPCConfig struct {
	Port    int    `yaml:"port"`
	Network string `yaml:"network"`
}

type KafkaConfig struct {
	Brokers       []string      `yaml:"-"`
	Topic         string        `yaml:"topic"`
	ConsumerGroup string        `yaml:"consumer_group"`
	MinBytes      int           `yaml:"min_bytes"`
	MaxBytes      int           `yaml:"max_bytes"`
	MaxWait       time.Duration `yaml:"max_wait"`
}

type ProcessorConfig struct {
	ID             string        `yaml:"id"`
	BatchSize      int           `yaml:"batch_size"`
	PollInterval   time.Duration `yaml:"poll_interval"`
	LockTTL        int64         `yaml:"lock_ttl"`
	MaxConcurrent  int           `yaml:"max_concurrent"`
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	RetryMaxDelay  time.Duration `yaml:"retry_max_delay"`
}

type IdempotencyConfig struct {
	KeyTTL time.Duration `yaml:"key_ttl"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
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

func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	loadSecretsFromEnv(cfg)

	return cfg, nil
}

func loadSecretsFromEnv(cfg *Config) {
	if url := os.Getenv("POSTGRES_URL"); url != "" {
		cfg.Postgres.URL = url
	}
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		cfg.Postgres.Password = password
	}
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = splitBrokers(brokers)
	}
}

func splitBrokers(brokers string) []string {
	if brokers == "" {
		return nil
	}

	// we count the comas to estimate the size of the slice and avoid unnecessary allocations
	count := 1
	for i := range brokers {
		if brokers[i] == ',' {
			count++
		}
	}

	result := make([]string, 0, count)
	start := 0
	for i := 0; i < len(brokers); i++ {
		if brokers[i] == ',' {
			if i > start {
				result = append(result, brokers[start:i])
			}
			start = i + 1
		}
	}
	if start < len(brokers) {
		result = append(result, brokers[start:])
	}

	return result
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
			Password: "",
			Database: "withdrawals",
			SSLMode:  "disable",
		},
	}
}

func LoadFromEnv() *Config {
	cfg, err := Load("")
	if err != nil {
		return DefaultConfig()
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

func GetConfigPath() string {
	if path := os.Getenv("CONFIG_PATH"); path != "" {
		return path
	}
	return "config.yaml"
}

func MustLoad() *Config {
	configPath := GetConfigPath()

	cfg, err := Load(configPath)
	if err != nil {
		cfg = DefaultConfig()
		loadSecretsFromEnv(cfg)
	}

	return cfg
}
