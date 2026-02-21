package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.GRPC.Port != 50051 {
		t.Errorf("GRPC.Port = %v, want 50051", cfg.GRPC.Port)
	}
	if cfg.GRPC.Network != "tcp" {
		t.Errorf("GRPC.Network = %v, want 'tcp'", cfg.GRPC.Network)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("Kafka.Brokers = %v, want [localhost:9092]", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "withdrawals" {
		t.Errorf("Kafka.Topic = %v, want 'withdrawals'", cfg.Kafka.Topic)
	}
	if cfg.Processor.ID != "processor-1" {
		t.Errorf("Processor.ID = %v, want 'processor-1'", cfg.Processor.ID)
	}
	if cfg.Processor.BatchSize != 100 {
		t.Errorf("Processor.BatchSize = %v, want 100", cfg.Processor.BatchSize)
	}
	if cfg.Idempotency.KeyTTL != 24*time.Hour {
		t.Errorf("Idempotency.KeyTTL = %v, want 24h", cfg.Idempotency.KeyTTL)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("Logging.Level = %v, want 'info'", cfg.Logging.Level)
	}
}

func TestConfigOptions(t *testing.T) {
	cfg := DefaultConfig()

	WithGRPCPort(8080)(cfg)
	WithKafkaBrokers([]string{"broker1:9092", "broker2:9092"})(cfg)
	WithKafkaTopic("custom-topic")(cfg)
	WithProcessorID("custom-processor")(cfg)
	WithLogLevel("debug")(cfg)

	if cfg.GRPC.Port != 8080 {
		t.Errorf("GRPC.Port = %v, want 8080", cfg.GRPC.Port)
	}
	if len(cfg.Kafka.Brokers) != 2 {
		t.Errorf("Kafka.Brokers count = %v, want 2", len(cfg.Kafka.Brokers))
	}
	if cfg.Kafka.Topic != "custom-topic" {
		t.Errorf("Kafka.Topic = %v, want 'custom-topic'", cfg.Kafka.Topic)
	}
	if cfg.Processor.ID != "custom-processor" {
		t.Errorf("Processor.ID = %v, want 'custom-processor'", cfg.Processor.ID)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %v, want 'debug'", cfg.Logging.Level)
	}
}

func TestLoadFromYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
grpc:
  port: 9000
  network: tcp

kafka:
  topic: test-topic
  consumer_group: test-group
  min_bytes: 100
  max_bytes: 5000000
  max_wait: 200ms

processor:
  id: test-processor
  batch_size: 50
  poll_interval: 50ms
  lock_ttl: 600
  max_concurrent: 20
  retry_base_delay: 2s
  retry_max_delay: 60s

idempotency:
  key_ttl: 48h

logging:
  level: debug
  format: console

postgres:
  host: testhost
  port: 5433
  user: testuser
  database: testdb
  ssl_mode: require
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPC.Port != 9000 {
		t.Errorf("GRPC.Port = %v, want 9000", cfg.GRPC.Port)
	}
	if cfg.Kafka.Topic != "test-topic" {
		t.Errorf("Kafka.Topic = %v, want 'test-topic'", cfg.Kafka.Topic)
	}
	if cfg.Kafka.ConsumerGroup != "test-group" {
		t.Errorf("Kafka.ConsumerGroup = %v, want 'test-group'", cfg.Kafka.ConsumerGroup)
	}
	if cfg.Processor.ID != "test-processor" {
		t.Errorf("Processor.ID = %v, want 'test-processor'", cfg.Processor.ID)
	}
	if cfg.Processor.BatchSize != 50 {
		t.Errorf("Processor.BatchSize = %v, want 50", cfg.Processor.BatchSize)
	}
	if cfg.Processor.MaxConcurrent != 20 {
		t.Errorf("Processor.MaxConcurrent = %v, want 20", cfg.Processor.MaxConcurrent)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %v, want 'debug'", cfg.Logging.Level)
	}
	if cfg.Postgres.Host != "testhost" {
		t.Errorf("Postgres.Host = %v, want 'testhost'", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 5433 {
		t.Errorf("Postgres.Port = %v, want 5433", cfg.Postgres.Port)
	}
	if cfg.Idempotency.KeyTTL != 48*time.Hour {
		t.Errorf("Idempotency.KeyTTL = %v, want 48h", cfg.Idempotency.KeyTTL)
	}
}

func TestLoadSecretsFromEnv(t *testing.T) {
	originalPassword := os.Getenv("POSTGRES_PASSWORD")
	originalBrokers := os.Getenv("KAFKA_BROKERS")
	originalURL := os.Getenv("POSTGRES_URL")

	defer func() {
		if originalPassword == "" {
			os.Unsetenv("POSTGRES_PASSWORD")
		} else {
			os.Setenv("POSTGRES_PASSWORD", originalPassword)
		}
		if originalBrokers == "" {
			os.Unsetenv("KAFKA_BROKERS")
		} else {
			os.Setenv("KAFKA_BROKERS", originalBrokers)
		}
		if originalURL == "" {
			os.Unsetenv("POSTGRES_URL")
		} else {
			os.Setenv("POSTGRES_URL", originalURL)
		}
	}()

	os.Setenv("POSTGRES_PASSWORD", "secret-password")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	os.Setenv("POSTGRES_URL", "postgres://user:pass@host:5432/db")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Postgres.Password != "secret-password" {
		t.Errorf("Postgres.Password = %v, want 'secret-password'", cfg.Postgres.Password)
	}
	if len(cfg.Kafka.Brokers) != 2 {
		t.Errorf("Kafka.Brokers count = %v, want 2", len(cfg.Kafka.Brokers))
	}
	if cfg.Kafka.Brokers[0] != "broker1:9092" || cfg.Kafka.Brokers[1] != "broker2:9092" {
		t.Errorf("Kafka.Brokers = %v, want [broker1:9092, broker2:9092]", cfg.Kafka.Brokers)
	}
	if cfg.Postgres.URL != "postgres://user:pass@host:5432/db" {
		t.Errorf("Postgres.URL = %v, want 'postgres://user:pass@host:5432/db'", cfg.Postgres.URL)
	}
}

func TestLoadFromEnv(t *testing.T) {
	originalPassword := os.Getenv("POSTGRES_PASSWORD")
	originalBrokers := os.Getenv("KAFKA_BROKERS")

	defer func() {
		if originalPassword == "" {
			os.Unsetenv("POSTGRES_PASSWORD")
		} else {
			os.Setenv("POSTGRES_PASSWORD", originalPassword)
		}
		if originalBrokers == "" {
			os.Unsetenv("KAFKA_BROKERS")
		} else {
			os.Setenv("KAFKA_BROKERS", originalBrokers)
		}
	}()

	os.Setenv("POSTGRES_PASSWORD", "env-password")
	os.Setenv("KAFKA_BROKERS", "env-broker:9092")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.Postgres.Password != "env-password" {
		t.Errorf("Postgres.Password = %v, want 'env-password'", cfg.Postgres.Password)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "env-broker:9092" {
		t.Errorf("Kafka.Brokers = %v, want [env-broker:9092]", cfg.Kafka.Brokers)
	}
}

func TestSplitBrokers(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"localhost:9092", []string{"localhost:9092"}},
		{"broker1:9092,broker2:9092", []string{"broker1:9092", "broker2:9092"}},
		{"broker1:9092,broker2:9092,broker3:9092", []string{"broker1:9092", "broker2:9092", "broker3:9092"}},
		{"", []string{}},
		{",,", []string{}},
	}

	for _, tt := range tests {
		result := splitBrokers(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("splitBrokers(%q) = %v, want %v", tt.input, result, tt.expected)
			continue
		}
		for i, v := range result {
			if v != tt.expected[i] {
				t.Errorf("splitBrokers(%q)[%d] = %v, want %v", tt.input, i, v, tt.expected[i])
			}
		}
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "invalid grpc port - zero",
			config: &Config{
				GRPC:      GRPCConfig{Port: 0},
				Kafka:     KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Processor: ProcessorConfig{BatchSize: 10, MaxConcurrent: 5},
			},
			wantErr: true,
		},
		{
			name: "invalid grpc port - negative",
			config: &Config{
				GRPC:      GRPCConfig{Port: -1},
				Kafka:     KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Processor: ProcessorConfig{BatchSize: 10, MaxConcurrent: 5},
			},
			wantErr: true,
		},
		{
			name: "invalid grpc port - too high",
			config: &Config{
				GRPC:      GRPCConfig{Port: 70000},
				Kafka:     KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Processor: ProcessorConfig{BatchSize: 10, MaxConcurrent: 5},
			},
			wantErr: true,
		},
		{
			name: "missing kafka brokers",
			config: &Config{
				GRPC:      GRPCConfig{Port: 50051},
				Kafka:     KafkaConfig{Brokers: []string{}, Topic: "test"},
				Processor: ProcessorConfig{BatchSize: 10, MaxConcurrent: 5},
			},
			wantErr: true,
		},
		{
			name: "missing kafka topic",
			config: &Config{
				GRPC:      GRPCConfig{Port: 50051},
				Kafka:     KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: ""},
				Processor: ProcessorConfig{BatchSize: 10, MaxConcurrent: 5},
			},
			wantErr: true,
		},
		{
			name: "invalid processor batch size",
			config: &Config{
				GRPC:      GRPCConfig{Port: 50051},
				Kafka:     KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Processor: ProcessorConfig{BatchSize: 0, MaxConcurrent: 5},
			},
			wantErr: true,
		},
		{
			name: "invalid processor max concurrent",
			config: &Config{
				GRPC:      GRPCConfig{Port: 50051},
				Kafka:     KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Processor: ProcessorConfig{BatchSize: 10, MaxConcurrent: 0},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPostgresConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   *PostgresConfig
		expected string
	}{
		{
			name: "using URL",
			config: &PostgresConfig{
				URL:      "postgres://custom:pass@customhost:5433/customdb",
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Password: "postgres",
				Database: "withdrawals",
				SSLMode:  "disable",
			},
			expected: "postgres://custom:pass@customhost:5433/customdb",
		},
		{
			name: "using individual fields",
			config: &PostgresConfig{
				URL:      "",
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Password: "secret",
				Database: "withdrawals",
				SSLMode:  "disable",
			},
			expected: "postgres://postgres:secret@localhost:5432/withdrawals?sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.ConnectionString()
			if result != tt.expected {
				t.Errorf("ConnectionString() = %v, want %v", result, tt.expected)
			}
		})
	}
}
