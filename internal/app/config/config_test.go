package config

import (
	"os"
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

func TestLoadFromEnv(t *testing.T) {
	originalValues := map[string]string{
		"GRPC_PORT":                os.Getenv("GRPC_PORT"),
		"KAFKA_BROKERS":            os.Getenv("KAFKA_BROKERS"),
		"KAFKA_TOPIC":              os.Getenv("KAFKA_TOPIC"),
		"KAFKA_CONSUMER_GROUP":     os.Getenv("KAFKA_CONSUMER_GROUP"),
		"PROCESSOR_ID":             os.Getenv("PROCESSOR_ID"),
		"PROCESSOR_BATCH_SIZE":     os.Getenv("PROCESSOR_BATCH_SIZE"),
		"PROCESSOR_MAX_CONCURRENT": os.Getenv("PROCESSOR_MAX_CONCURRENT"),
		"LOG_LEVEL":                os.Getenv("LOG_LEVEL"),
	}

	defer func() {
		for key, value := range originalValues {
			if value == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, value)
			}
		}
	}()

	os.Setenv("GRPC_PORT", "9000")
	os.Setenv("KAFKA_BROKERS", "kafka:9092")
	os.Setenv("KAFKA_TOPIC", "env-topic")
	os.Setenv("KAFKA_CONSUMER_GROUP", "env-group")
	os.Setenv("PROCESSOR_ID", "env-processor")
	os.Setenv("PROCESSOR_BATCH_SIZE", "50")
	os.Setenv("PROCESSOR_MAX_CONCURRENT", "20")
	os.Setenv("LOG_LEVEL", "warn")

	cfg := LoadFromEnv()

	if cfg.GRPC.Port != 9000 {
		t.Errorf("GRPC.Port = %v, want 9000", cfg.GRPC.Port)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "kafka:9092" {
		t.Errorf("Kafka.Brokers = %v, want [kafka:9092]", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "env-topic" {
		t.Errorf("Kafka.Topic = %v, want 'env-topic'", cfg.Kafka.Topic)
	}
	if cfg.Kafka.ConsumerGroup != "env-group" {
		t.Errorf("Kafka.ConsumerGroup = %v, want 'env-group'", cfg.Kafka.ConsumerGroup)
	}
	if cfg.Processor.ID != "env-processor" {
		t.Errorf("Processor.ID = %v, want 'env-processor'", cfg.Processor.ID)
	}
	if cfg.Processor.BatchSize != 50 {
		t.Errorf("Processor.BatchSize = %v, want 50", cfg.Processor.BatchSize)
	}
	if cfg.Processor.MaxConcurrent != 20 {
		t.Errorf("Processor.MaxConcurrent = %v, want 20", cfg.Processor.MaxConcurrent)
	}
	if cfg.Logging.Level != "warn" {
		t.Errorf("Logging.Level = %v, want 'warn'", cfg.Logging.Level)
	}
}

func TestLoadFromEnv_InvalidValues(t *testing.T) {
	originalPort := os.Getenv("GRPC_PORT")
	originalBatchSize := os.Getenv("PROCESSOR_BATCH_SIZE")
	originalMaxConcurrent := os.Getenv("PROCESSOR_MAX_CONCURRENT")

	defer func() {
		if originalPort == "" {
			os.Unsetenv("GRPC_PORT")
		} else {
			os.Setenv("GRPC_PORT", originalPort)
		}
		if originalBatchSize == "" {
			os.Unsetenv("PROCESSOR_BATCH_SIZE")
		} else {
			os.Setenv("PROCESSOR_BATCH_SIZE", originalBatchSize)
		}
		if originalMaxConcurrent == "" {
			os.Unsetenv("PROCESSOR_MAX_CONCURRENT")
		} else {
			os.Setenv("PROCESSOR_MAX_CONCURRENT", originalMaxConcurrent)
		}
	}()

	os.Setenv("GRPC_PORT", "invalid")
	os.Setenv("PROCESSOR_BATCH_SIZE", "invalid")
	os.Setenv("PROCESSOR_MAX_CONCURRENT", "invalid")

	cfg := LoadFromEnv()

	if cfg.GRPC.Port != 50051 {
		t.Errorf("GRPC.Port = %v, want 50051 (default for invalid value)", cfg.GRPC.Port)
	}
	if cfg.Processor.BatchSize != 100 {
		t.Errorf("Processor.BatchSize = %v, want 100 (default for invalid value)", cfg.Processor.BatchSize)
	}
	if cfg.Processor.MaxConcurrent != 10 {
		t.Errorf("Processor.MaxConcurrent = %v, want 10 (default for invalid value)", cfg.Processor.MaxConcurrent)
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
