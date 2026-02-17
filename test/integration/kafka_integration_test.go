package integration

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/infrastructure/kafka"
	"go.uber.org/zap"
)

func TestKafkaIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := kafkamodule.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %v", err)
	}

	topic := "test-withdrawals"

	producer, err := kafka.NewProducer(
		kafka.WithProducerBrokers(brokers),
		kafka.WithProducerTopic(topic),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	logger := zap.NewNop()
	producer = producer.WithLogger(logger)

	t.Run("publish and consume withdrawal message", func(t *testing.T) {
		withdrawal := &entities.Withdrawal{
			ID:              "test-withdrawal-1",
			IdempotencyKey:  "test-key-1",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			MaxRetries:      3,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}

		err := producer.PublishWithdrawal(ctx, withdrawal)
		if err != nil {
			t.Fatalf("failed to publish withdrawal: %v", err)
		}

		received := make(chan *kafka.WithdrawalMessage, 1)
		errors := make(chan error, 1)

		consumer, err := kafka.NewConsumer(
			kafka.WithBrokers(brokers),
			kafka.WithTopic(topic),
			kafka.WithGroupID("test-consumer-group"),
			kafka.WithStartOffset(-2),
		)
		if err != nil {
			t.Fatalf("failed to create consumer: %v", err)
		}
		defer consumer.Close()

		consumer.SetHandler(func(ctx context.Context, msg *kafka.WithdrawalMessage) error {
			received <- msg
			return nil
		})

		consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		go func() {
			if err := consumer.Consume(consumeCtx); err != nil && err != context.DeadlineExceeded {
				errors <- err
			}
		}()

		select {
		case msg := <-received:
			if msg.ID != withdrawal.ID {
				t.Errorf("ID = %v, want %v", msg.ID, withdrawal.ID)
			}
			if msg.IdempotencyKey != withdrawal.IdempotencyKey {
				t.Errorf("IdempotencyKey = %v, want %v", msg.IdempotencyKey, withdrawal.IdempotencyKey)
			}
			if msg.UserID != withdrawal.UserID {
				t.Errorf("UserID = %v, want %v", msg.UserID, withdrawal.UserID)
			}
			if msg.Amount != withdrawal.Amount.String() {
				t.Errorf("Amount = %v, want %v", msg.Amount, withdrawal.Amount.String())
			}

		case err := <-errors:
			t.Fatalf("consumer error: %v", err)

		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	t.Run("publish and consume event", func(t *testing.T) {
		event := &entities.WithdrawalEvent{
			WithdrawalID: "test-withdrawal-2",
			EventType:    entities.EventTypeCreated,
			Status:       entities.StatusPending,
			Timestamp:    time.Now().UTC(),
			Metadata: map[string]string{
				"source": "test",
			},
		}

		err := producer.PublishEvent(ctx, event)
		if err != nil {
			t.Fatalf("failed to publish event: %v", err)
		}
	})
}

func TestKafkaProducer_InvalidConfig(t *testing.T) {
	t.Run("returns error for empty brokers", func(t *testing.T) {
		_, err := kafka.NewProducer(
			kafka.WithProducerBrokers([]string{}),
			kafka.WithProducerTopic("test"),
		)
		if err == nil {
			t.Error("expected error for empty brokers")
		}
	})

	t.Run("returns error for empty topic", func(t *testing.T) {
		_, err := kafka.NewProducer(
			kafka.WithProducerBrokers([]string{"localhost:9092"}),
			kafka.WithProducerTopic(""),
		)
		if err == nil {
			t.Error("expected error for empty topic")
		}
	})
}

func TestKafkaConsumer_InvalidConfig(t *testing.T) {
	t.Run("returns error for empty brokers", func(t *testing.T) {
		_, err := kafka.NewConsumer(
			kafka.WithBrokers([]string{}),
			kafka.WithTopic("test"),
		)
		if err == nil {
			t.Error("expected error for empty brokers")
		}
	})

	t.Run("returns error for empty topic", func(t *testing.T) {
		_, err := kafka.NewConsumer(
			kafka.WithBrokers([]string{"localhost:9092"}),
			kafka.WithTopic(""),
		)
		if err == nil {
			t.Error("expected error for empty topic")
		}
	})
}

func TestWithdrawalMessage_Serialization(t *testing.T) {
	msg := &kafka.WithdrawalMessage{
		ID:              "test-id",
		IdempotencyKey:  "test-key",
		UserID:          "user-1",
		Asset:           "BTC",
		Amount:          "100000000",
		DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		Network:         "bitcoin",
		MaxRetries:      3,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	var decoded kafka.WithdrawalMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if decoded.ID != msg.ID {
		t.Errorf("ID = %v, want %v", decoded.ID, msg.ID)
	}
	if decoded.IdempotencyKey != msg.IdempotencyKey {
		t.Errorf("IdempotencyKey = %v, want %v", decoded.IdempotencyKey, msg.IdempotencyKey)
	}
	if decoded.Amount != msg.Amount {
		t.Errorf("Amount = %v, want %v", decoded.Amount, msg.Amount)
	}
}

func TestConsumerConfig_Defaults(t *testing.T) {
	config := kafka.DefaultConsumerConfig()

	if len(config.Brokers) != 1 || config.Brokers[0] != "localhost:9092" {
		t.Errorf("default brokers = %v, want [localhost:9092]", config.Brokers)
	}
	if config.Topic != "withdrawals" {
		t.Errorf("default topic = %v, want 'withdrawals'", config.Topic)
	}
	if config.GroupID != "withdrawal-processor" {
		t.Errorf("default group ID = %v, want 'withdrawal-processor'", config.GroupID)
	}
	if config.MinBytes != 1 {
		t.Errorf("default min bytes = %v, want 1", config.MinBytes)
	}
	if config.MaxBytes != 10e6 {
		t.Errorf("default max bytes = %v, want 10e6", config.MaxBytes)
	}
	if config.MaxWait != 100*time.Millisecond {
		t.Errorf("default max wait = %v, want 100ms", config.MaxWait)
	}
}

func TestProducerConfig_Defaults(t *testing.T) {
	config := kafka.DefaultProducerConfig()

	if len(config.Brokers) != 1 || config.Brokers[0] != "localhost:9092" {
		t.Errorf("default brokers = %v, want [localhost:9092]", config.Brokers)
	}
	if config.Topic != "withdrawals" {
		t.Errorf("default topic = %v, want 'withdrawals'", config.Topic)
	}
	if config.BatchSize != 100 {
		t.Errorf("default batch size = %v, want 100", config.BatchSize)
	}
	if config.BatchTimeout != 100*time.Millisecond {
		t.Errorf("default batch timeout = %v, want 100ms", config.BatchTimeout)
	}
	if config.Async != false {
		t.Error("default async should be false")
	}
}

func TestConsumerConfig_Options(t *testing.T) {
	config := kafka.DefaultConsumerConfig()

	kafka.WithBrokers([]string{"broker1:9092", "broker2:9092"})(config)
	kafka.WithTopic("custom-topic")(config)
	kafka.WithGroupID("custom-group")(config)
	kafka.WithMinBytes(100)(config)
	kafka.WithMaxBytes(1000000)(config)
	kafka.WithMaxWait(500 * time.Millisecond)(config)
	kafka.WithStartOffset(0)(config)

	if len(config.Brokers) != 2 {
		t.Errorf("brokers count = %v, want 2", len(config.Brokers))
	}
	if config.Topic != "custom-topic" {
		t.Errorf("topic = %v, want 'custom-topic'", config.Topic)
	}
	if config.GroupID != "custom-group" {
		t.Errorf("group ID = %v, want 'custom-group'", config.GroupID)
	}
	if config.MinBytes != 100 {
		t.Errorf("min bytes = %v, want 100", config.MinBytes)
	}
	if config.MaxBytes != 1000000 {
		t.Errorf("max bytes = %v, want 1000000", config.MaxBytes)
	}
	if config.MaxWait != 500*time.Millisecond {
		t.Errorf("max wait = %v, want 500ms", config.MaxWait)
	}
	if config.StartOffset != 0 {
		t.Errorf("start offset = %v, want 0", config.StartOffset)
	}
}

func TestProducerConfig_Options(t *testing.T) {
	config := kafka.DefaultProducerConfig()

	kafka.WithProducerBrokers([]string{"broker1:9092"})(config)
	kafka.WithProducerTopic("custom-topic")(config)
	kafka.WithBatchSize(50)(config)
	kafka.WithBatchTimeout(200 * time.Millisecond)(config)
	kafka.WithAsync(true)(config)

	if len(config.Brokers) != 1 || config.Brokers[0] != "broker1:9092" {
		t.Errorf("brokers = %v, want [broker1:9092]", config.Brokers)
	}
	if config.Topic != "custom-topic" {
		t.Errorf("topic = %v, want 'custom-topic'", config.Topic)
	}
	if config.BatchSize != 50 {
		t.Errorf("batch size = %v, want 50", config.BatchSize)
	}
	if config.BatchTimeout != 200*time.Millisecond {
		t.Errorf("batch timeout = %v, want 200ms", config.BatchTimeout)
	}
	if config.Async != true {
		t.Error("async should be true")
	}
}
