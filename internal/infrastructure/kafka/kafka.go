package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	"go.uber.org/zap"
)

type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	GroupID       string
	MinBytes      int
	MaxBytes      int
	MaxWait       time.Duration
	StartOffset   int64
	Username      string
	Password      string
	SASLMechanism string
}

type ConsumerOption func(*ConsumerConfig)

func WithBrokers(brokers []string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Brokers = brokers
	}
}

func WithTopic(topic string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topic = topic
	}
}

func WithGroupID(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

func WithMinBytes(minBytes int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MinBytes = minBytes
	}
}

func WithMaxBytes(maxBytes int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MaxBytes = maxBytes
	}
}

func WithMaxWait(maxWait time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MaxWait = maxWait
	}
}

func WithStartOffset(offset int64) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.StartOffset = offset
	}
}

func WithSASL(username, password, mechanism string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Username = username
		c.Password = password
		c.SASLMechanism = mechanism
	}
}

func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "withdrawals",
		GroupID:     "withdrawal-processor",
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	}
}

type WithdrawalMessage struct {
	ID              string `json:"id"`
	IdempotencyKey  string `json:"idempotency_key"`
	UserID          string `json:"user_id"`
	Asset           string `json:"asset"`
	Amount          string `json:"amount"`
	DestinationAddr string `json:"destination_address"`
	Network         string `json:"network"`
	MaxRetries      int    `json:"max_retries"`
}

type Consumer struct {
	reader  *kafka.Reader
	logger  *zap.Logger
	config  *ConsumerConfig
	handler MessageHandler
}

type MessageHandler func(ctx context.Context, msg *WithdrawalMessage) error

func newSASLMechanism(username, password, mechanism string) (sasl.Mechanism, error) {
	if username == "" || password == "" {
		return nil, nil
	}

	switch mechanism {
	case "PLAIN", "":
		return plain.Mechanism{
			Username: username,
			Password: password,
		}, nil
	case "SCRAM-SHA-256":
		m, err := scram.Mechanism(scram.SHA256, username, password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-256 mechanism: %w", err)
		}
		return m, nil
	case "SCRAM-SHA-512":
		m, err := scram.Mechanism(scram.SHA512, username, password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %w", err)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

func NewConsumer(opts ...ConsumerOption) (*Consumer, error) {
	config := DefaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		MinBytes:    config.MinBytes,
		MaxBytes:    config.MaxBytes,
		MaxWait:     config.MaxWait,
		StartOffset: config.StartOffset,
	}

	if config.Username != "" && config.Password != "" {
		mechanism, err := newSASLMechanism(config.Username, config.Password, config.SASLMechanism)
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		if mechanism != nil {
			readerConfig.Dialer = &kafka.Dialer{
				SASLMechanism: mechanism,
			}
		}
	}

	reader := kafka.NewReader(readerConfig)

	return &Consumer{
		reader: reader,
		config: config,
		logger: zap.NewNop(),
	}, nil
}

func WithLogger(logger *zap.Logger) func(*Consumer) {
	return func(c *Consumer) {
		c.logger = logger
	}
}

func (c *Consumer) SetHandler(handler MessageHandler) {
	c.handler = handler
}

func (c *Consumer) Consume(ctx context.Context) error {
	if c.handler == nil {
		return fmt.Errorf("message handler not set")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				c.logger.Error("failed to read message", zap.Error(err))
				continue
			}

			var withdrawalMsg WithdrawalMessage
			if err := json.Unmarshal(msg.Value, &withdrawalMsg); err != nil {
				c.logger.Error("failed to unmarshal message",
					zap.Error(err),
					zap.ByteString("raw", msg.Value),
				)
				continue
			}

			c.logger.Debug("processing message",
				zap.String("withdrawal_id", withdrawalMsg.ID),
				zap.String("idempotency_key", withdrawalMsg.IdempotencyKey),
			)

			if err := c.handler(ctx, &withdrawalMsg); err != nil {
				c.logger.Error("failed to handle message",
					zap.Error(err),
					zap.String("withdrawal_id", withdrawalMsg.ID),
				)
				continue
			}

			c.logger.Info("successfully processed withdrawal",
				zap.String("withdrawal_id", withdrawalMsg.ID),
			)
		}
	}
}

func (c *Consumer) Commit(ctx context.Context) error {
	return c.reader.CommitMessages(ctx)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

type ProducerConfig struct {
	Brokers       []string
	Topic         string
	BatchSize     int
	BatchTimeout  time.Duration
	Async         bool
	Username      string
	Password      string
	SASLMechanism string
}

type ProducerOption func(*ProducerConfig)

func WithProducerBrokers(brokers []string) ProducerOption {
	return func(p *ProducerConfig) {
		p.Brokers = brokers
	}
}

func WithProducerTopic(topic string) ProducerOption {
	return func(p *ProducerConfig) {
		p.Topic = topic
	}
}

func WithBatchSize(size int) ProducerOption {
	return func(p *ProducerConfig) {
		p.BatchSize = size
	}
}

func WithBatchTimeout(timeout time.Duration) ProducerOption {
	return func(p *ProducerConfig) {
		p.BatchTimeout = timeout
	}
}

func WithAsync(async bool) ProducerOption {
	return func(p *ProducerConfig) {
		p.Async = async
	}
}

func WithProducerSASL(username, password, mechanism string) ProducerOption {
	return func(p *ProducerConfig) {
		p.Username = username
		p.Password = password
		p.SASLMechanism = mechanism
	}
}

func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "withdrawals",
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
		Async:        false,
	}
}

type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
	config *ProducerConfig
}

func NewProducer(opts ...ProducerOption) (*Producer, error) {
	config := DefaultProducerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		Async:        config.Async,
	}

	if config.Username != "" && config.Password != "" {
		mechanism, err := newSASLMechanism(config.Username, config.Password, config.SASLMechanism)
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		if mechanism != nil {
			writer.Transport = &kafka.Transport{
				SASL: mechanism,
			}
		}
	}

	return &Producer{
		writer: writer,
		config: config,
		logger: zap.NewNop(),
	}, nil
}

func (p *Producer) WithLogger(logger *zap.Logger) *Producer {
	p.logger = logger
	return p
}

func (p *Producer) PublishWithdrawal(ctx context.Context, withdrawal *entities.Withdrawal) error {
	msg := WithdrawalMessage{
		ID:              withdrawal.ID,
		IdempotencyKey:  withdrawal.IdempotencyKey,
		UserID:          withdrawal.UserID,
		Asset:           withdrawal.Asset,
		Amount:          withdrawal.Amount.String(),
		DestinationAddr: withdrawal.DestinationAddr,
		Network:         withdrawal.Network,
		MaxRetries:      withdrawal.MaxRetries,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal withdrawal: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(withdrawal.ID),
		Value: data,
		Headers: []kafka.Header{
			{Key: "idempotency_key", Value: []byte(withdrawal.IdempotencyKey)},
			{Key: "user_id", Value: []byte(withdrawal.UserID)},
			{Key: "event_type", Value: []byte(entities.EventTypeCreated)},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to publish withdrawal: %w", err)
	}

	p.logger.Info("published withdrawal message",
		zap.String("withdrawal_id", withdrawal.ID),
		zap.String("idempotency_key", withdrawal.IdempotencyKey),
	)

	return nil
}

func (p *Producer) PublishEvent(ctx context.Context, event *entities.WithdrawalEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.WithdrawalID),
		Value: data,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "withdrawal_id", Value: []byte(event.WithdrawalID)},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
