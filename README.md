# Idempotent Withdrawal Processor

A production-grade, distributed withdrawal processing system built in Go with idempotency guarantees, automatic retries, and blockchain transaction broadcasting.

## Features

- **Idempotent Operations**: Safely retry withdrawal requests without duplicate processing
- **Distributed Processing**: Multiple processor instances with database-level locking
- **Automatic Retries**: Exponential backoff retry mechanism with configurable limits
- **gRPC API**: Type-safe protobuf-based service definitions
- **Event-Driven Architecture**: Kafka integration for async processing
- **Clean Architecture**: Domain-driven design with clear separation of concerns
- **Type-Safe SQL**: Generated database code using sqlc
- **Observability**: Structured logging with zap

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         gRPC API Layer                          │
│                    (WithdrawalService)                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                      Application Layer                          │
│              (WithdrawalService, Processor)                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                       Domain Layer                              │
│        (Entities, Repository Interfaces, Service Interfaces)    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   Infrastructure Layer                          │
│    (PostgreSQL, Kafka, Blockchain Service, Idempotency)         │
└─────────────────────────────────────────────────────────────────┘
```

## Withdrawal State Machine

```
                    ┌──────────┐
                    │ PENDING  │
                    └────┬─────┘
                         │
                         ▼
                  ┌────────────┐
          ┌──────▶│ PROCESSING │◀──────┐
          │       └──────┬─────┘       │
          │              │             │
          │       ┌──────▼──────┐      │
          │       │  COMPLETED  │      │
          │       └─────────────┘      │
          │                            │
    ┌─────▼─────┐              ┌───────┴───┐
    │  RETRYING │─────────────▶│   FAILED  │
    └───────────┘              └───────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.25+ |
| API | gRPC + Protobuf |
| Database | PostgreSQL |
| Message Queue | Apache Kafka |
| SQL Generation | sqlc |
| DI Framework | Uber fx |
| Logging | Uber zap |
| Testing | testcontainers-go |

## Project Structure

```
.
├── cmd/
│   └── processor/           # Application entrypoint
│       └── main.go
├── internal/
│   ├── app/
│   │   ├── config/          # Configuration management
│   │   └── di/              # Dependency injection (fx)
│   ├── domain/
│   │   ├── entities/        # Domain entities
│   │   ├── repositories/    # Repository interfaces
│   │   └── services/        # Service interfaces
│   ├── infrastructure/
│   │   ├── kafka/           # Kafka consumer/producer
│   │   ├── persistence/     # PostgreSQL repositories
│   │   │   └── postgres/
│   │   │       ├── db/      # sqlc generated code
│   │   │       ├── migrations/
│   │   │       └── queries/
│   │   └── services/        # Domain service implementations
│   └── transport/
│       └── grpc/            # gRPC server implementation
├── proto/
│   └── withdrawal/v1/       # Protobuf definitions
├── gen/
│   └── withdrawal/v1/       # Generated Go code
├── test/
│   └── integration/         # Integration tests
├── sqlc.yaml
├── buf.yaml
└── buf.gen.yaml
```

## Quick Start

### Prerequisites

- Go 1.25+
- PostgreSQL 15+
- Apache Kafka 3.0+
- Docker (for integration tests)

### Environment Variables

```bash
# gRPC
GRPC_PORT=50051

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=withdrawals
KAFKA_CONSUMER_GROUP=withdrawal-processor

# Processor
PROCESSOR_ID=processor-1
PROCESSOR_BATCH_SIZE=100
PROCESSOR_MAX_CONCURRENT=10

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DATABASE=withdrawals
POSTGRES_SSL_MODE=disable

# Logging
LOG_LEVEL=info
```

### Run Database Migrations

```bash
psql -d withdrawals -f internal/infrastructure/persistence/postgres/migrations/001_init_schema.sql
```

### Generate Code

```bash
# Generate protobuf code
buf generate

# Generate sqlc code
sqlc generate
```

### Build & Run

```bash
go build -o processor ./cmd/processor
./processor
```

## gRPC API

### WithdrawalService

| Method | Description |
|--------|-------------|
| `CreateWithdrawal` | Create a new withdrawal (idempotent) |
| `GetWithdrawal` | Get withdrawal by ID |
| `GetWithdrawalByIdempotencyKey` | Get withdrawal by idempotency key |
| `ListWithdrawals` | List withdrawals with pagination |
| `RetryWithdrawal` | Retry a failed withdrawal |

### WithdrawalProcessorService

| Method | Description |
|--------|-------------|
| `StartProcessor` | Start the background processor |
| `StopProcessor` | Stop the background processor |
| `GetProcessorStats` | Get processor statistics |

### Example: Create Withdrawal

```go
import pb "github.com/yaninyzwitty/idempotent-widthrawal-processor/gen/withdrawal/v1"

// Create idempotent withdrawal
resp, err := client.CreateWithdrawal(ctx, &pb.CreateWithdrawalRequest{
    IdempotencyKey:     "txn-abc123-v1",
    UserId:             "user-456",
    Asset:              "BTC",
    Amount:             "100000",  // satoshis
    DestinationAddress: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    Network:            "bitcoin",
    MaxRetries:         3,
})

if resp.AlreadyExists {
    // Withdrawal already processed - return cached result
    return resp.Withdrawal, nil
}
```

## Idempotency

Every withdrawal requires an `idempotency_key`. If a request with the same key is received:

1. **Within TTL** (default 24h): Returns the original withdrawal
2. **After TTL**: Creates a new withdrawal

```sql
-- Idempotency keys table
CREATE TABLE idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    withdrawal_id VARCHAR(36),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

## Distributed Processing

Multiple processor instances can run concurrently. Processing locks prevent duplicate work:

```sql
CREATE TABLE processing_locks (
    withdrawal_id VARCHAR(36) PRIMARY KEY,
    locked_by VARCHAR(36),
    locked_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

Lock acquisition is atomic - only one processor can acquire a lock for a withdrawal.

## Retry Strategy

Failed withdrawals automatically retry with exponential backoff:

```go
config := ProcessorConfig{
    RetryBaseDelay:  1 * time.Second,
    RetryMaxDelay:   30 * time.Second,
    RetryMultiplier: 2.0,
}
// Delays: 1s, 2s, 4s, 8s, 16s, 30s...
```

## Testing

### Unit Tests

```bash
go test ./... -v
```

### Integration Tests

```bash
go test ./test/integration/... -v
```

Integration tests use testcontainers to spin up PostgreSQL and Kafka.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `GRPC_PORT` | 50051 | gRPC server port |
| `KAFKA_BROKERS` | localhost:9092 | Kafka broker addresses |
| `KAFKA_TOPIC` | withdrawals | Kafka topic name |
| `PROCESSOR_BATCH_SIZE` | 100 | Max withdrawals per batch |
| `PROCESSOR_MAX_CONCURRENT` | 10 | Max concurrent processing |
| `PROCESSOR_LOCK_TTL` | 300 | Lock TTL in seconds |
| `IDEMPOTENCY_KEY_TTL` | 24h | Idempotency key expiration |

## License

MIT
