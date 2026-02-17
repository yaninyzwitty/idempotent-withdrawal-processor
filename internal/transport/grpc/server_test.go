package grpc

import (
	"context"
	"math/big"
	"testing"
	"time"

	withdrawalv1 "github.com/yaninyzwitty/idempotent-widthrawal-processor/gen/withdrawal/v1"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	svcmocks "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services/mocks"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWithdrawalGRPCServer_CreateWithdrawal(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	tests := []struct {
		name             string
		req              *withdrawalv1.CreateWithdrawalRequest
		setupMock        func(*svcmocks.MockWithdrawalService)
		wantErr          bool
		wantCode         codes.Code
		wantAlreadyExist bool
	}{
		{
			name: "creates withdrawal successfully",
			req: &withdrawalv1.CreateWithdrawalRequest{
				IdempotencyKey:     "test-key",
				UserId:             "user-1",
				Asset:              "BTC",
				Amount:             "100000000",
				DestinationAddress: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
				Network:            "bitcoin",
				MaxRetries:         3,
			},
			setupMock: func(m *svcmocks.MockWithdrawalService) {
				m.CreateWithdrawalFunc = func(ctx context.Context, req *domain.CreateWithdrawalRequest) (*entities.Withdrawal, bool, error) {
					return &entities.Withdrawal{
						ID:              "withdrawal-1",
						IdempotencyKey:  "test-key",
						UserID:          "user-1",
						Asset:           "BTC",
						Amount:          big.NewInt(100000000),
						DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
						Network:         "bitcoin",
						Status:          entities.StatusPending,
						CreatedAt:       time.Now(),
						UpdatedAt:       time.Now(),
					}, false, nil
				}
			},
			wantErr: false,
		},
		{
			name: "returns existing withdrawal",
			req: &withdrawalv1.CreateWithdrawalRequest{
				IdempotencyKey:     "existing-key",
				UserId:             "user-1",
				Asset:              "BTC",
				Amount:             "100000000",
				DestinationAddress: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
				Network:            "bitcoin",
			},
			setupMock: func(m *svcmocks.MockWithdrawalService) {
				m.CreateWithdrawalFunc = func(ctx context.Context, req *domain.CreateWithdrawalRequest) (*entities.Withdrawal, bool, error) {
					return &entities.Withdrawal{
						ID:              "withdrawal-1",
						IdempotencyKey:  "existing-key",
						UserID:          "user-1",
						Status:          entities.StatusCompleted,
						Amount:          big.NewInt(100000000),
						DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
						CreatedAt:       time.Now(),
						UpdatedAt:       time.Now(),
					}, true, nil
				}
			},
			wantErr:          false,
			wantAlreadyExist: true,
		},
		{
			name: "returns error for missing idempotency key",
			req: &withdrawalv1.CreateWithdrawalRequest{
				UserId:             "user-1",
				Amount:             "100000000",
				DestinationAddress: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			},
			setupMock: func(m *svcmocks.MockWithdrawalService) {},
			wantErr:   true,
			wantCode:  codes.InvalidArgument,
		},
		{
			name: "returns error for missing user ID",
			req: &withdrawalv1.CreateWithdrawalRequest{
				IdempotencyKey:     "test-key",
				Amount:             "100000000",
				DestinationAddress: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			},
			setupMock: func(m *svcmocks.MockWithdrawalService) {},
			wantErr:   true,
			wantCode:  codes.InvalidArgument,
		},
		{
			name: "returns error for missing amount",
			req: &withdrawalv1.CreateWithdrawalRequest{
				IdempotencyKey:     "test-key",
				UserId:             "user-1",
				DestinationAddress: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			},
			setupMock: func(m *svcmocks.MockWithdrawalService) {},
			wantErr:   true,
			wantCode:  codes.InvalidArgument,
		},
		{
			name: "returns error for missing destination address",
			req: &withdrawalv1.CreateWithdrawalRequest{
				IdempotencyKey: "test-key",
				UserId:         "user-1",
				Amount:         "100000000",
			},
			setupMock: func(m *svcmocks.MockWithdrawalService) {},
			wantErr:   true,
			wantCode:  codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockService := &svcmocks.MockWithdrawalService{}
			tt.setupMock(mockService)
			mockProcessor := &svcmocks.MockWithdrawalProcessor{}

			server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

			resp, err := server.CreateWithdrawal(ctx, tt.req)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
					return
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Errorf("expected gRPC status error, got %v", err)
					return
				}
				if st.Code() != tt.wantCode {
					t.Errorf("error code = %v, want %v", st.Code(), tt.wantCode)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if resp == nil {
				t.Fatal("response should not be nil")
			}

			if resp.AlreadyExists != tt.wantAlreadyExist {
				t.Errorf("AlreadyExists = %v, want %v", resp.AlreadyExists, tt.wantAlreadyExist)
			}

			if resp.Withdrawal == nil {
				t.Error("withdrawal should not be nil")
			}
		})
	}
}

func TestWithdrawalGRPCServer_GetWithdrawal(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("returns withdrawal successfully", func(t *testing.T) {
		expectedWithdrawal := &entities.Withdrawal{
			ID:              "withdrawal-1",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Asset:           "BTC",
			Amount:          big.NewInt(100000000),
			DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
			Network:         "bitcoin",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}

		mockService := &svcmocks.MockWithdrawalService{
			GetWithdrawalFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				if id == "withdrawal-1" {
					return expectedWithdrawal, nil
				}
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		resp, err := server.GetWithdrawal(ctx, &withdrawalv1.GetWithdrawalRequest{Id: "withdrawal-1"})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if resp.Withdrawal.Id != "withdrawal-1" {
			t.Errorf("Withdrawal.Id = %v, want 'withdrawal-1'", resp.Withdrawal.Id)
		}
	})

	t.Run("returns error for missing ID", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.GetWithdrawal(ctx, &withdrawalv1.GetWithdrawalRequest{Id: ""})

		if err == nil {
			t.Error("expected error for missing ID")
			return
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error, got %v", err)
		}
	})

	t.Run("returns not found for non-existent withdrawal", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{
			GetWithdrawalFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.GetWithdrawal(ctx, &withdrawalv1.GetWithdrawalRequest{Id: "non-existent"})

		if err == nil {
			t.Error("expected error for non-existent withdrawal")
			return
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.NotFound {
			t.Errorf("expected NotFound error, got %v", err)
		}
	})
}

func TestWithdrawalGRPCServer_GetWithdrawalByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("returns withdrawal when found", func(t *testing.T) {
		expectedWithdrawal := &entities.Withdrawal{
			ID:              "withdrawal-1",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Amount:          big.NewInt(100000000),
			DestinationAddr: "addr",
			Status:          entities.StatusPending,
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}

		mockService := &svcmocks.MockWithdrawalService{
			GetWithdrawalByIdempotencyKeyFunc: func(ctx context.Context, key string) (*entities.Withdrawal, error) {
				if key == "test-key" {
					return expectedWithdrawal, nil
				}
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		resp, err := server.GetWithdrawalByIdempotencyKey(ctx, &withdrawalv1.GetWithdrawalByIdempotencyKeyRequest{IdempotencyKey: "test-key"})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if !resp.Found {
			t.Error("expected Found to be true")
		}
		if resp.Withdrawal == nil {
			t.Error("withdrawal should not be nil")
		}
	})

	t.Run("returns not found when key not exists", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{
			GetWithdrawalByIdempotencyKeyFunc: func(ctx context.Context, key string) (*entities.Withdrawal, error) {
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		resp, err := server.GetWithdrawalByIdempotencyKey(ctx, &withdrawalv1.GetWithdrawalByIdempotencyKeyRequest{IdempotencyKey: "non-existent"})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if resp.Found {
			t.Error("expected Found to be false")
		}
		if resp.Withdrawal != nil {
			t.Error("withdrawal should be nil when not found")
		}
	})
}

func TestWithdrawalGRPCServer_ListWithdrawals(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("lists withdrawals successfully", func(t *testing.T) {
		expectedWithdrawals := []*entities.Withdrawal{
			{
				ID:              "w1",
				IdempotencyKey:  "k1",
				UserID:          "user-1",
				Amount:          big.NewInt(100),
				DestinationAddr: "addr1",
				Status:          entities.StatusPending,
				CreatedAt:       time.Now(),
				UpdatedAt:       time.Now(),
			},
			{
				ID:              "w2",
				IdempotencyKey:  "k2",
				UserID:          "user-1",
				Amount:          big.NewInt(200),
				DestinationAddr: "addr2",
				Status:          entities.StatusPending,
				CreatedAt:       time.Now(),
				UpdatedAt:       time.Now(),
			},
		}

		mockService := &svcmocks.MockWithdrawalService{
			ListWithdrawalsFunc: func(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error) {
				return expectedWithdrawals, nil
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		resp, err := server.ListWithdrawals(ctx, &withdrawalv1.ListWithdrawalsRequest{
			UserId:   "user-1",
			PageSize: 10,
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if len(resp.Withdrawals) != 2 {
			t.Errorf("len(Withdrawals) = %v, want 2", len(resp.Withdrawals))
		}
	})

	t.Run("uses default page size when not specified", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{
			ListWithdrawalsFunc: func(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error) {
				if limit != 50 {
					t.Errorf("limit = %v, want 50 (default)", limit)
				}
				return []*entities.Withdrawal{}, nil
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.ListWithdrawals(ctx, &withdrawalv1.ListWithdrawalsRequest{})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("caps page size at 100", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{
			ListWithdrawalsFunc: func(ctx context.Context, userID string, status entities.WithdrawalStatus, limit, offset int) ([]*entities.Withdrawal, error) {
				if limit != 100 {
					t.Errorf("limit = %v, want 100 (max)", limit)
				}
				return []*entities.Withdrawal{}, nil
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.ListWithdrawals(ctx, &withdrawalv1.ListWithdrawalsRequest{PageSize: 200})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestWithdrawalGRPCServer_RetryWithdrawal(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("retries withdrawal successfully", func(t *testing.T) {
		expectedWithdrawal := &entities.Withdrawal{
			ID:              "withdrawal-1",
			IdempotencyKey:  "test-key",
			UserID:          "user-1",
			Amount:          big.NewInt(100000000),
			DestinationAddr: "addr",
			Status:          entities.StatusRetrying,
			RetryCount:      1,
			MaxRetries:      3,
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}

		mockService := &svcmocks.MockWithdrawalService{
			RetryWithdrawalFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return expectedWithdrawal, nil
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		resp, err := server.RetryWithdrawal(ctx, &withdrawalv1.RetryWithdrawalRequest{Id: "withdrawal-1"})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if resp.Withdrawal.Status != withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_RETRYING {
			t.Errorf("Status = %v, want RETRYING", resp.Withdrawal.Status)
		}
	})

	t.Run("returns error for missing ID", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.RetryWithdrawal(ctx, &withdrawalv1.RetryWithdrawalRequest{Id: ""})

		if err == nil {
			t.Error("expected error for missing ID")
			return
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error, got %v", err)
		}
	})

	t.Run("returns not found for non-existent withdrawal", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{
			RetryWithdrawalFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return nil, entities.ErrWithdrawalNotFound
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.RetryWithdrawal(ctx, &withdrawalv1.RetryWithdrawalRequest{Id: "non-existent"})

		if err == nil {
			t.Error("expected error for non-existent withdrawal")
			return
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.NotFound {
			t.Errorf("expected NotFound error, got %v", err)
		}
	})

	t.Run("returns failed precondition when max retries exceeded", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{
			RetryWithdrawalFunc: func(ctx context.Context, id string) (*entities.Withdrawal, error) {
				return nil, entities.ErrMaxRetriesExceeded
			},
		}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.RetryWithdrawal(ctx, &withdrawalv1.RetryWithdrawalRequest{Id: "withdrawal-1"})

		if err == nil {
			t.Error("expected error when max retries exceeded")
			return
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.FailedPrecondition {
			t.Errorf("expected FailedPrecondition error, got %v", err)
		}
	})
}

func TestWithdrawalGRPCServer_ProcessorControl(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	t.Run("starts processor successfully", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{
			StartFunc: func(ctx context.Context) error {
				return nil
			},
		}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.StartProcessor(ctx, nil)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("stops processor successfully", func(t *testing.T) {
		mockService := &svcmocks.MockWithdrawalService{}
		mockProcessor := &svcmocks.MockWithdrawalProcessor{
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}

		server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

		_, err := server.StopProcessor(ctx, nil)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestGRPCServerConfig(t *testing.T) {
	t.Run("uses default config", func(t *testing.T) {
		config := DefaultGRPCServerConfig()
		if config.Port != 50051 {
			t.Errorf("Port = %v, want 50051", config.Port)
		}
		if config.Network != "tcp" {
			t.Errorf("Network = %v, want 'tcp'", config.Network)
		}
	})

	t.Run("applies config options", func(t *testing.T) {
		config := DefaultGRPCServerConfig()
		WithPort(8080)(config)
		WithNetwork("unix")(config)

		if config.Port != 8080 {
			t.Errorf("Port = %v, want 8080", config.Port)
		}
		if config.Network != "unix" {
			t.Errorf("Network = %v, want 'unix'", config.Network)
		}
	})

	t.Run("validates config", func(t *testing.T) {
		tests := []struct {
			port    int
			wantErr bool
		}{
			{50051, false},
			{0, true},
			{-1, true},
			{70000, true},
		}

		for _, tt := range tests {
			config := &GRPCServerConfig{Port: tt.port}
			err := ValidateConfig(config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig({Port: %d}) error = %v, wantErr %v", tt.port, err, tt.wantErr)
			}
		}
	})
}

func TestWithdrawalGRPCServer_toProto(t *testing.T) {
	logger := zap.NewNop()
	now := time.Now()
	processedAt := now.Add(1 * time.Hour)

	mockService := &svcmocks.MockWithdrawalService{}
	mockProcessor := &svcmocks.MockWithdrawalProcessor{}
	server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

	w := &entities.Withdrawal{
		ID:              "test-id",
		IdempotencyKey:  "test-key",
		UserID:          "user-1",
		Asset:           "BTC",
		Amount:          big.NewInt(100000000),
		DestinationAddr: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
		Network:         "bitcoin",
		Status:          entities.StatusCompleted,
		RetryCount:      1,
		MaxRetries:      3,
		ErrorMessage:    "",
		TxHash:          "0x123abc",
		CreatedAt:       now,
		UpdatedAt:       now,
		ProcessedAt:     &processedAt,
	}

	pb := server.toProto(w)

	if pb.Id != w.ID {
		t.Errorf("Id = %v, want %v", pb.Id, w.ID)
	}
	if pb.IdempotencyKey != w.IdempotencyKey {
		t.Errorf("IdempotencyKey = %v, want %v", pb.IdempotencyKey, w.IdempotencyKey)
	}
	if pb.Amount != w.Amount.String() {
		t.Errorf("Amount = %v, want %v", pb.Amount, w.Amount.String())
	}
	if pb.Status != withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_COMPLETED {
		t.Errorf("Status = %v, want COMPLETED", pb.Status)
	}
	if pb.ProcessedAt == nil {
		t.Error("ProcessedAt should not be nil")
	}
}

func TestWithdrawalGRPCServer_StatusConversion(t *testing.T) {
	logger := zap.NewNop()
	mockService := &svcmocks.MockWithdrawalService{}
	mockProcessor := &svcmocks.MockWithdrawalProcessor{}
	server := NewWithdrawalGRPCServer(mockService, mockProcessor, WithGRPCLogger(logger))

	tests := []struct {
		entityStatus entities.WithdrawalStatus
		protoStatus  withdrawalv1.WithdrawalStatus
	}{
		{entities.StatusUnspecified, withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_UNSPECIFIED},
		{entities.StatusPending, withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_PENDING},
		{entities.StatusProcessing, withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_PROCESSING},
		{entities.StatusCompleted, withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_COMPLETED},
		{entities.StatusFailed, withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_FAILED},
		{entities.StatusRetrying, withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_RETRYING},
	}

	for _, tt := range tests {
		t.Run(tt.entityStatus.String(), func(t *testing.T) {
			got := server.toProtoStatus(tt.entityStatus)
			if got != tt.protoStatus {
				t.Errorf("toProtoStatus(%v) = %v, want %v", tt.entityStatus, got, tt.protoStatus)
			}

			gotBack := server.fromProtoStatus(tt.protoStatus)
			if gotBack != tt.entityStatus {
				t.Errorf("fromProtoStatus(%v) = %v, want %v", tt.protoStatus, gotBack, tt.entityStatus)
			}
		})
	}
}
