package grpc

import (
	"context"
	"fmt"

	withdrawalv1 "github.com/yaninyzwitty/idempotent-widthrawal-processor/gen/withdrawal/v1"
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/entities"
	domain "github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/domain/services"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WithdrawalGRPCServer struct {
	withdrawalv1.UnimplementedWithdrawalServiceServer
	withdrawalv1.UnimplementedWithdrawalProcessorServiceServer
	svc       domain.WithdrawalService
	processor domain.WithdrawalProcessor
	logger    *zap.Logger
}

type WithdrawalGRPCServerOption func(*WithdrawalGRPCServer)

func WithGRPCLogger(logger *zap.Logger) WithdrawalGRPCServerOption {
	return func(s *WithdrawalGRPCServer) {
		s.logger = logger
	}
}

func NewWithdrawalGRPCServer(
	svc domain.WithdrawalService,
	processor domain.WithdrawalProcessor,
	opts ...WithdrawalGRPCServerOption,
) *WithdrawalGRPCServer {
	s := &WithdrawalGRPCServer{
		svc:       svc,
		processor: processor,
		logger:    zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *WithdrawalGRPCServer) CreateWithdrawal(ctx context.Context, req *withdrawalv1.CreateWithdrawalRequest) (*withdrawalv1.CreateWithdrawalResponse, error) {
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency key is required")
	}
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}
	if req.Amount == "" {
		return nil, status.Error(codes.InvalidArgument, "amount is required")
	}
	if req.DestinationAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "destination address is required")
	}

	maxRetries := int(req.MaxRetries)
	if maxRetries <= 0 {
		maxRetries = 3
	}

	createReq := &domain.CreateWithdrawalRequest{
		IdempotencyKey:  req.IdempotencyKey,
		UserID:          req.UserId,
		Asset:           req.Asset,
		Amount:          req.Amount,
		DestinationAddr: req.DestinationAddress,
		Network:         req.Network,
		MaxRetries:      maxRetries,
	}

	withdrawal, alreadyExists, err := s.svc.CreateWithdrawal(ctx, createReq)
	if err != nil {
		s.logger.Error("failed to create withdrawal",
			zap.Error(err),
			zap.String("idempotency_key", req.IdempotencyKey),
		)
		return nil, status.Errorf(codes.Internal, "failed to create withdrawal: %v", err)
	}

	return &withdrawalv1.CreateWithdrawalResponse{
		Withdrawal:    s.toProto(withdrawal),
		AlreadyExists: alreadyExists,
	}, nil
}

func (s *WithdrawalGRPCServer) GetWithdrawal(ctx context.Context, req *withdrawalv1.GetWithdrawalRequest) (*withdrawalv1.GetWithdrawalResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	withdrawal, err := s.svc.GetWithdrawal(ctx, req.Id)
	if err != nil {
		if err == entities.ErrWithdrawalNotFound {
			return nil, status.Error(codes.NotFound, "withdrawal not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get withdrawal: %v", err)
	}

	return &withdrawalv1.GetWithdrawalResponse{
		Withdrawal: s.toProto(withdrawal),
	}, nil
}

func (s *WithdrawalGRPCServer) GetWithdrawalByIdempotencyKey(ctx context.Context, req *withdrawalv1.GetWithdrawalByIdempotencyKeyRequest) (*withdrawalv1.GetWithdrawalByIdempotencyKeyResponse, error) {
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency key is required")
	}

	withdrawal, err := s.svc.GetWithdrawalByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		if err == entities.ErrWithdrawalNotFound {
			return &withdrawalv1.GetWithdrawalByIdempotencyKeyResponse{
				Found: false,
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get withdrawal: %v", err)
	}

	return &withdrawalv1.GetWithdrawalByIdempotencyKeyResponse{
		Withdrawal: s.toProto(withdrawal),
		Found:      true,
	}, nil
}

func (s *WithdrawalGRPCServer) ListWithdrawals(ctx context.Context, req *withdrawalv1.ListWithdrawalsRequest) (*withdrawalv1.ListWithdrawalsResponse, error) {
	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}

	withdrawals, err := s.svc.ListWithdrawals(ctx, req.UserId, s.fromProtoStatus(req.Status), pageSize, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list withdrawals: %v", err)
	}

	protoWithdrawals := make([]*withdrawalv1.Withdrawal, len(withdrawals))
	for i, w := range withdrawals {
		protoWithdrawals[i] = s.toProto(w)
	}

	return &withdrawalv1.ListWithdrawalsResponse{
		Withdrawals: protoWithdrawals,
	}, nil
}

func (s *WithdrawalGRPCServer) RetryWithdrawal(ctx context.Context, req *withdrawalv1.RetryWithdrawalRequest) (*withdrawalv1.RetryWithdrawalResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	withdrawal, err := s.svc.RetryWithdrawal(ctx, req.Id)
	if err != nil {
		if err == entities.ErrWithdrawalNotFound {
			return nil, status.Error(codes.NotFound, "withdrawal not found")
		}
		if err == entities.ErrMaxRetriesExceeded {
			return nil, status.Error(codes.FailedPrecondition, "max retries exceeded")
		}
		return nil, status.Errorf(codes.Internal, "failed to retry withdrawal: %v", err)
	}

	return &withdrawalv1.RetryWithdrawalResponse{
		Withdrawal: s.toProto(withdrawal),
	}, nil
}

func (s *WithdrawalGRPCServer) StartProcessor(ctx context.Context, _ *withdrawalv1.StartProcessorRequest) (*withdrawalv1.StartProcessorResponse, error) {
	if err := s.processor.Start(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start processor: %v", err)
	}
	return &withdrawalv1.StartProcessorResponse{}, nil
}

func (s *WithdrawalGRPCServer) StopProcessor(ctx context.Context, _ *withdrawalv1.StopProcessorRequest) (*withdrawalv1.StopProcessorResponse, error) {
	if err := s.processor.Stop(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to stop processor: %v", err)
	}
	return &withdrawalv1.StopProcessorResponse{}, nil
}

func (s *WithdrawalGRPCServer) GetProcessorStats(ctx context.Context, _ *withdrawalv1.GetProcessorStatsRequest) (*withdrawalv1.GetProcessorStatsResponse, error) {
	if p, ok := s.processor.(interface {
		GetStats() interface {
			ToProto() *withdrawalv1.GetProcessorStatsResponse
		}
	}); ok {
		return p.GetStats().ToProto(), nil
	}
	return &withdrawalv1.GetProcessorStatsResponse{}, nil
}

func (s *WithdrawalGRPCServer) toProto(w *entities.Withdrawal) *withdrawalv1.Withdrawal {
	pb := &withdrawalv1.Withdrawal{
		Id:                 w.ID,
		IdempotencyKey:     w.IdempotencyKey,
		UserId:             w.UserID,
		Asset:              w.Asset,
		Amount:             w.Amount.String(),
		DestinationAddress: w.DestinationAddr,
		Network:            w.Network,
		Status:             s.toProtoStatus(w.Status),
		RetryCount:         int32(w.RetryCount),
		MaxRetries:         int32(w.MaxRetries),
		ErrorMessage:       w.ErrorMessage,
		TxHash:             w.TxHash,
		CreatedAt:          timestamppb.New(w.CreatedAt),
		UpdatedAt:          timestamppb.New(w.UpdatedAt),
	}

	if w.ProcessedAt != nil {
		pb.ProcessedAt = timestamppb.New(*w.ProcessedAt)
	}

	return pb
}

func (s *WithdrawalGRPCServer) toProtoStatus(status entities.WithdrawalStatus) withdrawalv1.WithdrawalStatus {
	switch status {
	case entities.StatusPending:
		return withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_PENDING
	case entities.StatusProcessing:
		return withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_PROCESSING
	case entities.StatusCompleted:
		return withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_COMPLETED
	case entities.StatusFailed:
		return withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_FAILED
	case entities.StatusRetrying:
		return withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_RETRYING
	default:
		return withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_UNSPECIFIED
	}
}

func (s *WithdrawalGRPCServer) fromProtoStatus(status withdrawalv1.WithdrawalStatus) entities.WithdrawalStatus {
	switch status {
	case withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_PENDING:
		return entities.StatusPending
	case withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_PROCESSING:
		return entities.StatusProcessing
	case withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_COMPLETED:
		return entities.StatusCompleted
	case withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_FAILED:
		return entities.StatusFailed
	case withdrawalv1.WithdrawalStatus_WITHDRAWAL_STATUS_RETRYING:
		return entities.StatusRetrying
	default:
		return entities.StatusUnspecified
	}
}

func RegisterWithdrawalService(server interface {
	RegisterWithdrawalServiceServer(srv withdrawalv1.WithdrawalServiceServer)
	RegisterWithdrawalProcessorServiceServer(srv withdrawalv1.WithdrawalProcessorServiceServer)
}, grpcServer *WithdrawalGRPCServer) {
	server.RegisterWithdrawalServiceServer(grpcServer)
	server.RegisterWithdrawalProcessorServiceServer(grpcServer)
}

type GRPCServerConfig struct {
	Port    int
	Network string
}

type GRPCServerOption func(*GRPCServerConfig)

func WithPort(port int) GRPCServerOption {
	return func(c *GRPCServerConfig) {
		c.Port = port
	}
}

func WithNetwork(network string) GRPCServerOption {
	return func(c *GRPCServerConfig) {
		c.Network = network
	}
}

func DefaultGRPCServerConfig() *GRPCServerConfig {
	return &GRPCServerConfig{
		Port:    50051,
		Network: "tcp",
	}
}

func ValidateConfig(config *GRPCServerConfig) error {
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d", config.Port)
	}
	return nil
}
