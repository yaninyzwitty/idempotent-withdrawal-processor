package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool = pgxpool.Pool

func NewPool(ctx context.Context, connString string) (*Pool, error) {
	return pgxpool.New(ctx, connString)
}
