package postq

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Context interface {
	context.Context
	Pool() *pgxpool.Pool
}
