package database

import (
	"context"
	"time"

	"atlasq/internal/opensearchclient"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type LoggingPool struct {
	*pgxpool.Pool
}

func (lp *LoggingPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	// Log query event
	event := map[string]interface{}{
		"sql":       sql,
		"args":      args,
		"timestamp": time.Now(),
	}
	opensearchclient.LogDebug("system", "query", event)
	return lp.Pool.Query(ctx, sql, args...)
}

func (lp *LoggingPool) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	event := map[string]interface{}{
		"sql":       sql,
		"args":      args,
		"timestamp": time.Now(),
	}
	opensearchclient.LogDebug("system", "exec", event)
	return lp.Pool.Exec(ctx, sql, args...)
}

func (lp *LoggingPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	event := map[string]interface{}{
		"sql":       sql,
		"args":      args,
		"timestamp": time.Now(),
	}
	opensearchclient.LogDebug("system", "queryrow", event)
	return lp.Pool.QueryRow(ctx, sql, args...)
}

// WrapPool returns a LoggingPool that wraps a pgxpool.Pool
func WrapPool(pool *pgxpool.Pool) *LoggingPool {
	return &LoggingPool{Pool: pool}
}
