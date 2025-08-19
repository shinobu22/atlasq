package database

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
)

type PostgreSQL struct {
}

func (pg *PostgreSQL) Connect() (*pgxpool.Pool, error) {

	pool, err := pgxpool.Connect(context.Background(), pg.ConnectionURI())
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func (pg *PostgreSQL) Close(pool *pgxpool.Pool) {
	if pool != nil {
		pool.Close()
	}
}

func (pg *PostgreSQL) ConnectionURI() string {

	_ = godotenv.Load()

	host := os.Getenv("PG_HOST")
	port := os.Getenv("PG_PORT")
	user := os.Getenv("PG_USER")
	pass := os.Getenv("PG_PASS")
	db := os.Getenv("PG_DB")
	if port == "" {
		port = "5432"
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db)
}
