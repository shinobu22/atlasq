package main

import (
	"atlasq/internal/database"
	"context"
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type OrderItem struct {
	ProductID int64 `json:"product_id"`
	Quantity  int64 `json:"quantity"`
}

type DeductStockPayload struct {
	TenantID    string      `json:"tenant_id"`
	WarehouseID int64       `json:"warehouse_id"`
	Items       []OrderItem `json:"items"`
}

var pool *pgxpool.Pool

func handleDeductStock(ctx context.Context, t *asynq.Task) error {
	var p DeductStockPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("failed to acquire database connection: %v", err)
		return err
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})

	if err != nil {
		log.Printf("failed to start transaction: %v", err)
		return err
	}
	defer tx.Rollback(ctx)

	for _, item := range p.Items {
		var stockQty, reserveQty, onHandQty float64
		var stockID int64

		err := tx.QueryRow(
			ctx,
			`SELECT id, quantity, reserve, on_hand FROM stock WHERE product_id = $1 AND warehouse_id = $2 AND tenant_id = $3`,
			item.ProductID, p.WarehouseID, p.TenantID,
		).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)

		if err != nil {
			// สร้าง stock ใหม่
			err = tx.QueryRow(
				ctx,
				`INSERT INTO stock (
                    tenant_id, warehouse_id, product_id,
                    minimum, quantity, reserve, on_hand, status,
                    create_date, update_date, row_create_date, row_update_date
                ) VALUES (
                    $1, $2, $3,
                    0, $4, 0, $4, true,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                ) RETURNING id, quantity, reserve, on_hand`,
				p.TenantID, p.WarehouseID, item.ProductID, item.Quantity,
			).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)
			if err != nil {
				log.Printf("failed to create stock: %v", err)
				return err
			}
		}

		if stockQty < float64(item.Quantity) {
			log.Printf("not enough stock for product_id %d: have %f, need %d", item.ProductID, stockQty, item.Quantity)
			return nil // หรือ return error ถ้าต้องการให้ fail job
		}

		newQty := stockQty - float64(item.Quantity)
		_, err = tx.Exec(
			ctx,
			`UPDATE stock SET quantity = $1, on_hand = $1, update_date = CURRENT_TIMESTAMP, row_update_date = CURRENT_TIMESTAMP WHERE id = $2`,
			newQty, stockID,
		)
		if err != nil {
			log.Printf("failed to update stock: %v", err)
			return err
		}

		_, err = tx.Exec(
			ctx,
			`INSERT INTO transaction (
                model, event, teanant_id, product_id, warehouse_id, stock_id,
                quantity_old, quantity_change, quantity_new,
                reserve_old, reserve_change, reserve_new,
                on_hand_old, on_hand_change, on_hand_new,
                status, create_date, update_date, row_create_date, row_update_date
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15,
                $16, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )`,
			"ORDER", "ISSUE", p.TenantID, item.ProductID, p.WarehouseID, stockID,
			stockQty, -float64(item.Quantity), newQty,
			reserveQty, 0, reserveQty,
			onHandQty, -float64(item.Quantity), newQty,
			true,
		)
		if err != nil {
			log.Printf("failed to create transaction log: %v", err)
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("failed to commit transaction: %v", err)
		return err
	}

	log.Printf("Order processed for tenant %s, warehouse %d", p.TenantID, p.WarehouseID)
	return nil
}

func main() {
	db := &database.PostgreSQL{}

	// Connect to PostgreSQL
	pool, err := db.Connect()
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	log.Println("Connected to PostgreSQL successfully")

	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}, asynq.Config{Concurrency: 10})
	mux := asynq.NewServeMux()
	mux.HandleFunc("order:deduct_stock", handleDeductStock)
	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
