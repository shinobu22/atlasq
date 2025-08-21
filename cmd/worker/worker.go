package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"atlasq/internal/database"
	tasks "atlasq/internal/tasks"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type OrderItem struct {
	ProductID int64 `json:"product_id"`
	Quantity  int64 `json:"quantity"`
}

var pool *pgxpool.Pool

func main() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"default":  1,
				"critical": 2,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("order:deduct_stock", DeductStockTaskHandler)

	if err := srv.Run(mux); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}

// ----------------- Handler -----------------

func DeductStockTaskHandler(ctx context.Context, t *asynq.Task) error {
	log.Printf("DeductStockTaskHandler")
	var payload tasks.DeductStockPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	db := &database.PostgreSQL{}
	pool, err := db.Connect()
	if err != nil {
		log.Printf("failed to connect DB: %w", err)
		return fmt.Errorf("failed to connect DB: %w", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("failed to acquire DB connection: %w", err)
		return fmt.Errorf("failed to acquire DB connection: %w", err)
	}
	defer conn.Release()

	// ใช้ transaction
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		log.Printf("failed to begin tx: %w", err)
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, item := range payload.Items {
		var stockID int64
		var stockQty, reserveQty, onHandQty float64

		// Query stock
		err := tx.QueryRow(
			ctx,
			`SELECT id, quantity, reserve, on_hand 
             FROM stock 
             WHERE product_id=$1 AND warehouse_id=$2 AND tenant_id=$3`,
			item.ProductID, payload.WarehouseID, payload.TenantID,
		).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)

		if err != nil { // insert ถ้ายังไม่มี stock
			err = tx.QueryRow(
				ctx,
				`INSERT INTO stock (
                    tenant_id, warehouse_id, product_id, minimum,
                    quantity, reserve, on_hand, status,
                    create_date, update_date, row_create_date, row_update_date
                ) VALUES ($1,$2,$3,0,$4,0,$4,true,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                RETURNING id, quantity, reserve, on_hand`,
				payload.TenantID, payload.WarehouseID, item.ProductID, float64(item.Quantity),
			).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)
			if err != nil {
				log.Printf("failed to insert stock: %w", err)
				return fmt.Errorf("failed to insert stock: %w", err)
			}
		}

		// ตรวจสอบ idempotency: เคยสร้าง transaction สำหรับ order นี้แล้วหรือยัง
		var exists bool
		err = tx.QueryRow(
			ctx,
			`SELECT EXISTS (
                SELECT 1 FROM transaction
                WHERE model='ORDER' AND event='ISSUE'
                AND product_id=$1 AND warehouse_id=$2 AND teanant_id=$3
                AND quantity_change=$4
            )`,
			item.ProductID, payload.WarehouseID, payload.TenantID, -float64(item.Quantity),
		).Scan(&exists)
		if err != nil {
			log.Printf("failed to check idempotency: %w", err)
			return fmt.Errorf("failed to check idempotency: %w", err)
		}
		if exists {
			// หากเคยประมวลผลแล้ว ข้าม item นี้
			continue
		}

		if stockQty < float64(item.Quantity) {
			return fmt.Errorf("not enough stock for product_id=%d", item.ProductID)
		}

		newQty := stockQty - float64(item.Quantity)

		// update stock
		_, err = tx.Exec(
			ctx,
			`UPDATE stock 
             SET quantity=$1, on_hand=$1, update_date=CURRENT_TIMESTAMP, row_update_date=CURRENT_TIMESTAMP 
             WHERE id=$2`,
			newQty, stockID,
		)
		if err != nil {
			log.Printf("failed to update stock: %w", err)
			return fmt.Errorf("failed to update stock: %w", err)
		}

		// insert transaction log
		_, err = tx.Exec(
			ctx,
			`INSERT INTO transaction (
                model,event,teanant_id,product_id,warehouse_id,stock_id,
                quantity_old,quantity_change,quantity_new,
                reserve_old,reserve_change,reserve_new,
                on_hand_old,on_hand_change,on_hand_new,
                status,create_date,update_date,row_create_date,row_update_date
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
            )`,
			"ORDER", "ISSUE", payload.TenantID, item.ProductID, payload.WarehouseID, stockID,
			stockQty, -float64(item.Quantity), newQty,
			reserveQty, 0, reserveQty,
			onHandQty, -float64(item.Quantity), newQty,
			true,
		)
		if err != nil {
			log.Printf("failed to insert transaction: %w", err)
			return fmt.Errorf("failed to insert transaction: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("failed to commit tx: %w", err)
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	log.Printf("✅ Order processed: tenant=%d warehouse=%d items=%d", payload.TenantID, payload.WarehouseID, len(payload.Items))
	return nil
}
