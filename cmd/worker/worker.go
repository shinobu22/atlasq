package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"atlasq/internal/database"
	tasks "atlasq/internal/tasks"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgconn"
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
	log.Printf("Printf func DeductStockTaskHandler")
	var payload tasks.DeductStockPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	db := &database.PostgreSQL{}
	pool, err := db.Connect()
	if err != nil {
		log.Printf("failed to connect DB: %v", err)
		return err
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("failed to acquire DB connection: %v", err)
		return err
	}
	defer conn.Release()

	const maxRetries = 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			log.Printf("failed to begin tx: %v", err)
			return err
		}

		// ถ้า fn return ก่อน commit → rollback ให้แน่ใจ
		rollback := true
		defer func() {
			if rollback {
				_ = tx.Rollback(ctx)
			}
		}()

		// ---- business logic ----
		processErr := processStockTx(ctx, tx, payload)
		if processErr != nil {
			// ถ้าเจอ serialization conflict → retry
			if pgErr, ok := processErr.(*pgconn.PgError); ok && pgErr.Code == "40001" {
				log.Printf("serialization failure (retry %d/%d)", attempt, maxRetries)
				_ = tx.Rollback(ctx)
				time.Sleep(time.Duration(attempt) * 1000 * time.Millisecond) // backoff
				continue
			}
			_ = tx.Rollback(ctx)
			return processErr
		}

		// commit ถ้าไม่มี error
		if err := tx.Commit(ctx); err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "40001" {
				log.Printf("commit failed due to serialization (retry %d/%d)", attempt, maxRetries)
				time.Sleep(time.Duration(attempt) * 1000 * time.Millisecond)
				continue
			}
			return err
		}

		rollback = false // commit สำเร็จแล้ว → ไม่ต้อง rollback
		log.Printf("✅ Order processed: tenant=%d warehouse=%d items=%d",
			payload.TenantID, payload.WarehouseID, len(payload.Items))
		return nil
	}

	return fmt.Errorf("failed after %d retries due to serialization conflicts", maxRetries)
}

// แยก logic ออกมาเพื่อให้อ่านง่าย
func processStockTx(ctx context.Context, tx pgx.Tx, payload tasks.DeductStockPayload) error {
	log.Printf("processStockTx 1")
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
		log.Printf("processStockTx 1111")
		if err != nil {
			// insert ถ้ายังไม่มี stock
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
		log.Printf("processStockTx 2222")

		if stockQty < float64(item.Quantity) {
			log.Printf("not enough stock for product_id=%d", item.ProductID)
			return fmt.Errorf("not enough stock for product_id=%d , stockQty=%f , item.required=%d", item.ProductID, stockQty, item.Quantity)
		}
		log.Printf("processStockTx 4444")
		// เช็ค stock พอไหม
		newQty := stockQty - float64(item.Quantity)
		log.Printf("processStockTx 5555")
		// update stock
		_, err = tx.Exec(
			ctx,
			`UPDATE stock 
             SET quantity=$1, on_hand=$1, update_date=CURRENT_TIMESTAMP, row_update_date=CURRENT_TIMESTAMP 
             WHERE id=$2`,
			newQty, stockID,
		)
		log.Printf("processStockTx 6666")
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
		log.Printf("###### finish insert transaction stockID=%d ######", stockID)
		if err != nil {
			log.Printf("failed to insert transaction: %w", err)
			return fmt.Errorf("failed to insert transaction: %w", err)
		}
	}
	return nil
}
