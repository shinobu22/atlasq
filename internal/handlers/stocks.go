package handlers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// StockIssueRequest สำหรับรับ input
type StockIssueRequest struct {
	AppID       int64   `json:"app_id"`
	StoreID     int64   `json:"store_id"`
	ProductID   int64   `json:"product_id"`
	WarehouseID int64   `json:"warehouse_id"`
	Quantity    float64 `json:"quantity"`
	Model       string  `json:"model"` // <-- เพิ่มตรงนี้
}

// Fiber handler สำหรับ /stock-issue
func StockIssueHandler(pool *pgxpool.Pool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		var req StockIssueRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}

		fmt.Println("request %v", req)
		if req.AppID == 0 || req.StoreID == 0 || req.ProductID == 0 || req.WarehouseID == 0 || req.Quantity <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request"})
		}

		if err := StockIssue(c.Context(), pool, req); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"message":   "Stock issued successfully",
			"app_id":    req.AppID,
			"store_id":  req.StoreID,
			"product":   req.ProductID,
			"warehouse": req.WarehouseID,
			"quantity":  req.Quantity,
		})
	}
}

// StockIssue logic transaction + Serializable isolation
// StockIssue logic transaction + Serializable isolation
func StockIssue(ctx context.Context, pool *pgxpool.Pool, req StockIssueRequest) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock stock row
	var stockID int64
	var balance, reserve, onHand float64
	err = tx.QueryRow(ctx, `
		SELECT id, balance, reserve, on_hand 
		FROM stock 
		WHERE product_id=$1 AND warehouse_id=$2 
		FOR UPDATE
	`, req.ProductID, req.WarehouseID).Scan(&stockID, &balance, &reserve, &onHand)
	if err != nil {
		return fmt.Errorf("failed to fetch stock: %w", err)
	}

	if balance < req.Quantity {
		return errors.New("insufficient stock balance")
	}

	// Update stock table
	_, err = tx.Exec(ctx, `
		UPDATE stock 
		SET balance = balance - $1, reserve = reserve - $1 
		WHERE id = $2
	`, req.Quantity, stockID)
	if err != nil {
		return fmt.Errorf("failed to update stock: %w", err)
	}

	// Update stock_balance
	currentYearMonth := time.Date(time.Now().Year(), time.Now().Month(), 1, 0, 0, 0, 0, time.UTC)
	_, err = tx.Exec(ctx, `
		UPDATE stock_balance
		SET balance = balance - $1, reserve = reserve - $1
		WHERE stock_id=$2 AND year_month >= $3
	`, req.Quantity, stockID, currentYearMonth)
	if err != nil {
		return fmt.Errorf("failed to update stock_balance: %w", err)
	}

	// ดึง lot ทั้งหมดก่อน
	type Lot struct {
		ID          int64
		Balance     float64
		CostFIFO    float64
		CostAverage float64
	}
	lots := []Lot{}
	rows, err := tx.Query(ctx, `
		SELECT id, balance, cost_fifo, cost_average
		FROM lot
		WHERE stock_id=$1 AND balance > 0
		ORDER BY created_date ASC
	`, stockID)
	if err != nil {
		return fmt.Errorf("failed to fetch lots: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var l Lot
		if err := rows.Scan(&l.ID, &l.Balance, &l.CostFIFO, &l.CostAverage); err != nil {
			return err
		}
		lots = append(lots, l)
	}

	// Deduct lots FIFO
	remaining := req.Quantity
	for _, lot := range lots {
		toDeduct := remaining
		if lot.Balance < toDeduct {
			toDeduct = lot.Balance
		}

		// Update lot
		if _, err := tx.Exec(ctx, `UPDATE lot SET balance = balance - $1 WHERE id=$2`, toDeduct, lot.ID); err != nil {
			return fmt.Errorf("failed to update lot: %w", err)
		}

		// Insert stock_movement
		_, err = tx.Exec(ctx, `
			INSERT INTO stock_movement (
				app_id, store_id, stock_id, lot_id, balance_before, balance_after, balance_change,
				reserve_before, reserve_after, reserve_change, cost_fifo, cost_average,
				action, model, created_date, updated_date
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'issue',$13,NOW(),NOW())`,
			req.AppID, req.StoreID, stockID, lot.ID, balance, balance-toDeduct, -toDeduct,
			reserve, reserve-toDeduct, -toDeduct, lot.CostFIFO, lot.CostAverage, req.Model)

		if err != nil {
			return fmt.Errorf("failed to insert stock_movement: %w", err)
		}

		remaining -= toDeduct
		balance -= toDeduct
		reserve -= toDeduct

		if remaining <= 0 {
			break
		}
	}

	if remaining > 0 {
		return errors.New("not enough lot quantity to fulfill the request")
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
