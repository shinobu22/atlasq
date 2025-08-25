package handlers

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type OrderItem struct {
	ProductID int64 `json:"product_id"`
	Quantity  int64 `json:"quantity"`
}

type OrderRequest struct {
	WarehouseID int64       `json:"warehouse_id"`
	Items       []OrderItem `json:"items"`
}

func CreateOrder(pool *pgxpool.Pool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		conn, err := pool.Acquire(c.Context())
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to acquire database connection")
		}
		defer conn.Release()

		tenantID := c.Query("tenant")
		if tenantID == "" {
			return fiber.NewError(fiber.StatusBadRequest, "tenant query string is required")
		}

		// ตรวจสอบ tenant
		var exists bool
		if err := conn.QueryRow(c.Context(), `SELECT EXISTS(SELECT 1 FROM tenant WHERE id=$1)`, tenantID).Scan(&exists); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to validate tenant")
		}
		if !exists {
			return fiber.NewError(fiber.StatusBadRequest, "tenant not found")
		}

		var req OrderRequest
		if err := c.BodyParser(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
		if req.WarehouseID == 0 || len(req.Items) == 0 {
			return fiber.NewError(fiber.StatusBadRequest, "warehouse_id and items are required")
		}

		tx, err := conn.BeginTx(c.Context(), pgx.TxOptions{
			IsoLevel: pgx.RepeatableRead,
		})
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to start transaction")
		}
		defer tx.Rollback(c.Context())

		for _, item := range req.Items {
			var stockQty, reserveQty, onHandQty float64
			var stockID int64

			err := tx.QueryRow(
				c.Context(),
				`SELECT id, quantity, reserve, on_hand FROM stock WHERE product_id=$1 AND warehouse_id=$2 AND tenant_id=$3`,
				item.ProductID, req.WarehouseID, tenantID,
			).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)

			if err != nil { // ไม่เจอ stock -> insert
				err = tx.QueryRow(
					c.Context(),
					`INSERT INTO stock (
						tenant_id, warehouse_id, product_id,
						minimum, quantity, reserve, on_hand, status,
						create_date, update_date, row_create_date, row_update_date
					) VALUES ($1,$2,$3,0,$4,0,$4,true,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
					RETURNING id, quantity, reserve, on_hand`,
					tenantID, req.WarehouseID, item.ProductID, item.Quantity,
				).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)
				if err != nil {
					return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create stock: %v", err))
				}
			}

			if stockQty < float64(item.Quantity) {
				return fiber.NewError(fiber.StatusBadRequest,
					fmt.Sprintf("not enough stock for product %d, current: %.0f, required: %d", item.ProductID, stockQty, item.Quantity),
				)
			}

			newQty := stockQty - float64(item.Quantity)
			_, err = tx.Exec(
				c.Context(),
				`UPDATE stock SET quantity=$1, on_hand=$1, update_date=CURRENT_TIMESTAMP, row_update_date=CURRENT_TIMESTAMP WHERE id=$2`,
				newQty, stockID,
			)
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, "failed to update stock")
			}

			// สร้าง transaction log
			_, err = tx.Exec(
				c.Context(),
				`INSERT INTO transaction (
					model, event, teanant_id, product_id, warehouse_id, stock_id,
					quantity_old, quantity_change, quantity_new,
					reserve_old, reserve_change, reserve_new,
					on_hand_old, on_hand_change, on_hand_new,
					status, create_date, update_date, row_create_date, row_update_date
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`,
				"ORDER", "ISSUE", tenantID, item.ProductID, req.WarehouseID, stockID,
				stockQty, -float64(item.Quantity), newQty,
				reserveQty, 0, reserveQty,
				onHandQty, -float64(item.Quantity), newQty,
				true,
			)
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, "failed to create transaction log")
			}
		}

		if err := tx.Commit(c.Context()); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to commit transaction")
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Order created",
		})
	}
}
