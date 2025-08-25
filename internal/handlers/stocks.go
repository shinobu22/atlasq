package handlers

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v4/pgxpool"
)

type StockRequest struct {
	ProductID   int64 `json:"product_id"`
	WarehouseID int64 `json:"warehouse_id"`
	Quantity    int64 `json:"quantity"` // จำนวนที่เพิ่ม (+) หรือ ลด (-)
}

func CreateOrUpdateStock(pool *pgxpool.Pool) fiber.Handler {
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

		var req StockRequest
		if err := c.BodyParser(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}

		if req.ProductID == 0 || req.WarehouseID == 0 || req.Quantity == 0 {
			return fiber.NewError(fiber.StatusBadRequest, "product_id, warehouse_id, and quantity are required")
		}

		// เริ่ม transaction
		tx, err := conn.Begin(c.Context())
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to start transaction")
		}
		defer tx.Rollback(c.Context())

		var currentStock int64
		err = tx.QueryRow(
			c.Context(),
			`SELECT quantity FROM stock WHERE product_id=$1 AND warehouse_id=$2 AND tenant_id=$3`,
			req.ProductID, req.WarehouseID, tenantID,
		).Scan(&currentStock)

		if err != nil { // ไม่เจอ stock -> insert
			_, err := tx.Exec(
				c.Context(),
				`INSERT INTO stock (
					tenant_id, warehouse_id, product_id,
					minimum, quantity, reserve, on_hand, status,
					create_date, update_date, row_create_date, row_update_date
				) VALUES ($1,$2,$3,0,$4,0,$4,true,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`,
				tenantID, req.WarehouseID, req.ProductID, req.Quantity,
			)
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, "failed to create stock")
			}
			currentStock = req.Quantity
		} else {
			newStock := currentStock + req.Quantity
			if newStock < 0 {
				return fiber.NewError(fiber.StatusBadRequest,
					fmt.Sprintf("not enough stock to deduct, current: %d, deduct: %d", currentStock, req.Quantity),
				)
			}

			_, err := tx.Exec(
				c.Context(),
				`UPDATE stock SET quantity=$1, on_hand=$1, update_date=CURRENT_TIMESTAMP, row_update_date=CURRENT_TIMESTAMP
				 WHERE product_id=$2 AND warehouse_id=$3 AND tenant_id=$4`,
				newStock, req.ProductID, req.WarehouseID, tenantID,
			)
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, "failed to update stock")
			}
			currentStock = newStock
		}

		// สร้าง transaction log
		_, err = tx.Exec(
			c.Context(),
			`INSERT INTO transaction (
				model,event,teanant_id,product_id,warehouse_id,stock_id,
				quantity_old,quantity_change,quantity_new,
				reserve_old,reserve_change,reserve_new,
				on_hand_old,on_hand_change,on_hand_new,
				status,create_date,update_date,row_create_date,row_update_date
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`,
			"STOCK", "ISSUE", tenantID, req.ProductID, req.WarehouseID, 0, // stock_id = 0 ถ้าไม่มี
			currentStock-req.Quantity, req.Quantity, currentStock,
			0, 0, 0, // reserve
			0, 0, 0, // on_hand
			true,
		)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to create transaction log")
		}

		if err := tx.Commit(c.Context()); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to commit transaction")
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message":      "Stock updated",
			"currentStock": currentStock,
		})
	}
}
