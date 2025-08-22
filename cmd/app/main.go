package main

import (
	"atlasq/internal/database"
	tasks "atlasq/internal/tasks"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	"github.com/jackc/pgx/v4"

	"atlasq/internal/logger"

	"go.uber.org/zap"
)

func main() {
	db := &database.PostgreSQL{}

	// Connect to PostgreSQL
	pool, err := db.Connect()
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	// Asynq client ประกาศไว้ข้างนอก handler เพื่อ reuse
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"})
	defer client.Close()

	log.Println("Connected to PostgreSQL successfully")

	_ = asynq.NewClient(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"})

	app := fiber.New()

	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		elapsed := time.Since(start)
		log.Printf("[%s] %s took %s", c.Method(), c.Path(), elapsed)
		return err
	})

	// Asynqmon Web UI
	r := asynqmon.New(asynqmon.Options{
		RootPath:     "/monitor",
		RedisConnOpt: asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
	})

	// ใช้ adaptor.WrapHandler / HTTPHandler เพื่อแปลงให้ Fiber ใช้ได้
	app.Use("/monitor", adaptor.HTTPHandler(r))

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("AtlasQ")
	})

	type TenantRequest struct {
		Name string `json:"name" validate:"required,max=255"`
	}

	app.Post("/api/v1/tenants", func(c *fiber.Ctx) error {

		conn, err := pool.Acquire(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to acquire database connection",
			})
		}
		defer conn.Release()

		var req TenantRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "invalid request body",
			})
		}
		if len(req.Name) == 0 || len(req.Name) > 255 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "name is required and must be <= 255 characters",
			})
		}
		// TODO: insert tenant to database here
		_, err = conn.Exec(
			c.Context(),
			`INSERT INTO tenant (name) VALUES ($1)`,
			req.Name,
		)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to insert tenant",
				"test":  err.Error(),
			})
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Tenant created",
			"name":    req.Name,
		})
	})

	type ProductRequest struct {
		Name        string  `json:"name" validate:"required,max=255"`
		Description string  `json:"description"`
		Price       float64 `json:"price" validate:"required,gte=0"`
		SKU         string  `json:"sku"`
	}

	app.Post("/api/v1/products", func(c *fiber.Ctx) error {

		conn, err := pool.Acquire(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to acquire database connection",
			})
		}
		defer conn.Release()

		tenantID := c.Query("tenant")
		if tenantID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "tenant query string is required",
			})
		}

		// Validate tenant exists
		var exists bool
		err = conn.QueryRow(
			c.Context(),
			`SELECT EXISTS(SELECT 1 FROM tenant WHERE id = $1)`, tenantID,
		).Scan(&exists)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to validate tenant",
			})
		}
		if !exists {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "tenant not found",
			})
		}

		var req ProductRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		if len(req.Name) == 0 || len(req.Name) > 255 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "name is required and must be <= 255 characters",
			})
		}
		if req.Price == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "price is required and must be > 0",
			})
		}

		_, err = conn.Exec(
			c.Context(),
			`INSERT INTO product (tenant_id, name, description, price, sku) VALUES ($1, $2, $3, $4, $5)`,
			tenantID, req.Name, req.Description, req.Price, req.SKU,
		)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":   "failed to insert product",
				"message": err.Error(),
			})
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Product created",
			"name":    req.Name,
		})
	})

	type StockRequest struct {
		ProductID   int64 `json:"product_id"`
		WarehouseID int64 `json:"warehouse_id"`
		Quantity    int64 `json:"quantity"` // จำนวนที่เพิ่ม (+) หรือ ลด (-)
	}

	app.Post("/api/v1/stocks", func(c *fiber.Ctx) error {

		conn, err := pool.Acquire(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to acquire database connection",
			})
		}
		defer conn.Release()

		tenantID := c.Query("tenant")
		if tenantID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "tenant query string is required",
			})
		}

		var req StockRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "invalid request body",
			})
		}

		if req.ProductID == 0 || req.WarehouseID == 0 || req.Quantity == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "product_id, warehouse_id, and quantity are required",
			})
		}

		tx, err := conn.Begin(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to start transaction",
			})
		}
		defer tx.Rollback(c.Context())

		var currentStock int64
		err = tx.QueryRow(
			c.Context(),
			`SELECT quantity FROM stock WHERE product_id = $1 AND warehouse_id = $2 AND tenant_id = $3`,
			req.ProductID, req.WarehouseID, tenantID,
		).Scan(&currentStock)

		if err != nil { // ไม่เจอ stock
			_, err := tx.Exec(
				c.Context(),
				`INSERT INTO stock (
					tenant_id, warehouse_id, product_id,
					minimum, quantity, reserve, on_hand, status,
					create_date, update_date, row_create_date, row_update_date
				) VALUES (
					$1, $2, $3,
					0, $4, 0, $4, true,
					CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
				)`,
				req.ProductID, req.WarehouseID, tenantID, req.Quantity,
			)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "failed to create stock",
				})
			}
			currentStock = req.Quantity
		} else {
			newStock := currentStock + req.Quantity
			if newStock < 0 {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error":         "not enough stock to deduct",
					"current_stock": currentStock,
					"deduct":        req.Quantity,
				})
			}
			_, err := tx.Exec(
				c.Context(),
				`UPDATE stock SET
					quantity = $1,
					on_hand = $1,
					update_date = CURRENT_TIMESTAMP,
					row_update_date = CURRENT_TIMESTAMP
				WHERE product_id = $2 AND warehouse_id = $3 AND tenant_id = $4`,
				newStock, req.ProductID, req.WarehouseID, tenantID,
			)
			if err != nil {
				fmt.Println("failed to update stock")
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "failed to update stock",
				})
			}
			currentStock = newStock
		}

		_, err = tx.Exec(
			c.Context(),
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
			"STOCK", "ISSUE", tenantID, req.ProductID, req.WarehouseID, 0, // stock_id = 0 ถ้าไม่มี
			currentStock, req.Quantity, currentStock+req.Quantity,
			0, 0, 0, // reserve
			0, 0, 0, // on_hand
			true,
		)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to create transaction log",
			})
		}

		if err := tx.Commit(c.Context()); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to commit transaction",
			})
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message":      "Stock updated",
			"currentStock": currentStock,
		})
	})

	type OrderItem struct {
		ProductID int64 `json:"product_id"`
		Quantity  int64 `json:"quantity"`
	}

	type OrderRequest struct {
		WarehouseID int64       `json:"warehouse_id"`
		Items       []OrderItem `json:"items"`
	}

	app.Post("/api/v1/orders", func(c *fiber.Ctx) error {

		conn, err := pool.Acquire(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to acquire database connection",
			})
		}
		defer conn.Release()

		tenantID := c.Query("tenant")
		if tenantID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "tenant query string is required",
			})
		}

		var req OrderRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "invalid request body",
			})
		}
		if req.WarehouseID == 0 || len(req.Items) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "warehouse_id and items are required",
			})
		}

		/*
			Isolation Level
			1. Read Uncommitted
				อ่านข้อมูลที่ยังไม่ถูก commit จาก transaction อื่นได้
				อาจเจอข้อมูลที่ถูก rollback (ข้อมูลผิด/ไม่จริง)
				ไม่ปลอดภัย (PostgreSQL ไม่รองรับ level นี้)
			2. Read Committed (ค่า default ของ PostgreSQL)
				อ่านข้อมูลที่ถูก commit แล้วเท่านั้น
				ไม่เห็นข้อมูลที่ยังไม่ commit จาก transaction อื่น
				ป้องกัน dirty read ได้
				อาจเจอปัญหา "non-repeatable read" (ข้อมูลเปลี่ยนระหว่าง transaction)
			3. Repeatable Read
				ข้อมูลที่อ่านครั้งแรก จะเหมือนเดิมตลอดทั้ง transaction
				ไม่เจอ dirty read หรือ non-repeatable read
				อาจเจอ "phantom read" (แถวใหม่ถูก insert โดย transaction อื่น)
			4. Serializable
				เข้มงวดที่สุด ทุก transaction เหมือนรันทีละตัว
				ป้องกันทุกปัญหา (dirty read, non-repeatable read, phantom read)
				อาจช้ากว่า เพราะต้อง lock มากขึ้น
		*/

		tx, err := conn.BeginTx(c.Context(), pgx.TxOptions{
			IsoLevel: pgx.RepeatableRead, // หรือ pgx.Serializable
		})
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": err.Error(),
				"error":   "failed to start transaction",
			})
		}
		defer tx.Rollback(c.Context())

		for _, item := range req.Items {
			var stockQty, reserveQty, onHandQty float64
			var stockID int64

			// หา stock
			err := tx.QueryRow(
				c.Context(),
				`SELECT id, quantity, reserve, on_hand FROM stock WHERE product_id = $1 AND warehouse_id = $2 AND tenant_id = $3`,
				item.ProductID, req.WarehouseID, tenantID,
			).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)

			if err != nil { // ไม่เจอ stock
				// สร้าง stock ใหม่
				err = tx.QueryRow(
					c.Context(),
					`INSERT INTO stock (
                        tenant_id, warehouse_id, product_id,
                        minimum, quantity, reserve, on_hand, status,
                        create_date, update_date, row_create_date, row_update_date
                    ) VALUES (
                        $1, $2, $3,
                        0, $4, 0, $4, true,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    ) RETURNING id, quantity, reserve, on_hand`,
					tenantID, req.WarehouseID, item.ProductID, item.Quantity,
				).Scan(&stockID, &stockQty, &reserveQty, &onHandQty)
				if err != nil {
					return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
						"error":   "failed to create stock",
						"message": err.Error(),
					})
				}
			}

			// เช็ค stock พอไหม
			if stockQty < float64(item.Quantity) {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error":      "not enough stock",
					"product_id": item.ProductID,
					"stock":      stockQty,
					"required":   item.Quantity,
				})
			}

			// ลด stock
			newQty := stockQty - float64(item.Quantity)
			_, err = tx.Exec(
				c.Context(),
				`UPDATE stock SET quantity = $1, on_hand = $1, update_date = CURRENT_TIMESTAMP, row_update_date = CURRENT_TIMESTAMP WHERE id = $2`,
				newQty, stockID,
			)
			if err != nil {
				fmt.Println("111 err", err)
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "111 failed to update stock",
				})
			}

			// // สร้าง order item
			// _, err = tx.Exec(
			// 	c.Context(),
			// 	`INSERT INTO order_items (product_id, quantity, warehouse_id, tenant_id) VALUES ($1, $2, $3, $4)`,
			// 	item.ProductID, item.Quantity, req.WarehouseID, tenantID,
			// )
			// if err != nil {
			// 	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			// 		"error":   "failed to create order item",
			// 		"message": err.Error(),
			// 	})
			// }

			// สร้าง transaction log
			_, err = tx.Exec(
				c.Context(),
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
				"ORDER", "ISSUE", tenantID, item.ProductID, req.WarehouseID, stockID,
				stockQty, -float64(item.Quantity), newQty,
				reserveQty, 0, reserveQty,
				onHandQty, -float64(item.Quantity), newQty,
				true,
			)
			if err != nil {
				fmt.Println("222 err", err)
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "failed to create transaction log",
				})
			}
		}

		if err := tx.Commit(c.Context()); err != nil {
			fmt.Println("222 err", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to commit transaction",
			})
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Order created",
		})
	})

	// API endpoint to enqueue order tasks
	app.Post("/api/v1/orders-queue", func(c *fiber.Ctx) error {
		tenantIDStr := c.Query("tenant")
		if tenantIDStr == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "tenant query string is required"})
		}

		tenantID, err := strconv.ParseInt(tenantIDStr, 10, 64)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid tenant ID"})
		}

		var req tasks.OrderRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
		}

		if req.WarehouseID == 0 || len(req.Items) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "warehouse_id and items are required"})
		}

		payload := tasks.DeductStockPayload{
			TenantID:    tenantID,
			WarehouseID: req.WarehouseID,
			Items:       req.Items,
		}

		data, err := json.Marshal(payload)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create task payload"})
		}

		task := asynq.NewTask("order:deduct_stock", data)
		if _, err := client.Enqueue(task); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to enqueue task"})
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{"message": "Order enqueued for processing"})
	})

	if err := app.Listen(":8080"); err != nil {
		log.Fatalf("failed to start Fiber app: %v", err)
	}

	_ = logger.Init("atlasq")
	logger.L().Info("worker started", zap.String("component", "job-consumer"))

	logger.WithJob("12345", "high", "tiktok").Info("job processed",
		zap.Int("duration_ms", 128),
		zap.String("status", "success"),
	)
}
