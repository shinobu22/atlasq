package handlers

import (
	"encoding/json"
	"strconv"

	tasks "atlasq/internal/tasks"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
)

// EnqueueOrderHandler คืนค่า fiber.Handler
func CreateOrderQueue(client *asynq.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tenantIDStr := c.Query("tenant")
		if tenantIDStr == "" {
			return fiber.NewError(fiber.StatusBadRequest, "tenant query string is required")
		}

		tenantID, err := strconv.ParseInt(tenantIDStr, 10, 64)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid tenant ID")
		}

		var req tasks.OrderRequest
		if err := c.BodyParser(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}

		if req.WarehouseID == 0 || len(req.Items) == 0 {
			return fiber.NewError(fiber.StatusBadRequest, "warehouse_id and items are required")
		}

		payload := tasks.DeductStockPayload{
			TenantID:    tenantID,
			OrderNumber: req.OrderNumber,
			WarehouseID: req.WarehouseID,
			OrderID:     req.OrderID,
			Items:       req.Items,
		}

		data, err := json.Marshal(payload)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to create task payload")
		}

		task := asynq.NewTask("order:deduct_stock", data, asynq.MaxRetry(10))

		if _, err := client.Enqueue(task); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to enqueue task")
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Order enqueued for processing",
		})
	}
}
