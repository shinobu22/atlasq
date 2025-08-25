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
		var tenantFromBody int64

		var req tasks.OrderRequest
		// Try standard BodyParser first (application/json)
		if err := c.BodyParser(&req); err != nil {
			// Fallback: try raw JSON unmarshal from body
			b := c.Body()
			if len(b) == 0 {
				return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
			}
			if err2 := json.Unmarshal(b, &req); err2 != nil {
				return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
			}
		}

		// allow tenant in body as well (if not provided as query param)
		if tenantIDStr == "" {
			// try to extract tenant from body if present
			var tmp map[string]interface{}
			_ = json.Unmarshal(c.Body(), &tmp)
			if v, ok := tmp["tenant"]; ok {
				switch t := v.(type) {
				case float64:
					tenantFromBody = int64(t)
				case string:
					if id, err := strconv.ParseInt(t, 10, 64); err == nil {
						tenantFromBody = id
					}
				}
			}
		}

		if tenantIDStr == "" && tenantFromBody == 0 {
			return fiber.NewError(fiber.StatusBadRequest, "tenant query string is required")
		}

		var tenantID int64
		if tenantIDStr != "" {
			t, err := strconv.ParseInt(tenantIDStr, 10, 64)
			if err != nil {
				return fiber.NewError(fiber.StatusBadRequest, "invalid tenant ID")
			}
			tenantID = t
		} else {
			tenantID = tenantFromBody
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

		task := asynq.NewTask("order:deduct_stock", data)
		if info, err := client.Enqueue(task); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to enqueue task")
		} else {
			// return enqueue info to caller for debugging
			return c.Status(fiber.StatusCreated).JSON(fiber.Map{
				"message": "Order enqueued for processing",
				"task_id": info.ID,
				"queue":   info.Queue,
			})
		}
	}
}
