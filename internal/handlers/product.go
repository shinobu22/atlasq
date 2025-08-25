package handlers

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v4/pgxpool"
)

type ProductRequest struct {
	Name        string  `json:"name" validate:"required,max=255"`
	Description string  `json:"description"`
	Price       float64 `json:"price" validate:"required,gte=0"`
	SKU         string  `json:"sku"`
}

func CreateProduct(pool *pgxpool.Pool) fiber.Handler {
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

		var exists bool
		if err := conn.QueryRow(c.Context(), `SELECT EXISTS(SELECT 1 FROM tenant WHERE id=$1)`, tenantID).Scan(&exists); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to validate tenant")
		}
		if !exists {
			return fiber.NewError(fiber.StatusBadRequest, "tenant not found")
		}

		var req ProductRequest
		if err := c.BodyParser(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}
		if len(req.Name) == 0 || len(req.Name) > 255 {
			return fiber.NewError(fiber.StatusBadRequest, "name is required and must be <= 255 characters")
		}
		if req.Price <= 0 {
			return fiber.NewError(fiber.StatusBadRequest, "price must be > 0")
		}

		_, err = conn.Exec(c.Context(), `INSERT INTO product (tenant_id, name, description, price, sku) VALUES ($1,$2,$3,$4,$5)`,
			tenantID, req.Name, req.Description, req.Price, req.SKU)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, err.Error())
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Product created",
			"name":    req.Name,
		})
	}
}
