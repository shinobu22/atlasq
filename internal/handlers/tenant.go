package handlers

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v4/pgxpool"
)

type TenantRequest struct {
	Name string `json:"name" validate:"required,max=255"`
}

func CreateTenant(pool *pgxpool.Pool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		conn, err := pool.Acquire(c.Context())
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to acquire database connection")
		}
		defer conn.Release()

		var req TenantRequest
		if err := c.BodyParser(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
		if len(req.Name) == 0 || len(req.Name) > 255 {
			return fiber.NewError(fiber.StatusBadRequest, "name is required and must be <= 255 characters")
		}

		_, err = conn.Exec(c.Context(), `INSERT INTO tenant (name) VALUES ($1)`, req.Name)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to insert tenant")
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"message": "Tenant created",
			"name":    req.Name,
		})
	}
}
