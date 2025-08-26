package main

import (
	"atlasq/internal/database"
	"atlasq/internal/handlers"
	"log"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
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

	app.Get("/", func(c *fiber.Ctx) error { return c.SendString("AtlasQ") })

	// API routes
	api := app.Group("/api/v1")

	api.Post("/tenants", handlers.CreateTenant(pool.Pool))
	api.Post("/products", handlers.CreateProduct(pool.Pool))
	api.Post("/orders-old", handlers.CreateOrderOld(pool.Pool))
	api.Post("/orders-queue", handlers.CreateOrderQueue(client))
	api.Get("/orders/:id", handlers.GetOrderByID(pool.Pool))
	// api.Post("/orders", handlers.CreateOrder(pool.Pool))
	api.Post("/stock-issue", handlers.StockIssueHandler(pool.Pool))

	if err := app.Listen(":8080"); err != nil {
		log.Fatalf("failed to start Fiber app: %v", err)
	}

}
