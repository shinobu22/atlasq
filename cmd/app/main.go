package main

import (
	"atlasq/internal/database"
	"atlasq/internal/handlers"
	"atlasq/internal/opensearchclient"
	"log"
	"os"
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

	// diagnostic endpoint to trigger a debug log to OpenSearch
	app.Post("/internal/logs/test", func(c *fiber.Ctx) error {
		var body map[string]interface{}
		if err := c.BodyParser(&body); err != nil {
			body = map[string]interface{}{"message": "no body"}
		}
		// send a debug log
		opensearchclient.LogDebug("manual-test", "postman-test", body)

		info := map[string]interface{}{
			"opensearch_url":    os.Getenv("OPENSEARCH_URL"),
			"opensearch_index":  os.Getenv("OPENSEARCH_LOG_INDEX"),
			"enabled_db_sink":   os.Getenv("ENABLE_DB_SINK"),
			"dashboard_webhook": os.Getenv("DASHBOARD_WEBHOOK_URL"),
		}
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "sent", "info": info})
	})

	// API routes
	api := app.Group("/api/v1")

	api.Post("/tenants", handlers.CreateTenant(pool.Pool))
	api.Post("/products", handlers.CreateProduct(pool.Pool))
	api.Post("/orders", handlers.CreateOrder(pool.Pool))
	api.Post("/orders-queue", handlers.CreateOrderQueue(client))

	if err := app.Listen(":8080"); err != nil {
		log.Fatalf("failed to start Fiber app: %v", err)
	}

}
