package main

import (
	"atlasq/internal/database"
	"atlasq/internal/migration"
	"log"
	"os"

	_ "github.com/golang-migrate/migrate/v4/database/postgres" // <-- เพิ่มบรรทัดนี้!
	_ "github.com/golang-migrate/migrate/v4/source/file"       // <-- สำคัญ!
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: go run cmd/migrate.go up|down")
	}

	migrate := &migration.Migrate{
		Db: &database.PostgreSQL{},
	}

	switch os.Args[1] {
	case "up":
		if err := migrate.MigrateUp(); err != nil {
			log.Fatalf("migration up failed: %v", err)
		}
		log.Println("migration up success")
	case "down":
		if err := migrate.MigrateDown(); err != nil {
			log.Fatalf("migration down failed: %v", err)
		}
		log.Println("migration down success")
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
