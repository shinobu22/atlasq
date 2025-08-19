package migration

import (
	"atlasq/internal/database"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
)

type Migrate struct {
	Db *database.PostgreSQL
}

func (m *Migrate) migrationPath() string {
	return "file:///Users/rosemary/projects/gosaas/atlasq/internal/migration"
}

func (m *Migrate) MigrateUp() error {

	fmt.Println("Begin migration...")

	mig, err := migrate.New(m.migrationPath(), m.Db.ConnectionURI())

	if err != nil {
		return err
	}

	fmt.Println(mig.Version())

	fmt.Println("Starting migration...")

	err = mig.Up()
	if err != nil && err != migrate.ErrNoChange {
		fmt.Println("End with ERROR -> ", err)
		return err
	}

	fmt.Println("End with no ERROR!")
	return nil
}

func (m *Migrate) MigrateDown() error {
	mig, err := migrate.New(m.migrationPath(), m.Db.ConnectionURI())
	if err != nil {
		return err
	}
	err = mig.Down()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}
