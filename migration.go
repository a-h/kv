package kv

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
)

// MigrationExecutor defines the interface for executing SQL statements.
type MigrationExecutor interface {
	Exec(ctx context.Context, sql string) error
	// GetVersion returns the current migration version from the database.
	GetVersion(ctx context.Context) (int, error)
	// SetVersion atomically executes a migration and updates the version in a single transaction.
	SetVersion(ctx context.Context, migrationSQL string, version int) error
	// AcquireMigrationLock acquires an exclusive lock for migration operations.
	AcquireMigrationLock(ctx context.Context) error
	// ReleaseMigrationLock releases the migration lock.
	ReleaseMigrationLock(ctx context.Context) error
}

// MigrationRunner handles database migrations using embedded filesystems.
type MigrationRunner struct {
	executor     MigrationExecutor
	migrationsFS embed.FS
}

// NewMigrationRunner creates a new migration runner.
func NewMigrationRunner(executor MigrationExecutor, migrationsFS embed.FS) *MigrationRunner {
	return &MigrationRunner{
		executor:     executor,
		migrationsFS: migrationsFS,
	}
}

// Migration represents a database migration.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// getMigrations reads all migration files from the embedded filesystem.
func (mr *MigrationRunner) getMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(mr.migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version, name, err := parseMigrationFilename(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to parse migration filename %s: %w", entry.Name(), err)
		}
		sqlBytes, err := fs.ReadFile(mr.migrationsFS, path.Join("migrations", entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", entry.Name(), err)
		}
		migrations = append(migrations, Migration{
			Version: version,
			Name:    name,
			SQL:     strings.TrimSpace(string(sqlBytes)),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// Migrate runs all pending migrations.
func (mr *MigrationRunner) Migrate(ctx context.Context) error {
	if err := mr.executor.AcquireMigrationLock(ctx); err != nil {
		return fmt.Errorf("failed to acquire migration lock: %w", err)
	}
	defer func() {
		_ = mr.executor.ReleaseMigrationLock(ctx)
	}()

	currentVersion, err := mr.executor.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}
	migrations, err := mr.getMigrations()
	if err != nil {
		return fmt.Errorf("failed to get migrations: %w", err)
	}
	for _, migration := range migrations {
		if migration.Version <= currentVersion {
			continue
		}
		if migration.Version == 1 {
			if err := mr.executor.Exec(ctx, migration.SQL); err != nil {
				return fmt.Errorf("failed to apply migration %d (%s): %w", migration.Version, migration.Name, err)
			}
			continue
		}
		if err := mr.executor.SetVersion(ctx, migration.SQL, migration.Version); err != nil {
			return fmt.Errorf("failed to apply migration %d (%s): %w", migration.Version, migration.Name, err)
		}
	}
	return nil
}

// parseMigrationFilename extracts version and name from migration filename.
func parseMigrationFilename(filename string) (version int, name string, err error) {
	nameWithoutExt := strings.TrimSuffix(filename, ".sql")

	parts := strings.SplitN(nameWithoutExt, "_", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid migration filename format: %s", filename)
	}

	version, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", fmt.Errorf("invalid version number in filename %s: %w", filename, err)
	}

	name = strings.ReplaceAll(parts[1], "_", " ")
	return version, name, nil
}
