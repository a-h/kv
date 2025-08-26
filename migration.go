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
	QueryIntScalar(ctx context.Context, sql string) (int, error)
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
		if entry.Name() == "get_current_version.sql" {
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

// getCurrentVersion returns the current schema version from the database.
func (mr *MigrationRunner) getCurrentVersion(ctx context.Context) (int, error) {
	versionQueryBytes, err := fs.ReadFile(mr.migrationsFS, path.Join("migrations", "get_current_version.sql"))
	if err != nil {
		return 0, fmt.Errorf("failed to read get_current_version.sql: %w", err)
	}
	version, err := mr.executor.QueryIntScalar(ctx, strings.TrimSpace(string(versionQueryBytes)))
	if err != nil {
		// If the migration_version table doesn't exist yet, that's expected for a fresh database.
		// Return 0 to indicate no migrations have been applied.
		if strings.Contains(err.Error(), "no such table") || strings.Contains(err.Error(), "relation") && strings.Contains(err.Error(), "does not exist") {
			return 0, nil
		}
		// For other errors (connection issues, syntax errors, etc.), propagate them.
		return 0, fmt.Errorf("failed to query current migration version: %w", err)
	}
	return version, nil
}

// Migrate runs all pending migrations.
func (mr *MigrationRunner) Migrate(ctx context.Context) error {
	currentVersion, err := mr.getCurrentVersion(ctx)
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
		if err := mr.executor.Exec(ctx, migration.SQL); err != nil {
			return fmt.Errorf("failed to apply migration %d (%s): %w", migration.Version, migration.Name, err)
		}
		if migration.Version <= 1 {
			continue
		}
		updateSQL := fmt.Sprintf("INSERT INTO migration_version (version) VALUES (%d)", migration.Version)
		if err := mr.executor.Exec(ctx, updateSQL); err != nil {
			return fmt.Errorf("failed to update migration version for migration %d: %w", migration.Version, err)
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
