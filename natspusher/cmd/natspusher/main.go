package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/a-h/kv"
	"github.com/a-h/kv/natspusher"
	"github.com/a-h/kv/postgreskv"
	"github.com/a-h/kv/rqlitekv"
	"github.com/a-h/kv/sqlitekv"
	"github.com/alecthomas/kong"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
	"zombiezen.com/go/sqlite/sqlitex"
)

// CLI represents the command-line interface for the NATS pusher.
type CLI struct {
	// Store configuration
	StoreType       string `help:"The type of KV store to use." enum:"sqlite,rqlite,postgres" default:"sqlite"`
	StoreConnection string `help:"The connection string for the KV store." default:"file:data.db?mode=rwc"`

	// Stream configuration
	StreamName   string        `help:"Name of the KV stream to consume from." required:""`
	ConsumerName string        `help:"Name of the consumer (for tracking position)." default:"natspusher"`
	RecordType   string        `help:"Type filter for records (empty for all types)." default:""`
	BatchSize    int           `help:"Number of records to process in each batch." default:"10"`
	BatchTimeout time.Duration `help:"Maximum time to wait for a batch." default:"5m"`

	// NATS configuration
	NatsURL       string `help:"NATS server URL." default:"nats://localhost:4222"`
	NatsCredsFile string `help:"NATS credentials file path." env:"NATS_CREDS"`
	NatsToken     string `help:"NATS authentication token." env:"NATS_TOKEN"`
	NatsUser      string `help:"NATS username." env:"NATS_USER"`
	NatsPassword  string `help:"NATS password." env:"NATS_PASS"`
	SubjectPrefix string `help:"NATS subject prefix for published messages." default:"kv"`
	MaxRetries    int    `help:"Maximum number of retries for failed publishes." default:"3"`

	// Additional headers
	HeaderSource      string `help:"Source header to add to NATS messages." default:"kv-store"`
	HeaderEnvironment string `help:"Environment header to add to NATS messages." default:"development"`

	// Logging
	LogLevel  string `help:"Log level (debug, info, warn, error)." enum:"debug,info,warn,error" default:"info"`
	LogFormat string `help:"Log format (json, text)." enum:"json,text" default:"text"`
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("natspusher"),
		kong.Description("Push KV store stream data to NATS"),
		kong.UsageOnError(),
	)

	// Set up context with signal handling.
	appCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Configure logging.
	logger := setupLogger(cli.LogLevel, cli.LogFormat)
	slog.SetDefault(logger)

	if err := run(appCtx, cli, logger); err != nil {
		logger.Error("Application failed", slog.String("error", err.Error()))
		ctx.Exit(1)
	}
}

func run(ctx context.Context, cli CLI, logger *slog.Logger) error {
	store, err := createStore(cli)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	if err := store.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}

	// Determine record type filter.
	recordType := kv.TypeAll
	if cli.RecordType != "" {
		recordType = kv.Type(cli.RecordType)
	}

	consumer := kv.NewStreamConsumer(ctx, store, cli.StreamName, cli.ConsumerName, recordType)
	logger.Info("Created stream consumer",
		slog.String("stream", cli.StreamName),
		slog.String("consumer", cli.ConsumerName),
		slog.String("type", cli.RecordType),
	)

	nc, err := connectNATS(cli)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	logger.Info("Connected to NATS", slog.String("url", cli.NatsURL))

	config := natspusher.Config{
		SubjectPrefix: cli.SubjectPrefix,
		MaxRetries:    cli.MaxRetries,
		Logger:        logger,
		Headers: nats.Header{
			"source":      []string{cli.HeaderSource},
			"environment": []string{cli.HeaderEnvironment},
		},
	}

	pusher, err := natspusher.New(nc, consumer, config)
	if err != nil {
		return fmt.Errorf("failed to create pusher: %w", err)
	}

	logger.Info("Starting NATS pusher...")
	return pusher.Run(ctx)
}

func createStore(cli CLI) (kv.Store, error) {
	switch cli.StoreType {
	case "sqlite":
		pool, err := sqlitex.NewPool(cli.StoreConnection, sqlitex.PoolOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite pool: %w", err)
		}
		return sqlitekv.New(pool), nil

	case "rqlite":
		u, err := url.Parse(cli.StoreConnection)
		if err != nil {
			return nil, fmt.Errorf("failed to parse rqlite connection string: %w", err)
		}
		user := u.Query().Get("user")
		password := u.Query().Get("password")
		// Remove user and password from the connection string.
		u.RawQuery = ""
		client := rqlitehttp.NewClient(u.String(), nil)
		if user != "" && password != "" {
			client.SetBasicAuth(user, password)
		}
		return rqlitekv.New(client), nil

	case "postgres":
		pool, err := pgxpool.New(context.Background(), cli.StoreConnection)
		if err != nil {
			return nil, fmt.Errorf("failed to create Postgres pool: %w", err)
		}
		return postgreskv.New(pool), nil

	default:
		return nil, fmt.Errorf("unsupported store type: %s", cli.StoreType)
	}
}

func connectNATS(cli CLI) (*nats.Conn, error) {
	var opts []nats.Option

	// Add authentication options.
	if cli.NatsCredsFile != "" {
		opts = append(opts, nats.UserCredentials(cli.NatsCredsFile))
	} else if cli.NatsToken != "" {
		opts = append(opts, nats.Token(cli.NatsToken))
	} else if cli.NatsUser != "" && cli.NatsPassword != "" {
		opts = append(opts, nats.UserInfo(cli.NatsUser, cli.NatsPassword))
	}

	// Add connection name for identification.
	opts = append(opts, nats.Name("natspusher"))

	return nats.Connect(cli.NatsURL, opts...)
}

func setupLogger(level, format string) *slog.Logger {
	var logLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}
