package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/a-h/kv"
	"github.com/a-h/kv/postgreskv"
	"github.com/a-h/kv/rqlitekv"
	"github.com/a-h/kv/sqlitekv"
	"github.com/alecthomas/kong"
	"github.com/jackc/pgx/v5/pgxpool"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
	"zombiezen.com/go/sqlite/sqlitex"
)

type GlobalFlags struct {
	Type       string `help:"The type of KV store to use." enum:"sqlite,rqlite,postgres" default:"sqlite"`
	Connection string `help:"The connection string to use." default:"file:data.db?mode=rwc"`
}

func (g GlobalFlags) Store() (kv.Store, error) {
	switch g.Type {
	case "sqlite":
		pool, err := sqlitex.NewPool(g.Connection, sqlitex.PoolOptions{})
		if err != nil {
			return nil, err
		}
		return sqlitekv.New(pool), nil
	case "rqlite":
		u, err := url.Parse(g.Connection)
		if err != nil {
			return nil, err
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
		pool, err := pgxpool.New(context.Background(), g.Connection)
		if err != nil {
			return nil, err
		}
		return postgreskv.New(pool), nil
	default:
		return nil, fmt.Errorf("unknown store type %q", g.Type)
	}
}

type CLI struct {
	GlobalFlags

	Init             InitCommand             `cmd:"init" help:"Initialize the store."`
	Get              GetCommand              `cmd:"get" help:"Get a key."`
	GetPrefix        GetPrefixCommand        `cmd:"get-prefix" help:"Get all keys with a given prefix."`
	GetRange         GetRangeCommand         `cmd:"get-range" help:"Get a range of keys."`
	List             ListCommand             `cmd:"list" help:"List all keys."`
	Put              PutCommand              `cmd:"put" help:"Put a key."`
	Delete           DeleteCommand           `cmd:"delete" help:"Delete a key."`
	DeletePrefix     DeletePrefixCommand     `cmd:"delete-prefix" help:"Delete all keys with a given prefix."`
	DeleteRange      DeleteRangeCommand      `cmd:"delete-range" help:"Delete a range of keys."`
	Count            CountCommand            `cmd:"count" help:"Count the number of keys."`
	CountPrefix      CountPrefixCommand      `cmd:"count-prefix" help:"Count the number of keys with a given prefix."`
	CountRange       CountRangeCommand       `cmd:"count-range" help:"Count the number of keys in a range."`
	Patch            PatchCommand            `cmd:"patch" help:"Patch a key."`
	BenchmarkGet     BenchmarkGetCommand     `cmd:"benchmark-get" help:"Benchmark getting records."`
	BenchmarkPut     BenchmarkPutCommand     `cmd:"benchmark-put" help:"Benchmark putting records."`
	BenchmarkPatch   BenchmarkPatchCommand   `cmd:"benchmark-patch" help:"Benchmark patching records."`
	ConsumerGetBatch ConsumerGetBatchCommand `cmd:"consumer-get-batch" help:"Get a batch of records for a consumer."`
	ConsumerStatus   ConsumerStatusCommand   `cmd:"consumer-status" help:"Show the status of a consumer."`
	ConsumerCommit   ConsumerCommitCommand   `cmd:"consumer-commit" help:"Commit the consumer position to a sequence number."`
}

func main() {
	var cli CLI
	ctx := context.Background()
	kctx := kong.Parse(&cli,
		kong.UsageOnError(),
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.BindTo(cli.GlobalFlags, (*GlobalFlags)(nil)),
	)
	if err := kctx.Run(ctx, cli.GlobalFlags); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
