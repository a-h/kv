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

	Init      InitCommand      `cmd:"init" help:"Initialize the store."`
	Get       GetCommand       `cmd:"get" help:"Get operations."`
	Delete    DeleteCommand    `cmd:"delete" help:"Delete operations."`
	Count     CountCommand     `cmd:"count" help:"Count operations."`
	Benchmark BenchmarkCommand `cmd:"benchmark" help:"Benchmark operations."`
	List      ListCommand      `cmd:"list" help:"List all keys."`
	Put       PutCommand       `cmd:"put" help:"Put a key."`
	Patch     PatchCommand     `cmd:"patch" help:"Patch a key."`
	Stream    StreamCommand    `cmd:"stream" help:"Stream operations (get, seq, trim)."`
	Consumer  ConsumerCommand  `cmd:"consumer" help:"Consumer operations (get, status, commit)."`
	Lock      LockCommand      `cmd:"lock" help:"Lock operations (acquire, release)."`
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
