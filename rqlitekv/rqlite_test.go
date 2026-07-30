package rqlitekv

import (
	"testing"

	"github.com/a-h/kv/tests"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func TestRqlite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client, err := rqlitehttp.NewClient("http://localhost:4001", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Username and password configured in auth.json.
	client.SetBasicAuth("admin", "secret")

	store := NewStore(client)
	scheduler := NewScheduler(client)
	tests.Run(t, store, scheduler)
}
