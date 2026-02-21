package kv

import "context"

// FileSystem provides deletion operations on the underlying file storage.
type FileSystem interface {
	Delete(ctx context.Context, group, name string) error
}
