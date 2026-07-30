package rqlitekv

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/a-h/kv"
)

func TestTryGetInt(t *testing.T) {
	tests := []struct {
		name        string
		input       json.Number
		expected    int
		expectedErr bool
	}{
		{
			name:     "integer",
			input:    json.Number("42"),
			expected: 42,
		},
		{
			name:        "float",
			input:       json.Number("3.14"),
			expectedErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tryGetInt(tt.input)
			if tt.expectedErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("got %d, expected %d", got, tt.expected)
			}
		})
	}
}

func TestNewRowFromValues(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Nanosecond)

	tests := []struct {
		name        string
		values      []any
		expected    kv.Record
		expectedErr bool
	}{
		{
			name: "valid row with json.Number version",
			values: []any{
				"my-key",
				json.Number("3"),
				`{"foo":"bar"}`,
				"map[string]interface {}",
				created.Format(time.RFC3339Nano),
			},
			expected: kv.Record{
				Key:     "my-key",
				Version: 3,
				Value:   []byte(`{"foo":"bar"}`),
				Type:    "map[string]interface {}",
				Created: created,
			},
		},
		{
			name: "null value column",
			values: []any{
				"my-key",
				json.Number("1"),
				nil,
				"map[string]interface {}",
				created.Format(time.RFC3339Nano),
			},
			expected: kv.Record{
				Key:     "my-key",
				Version: 1,
				Value:   nil,
				Type:    "map[string]interface {}",
				Created: created,
			},
		},
		{
			name:        "wrong column count",
			values:      []any{"key", json.Number("1"), "value"},
			expectedErr: true,
		},
		{
			name:        "non-string key",
			values:      []any{42, json.Number("1"), "value", "type", created.Format(time.RFC3339Nano)},
			expectedErr: true,
		},
		{
			name:        "non-number version",
			values:      []any{"key", "not-a-number", "value", "type", created.Format(time.RFC3339Nano)},
			expectedErr: true,
		},
		{
			name:        "invalid created timestamp",
			values:      []any{"key", json.Number("1"), "value", "type", "not-a-time"},
			expectedErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newRowFromValues(tt.values)
			if tt.expectedErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Key != tt.expected.Key {
				t.Errorf("key: got %q, expected %q", got.Key, tt.expected.Key)
			}
			if got.Version != tt.expected.Version {
				t.Errorf("version: got %d, expected %d", got.Version, tt.expected.Version)
			}
			if string(got.Value) != string(tt.expected.Value) {
				t.Errorf("value: got %q, expected %q", got.Value, tt.expected.Value)
			}
			if got.Type != tt.expected.Type {
				t.Errorf("type: got %q, expected %q", got.Type, tt.expected.Type)
			}
			if !got.Created.Equal(tt.expected.Created) {
				t.Errorf("created: got %v, expected %v", got.Created, tt.expected.Created)
			}
		})
	}
}
