package rqlitekv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/a-h/kv"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func NewScheduler(client *rqlitehttp.Client) *Scheduler {
	return &Scheduler{
		Rqlite: New(client),
	}
}

type Scheduler struct {
	*Rqlite
}

// Task methods for RQLite
func (rq *Rqlite) New(ctx context.Context, task kv.Task) error {
	stmt := rqlitehttp.SQLStatement{
		SQL: `insert into tasks (id, name, payload, status, created, scheduled_for, retry_count, max_retries, timeout_seconds)
values (:id, :name, :payload, :status, :created, :scheduled_for, :retry_count, :max_retries, :timeout_seconds);`,
		NamedParams: map[string]any{
			"id":              task.ID,
			"name":            task.Name,
			"payload":         task.Payload,
			"status":          string(task.Status),
			"created":         task.Created.Format(time.RFC3339Nano),
			"scheduled_for":   task.ScheduledFor.Format(time.RFC3339Nano),
			"retry_count":     task.RetryCount,
			"max_retries":     task.MaxRetries,
			"timeout_seconds": task.TimeoutSeconds,
		},
	}
	_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return fmt.Errorf("task create: %w", err)
	}
	return nil
}

func (rq *Rqlite) Get(ctx context.Context, id string) (task kv.Task, ok bool, err error) {
	stmt := rqlitehttp.SQLStatement{
		SQL: `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where id = :id limit 1;`,
		NamedParams: map[string]any{
			"id": id,
		},
	}

	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	qr, err := rq.Client.Query(ctx, rqlitehttp.SQLStatements{stmt}, opts)
	if err != nil {
		return task, false, fmt.Errorf("task get: %w", err)
	}

	if len(qr.Results) == 0 || len(qr.Results[0].Values) == 0 {
		return task, false, nil
	}

	if qr.Results[0].Error != "" {
		return task, false, fmt.Errorf("task get: %s", qr.Results[0].Error)
	}

	if len(qr.Results[0].Values) != 1 {
		return task, false, fmt.Errorf("task get: multiple rows found for id %q", id)
	}

	task, err = rq.parseTaskFromValues(qr.Results[0].Values[0])
	if err != nil {
		return task, false, fmt.Errorf("task get: %w", err)
	}

	return task, true, nil
}

func (rq *Rqlite) List(ctx context.Context, status kv.TaskStatus, name string, offset, limit int) ([]kv.Task, error) {
	sql, namedParams := getTaskListQuery(status, name, offset, limit)

	stmt := rqlitehttp.SQLStatement{
		SQL:         sql,
		NamedParams: namedParams,
	}

	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	qr, err := rq.Client.Query(ctx, rqlitehttp.SQLStatements{stmt}, opts)
	if err != nil {
		return nil, fmt.Errorf("task list: %w", err)
	}

	if qr.Results[0].Error != "" {
		return nil, fmt.Errorf("task list: %s", qr.Results[0].Error)
	}

	tasks := make([]kv.Task, len(qr.Results[0].Values))
	for i, values := range qr.Results[0].Values {
		task, err := rq.parseTaskFromValues(values)
		if err != nil {
			return nil, fmt.Errorf("task list: row %d: %w", i, err)
		}
		tasks[i] = task
	}

	return tasks, nil
}

// getTaskListQuery builds the SQL query and parameters for TaskList based on the given filters.
func getTaskListQuery(status kv.TaskStatus, name string, offset, limit int) (string, map[string]any) {
	const (
		queryAll             = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks order by created desc limit :limit offset :offset`
		queryByStatus        = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where status = :status order by created desc limit :limit offset :offset`
		queryByName          = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where name = :name order by created desc limit :limit offset :offset`
		queryByStatusAndName = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where status = :status and name = :name order by created desc limit :limit offset :offset`
	)

	namedParams := map[string]any{
		"offset": offset,
		"limit":  limit,
	}

	if status != "" && name != "" {
		namedParams["status"] = string(status)
		namedParams["name"] = name
		return queryByStatusAndName, namedParams
	}

	if status != "" {
		namedParams["status"] = string(status)
		return queryByStatus, namedParams
	}

	if name != "" {
		namedParams["name"] = name
		return queryByName, namedParams
	}

	return queryAll, namedParams
}

// getTaskGetNextPendingFindStatements builds the find SQL statements for TaskGetNextPending based on task types.
func getTaskGetNextPendingFindStatements(now time.Time, taskTypes []string) rqlitehttp.SQLStatements {
	const (
		sqlAllTypes = `select id from tasks 
where status = 'pending' 
  and scheduled_for <= :now
  and (locked_by = '' or locked_by is null or lock_expires_at <= :now)
order by scheduled_for asc
limit 1;`

		sqlSpecificTypes = `select id from tasks 
where status = 'pending' 
  and scheduled_for <= :now
  and name in (select value from json_each(:task_types))
  and (locked_by = '' or locked_by is null or lock_expires_at <= :now)
order by scheduled_for asc
limit 1;`
	)

	namedParams := map[string]any{
		"now": now.Format(time.RFC3339Nano),
	}

	if len(taskTypes) == 0 {
		return rqlitehttp.SQLStatements{{
			SQL:         sqlAllTypes,
			NamedParams: namedParams,
		}}
	}

	// Use JSON array parameter for safe handling of task types.
	taskTypesJSON, err := json.Marshal(taskTypes)
	if err != nil {
		// Fallback to all types if JSON marshaling fails.
		return rqlitehttp.SQLStatements{{
			SQL:         sqlAllTypes,
			NamedParams: namedParams,
		}}
	}

	namedParams["task_types"] = string(taskTypesJSON)
	return rqlitehttp.SQLStatements{{
		SQL:         sqlSpecificTypes,
		NamedParams: namedParams,
	}}
}

func (rq *Rqlite) Lock(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (task kv.Task, locked bool, err error) {
	now := rq.Now()
	lockExpiresAt := now.Add(lockDuration)

	// RQLite doesn't support UPDATE...RETURNING in a single statement like SQLite,
	// so we need to do this in two steps: first find the task, then update it.

	findStmts := getTaskGetNextPendingFindStatements(now, taskTypes)

	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	qr, err := rq.Client.Query(ctx, findStmts, opts)
	if err != nil {
		return task, false, fmt.Errorf("task get next pending: find: %w", err)
	}

	if len(qr.Results) == 0 || len(qr.Results[0].Values) == 0 {
		return task, false, nil // No pending tasks.
	}

	if qr.Results[0].Error != "" {
		return task, false, fmt.Errorf("task get next pending: find: %s", qr.Results[0].Error)
	}

	taskID, ok := qr.Results[0].Values[0][0].(string)
	if !ok {
		return task, false, fmt.Errorf("task get next pending: invalid task id type: %T", qr.Results[0].Values[0][0])
	}

	// Now update and get the task in a transaction.
	updateStmt := rqlitehttp.SQLStatement{
		SQL: `update tasks set 
status = 'running',
locked_by = :runner_id,
locked_at = :now,
lock_expires_at = :lock_expires_at
where id = :id
  and status = 'pending'
  and scheduled_for <= :now
  and (locked_by = '' or locked_by is null or lock_expires_at <= :now);`,
		NamedParams: map[string]any{
			"id":              taskID,
			"runner_id":       runnerID,
			"now":             now.Format(time.RFC3339Nano),
			"lock_expires_at": lockExpiresAt.Format(time.RFC3339Nano),
		},
	}

	getStmt := rqlitehttp.SQLStatement{
		SQL: `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where id = :id;`,
		NamedParams: map[string]any{
			"id": taskID,
		},
	}

	// Execute both in a transaction.
	rowsAffected, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{updateStmt})
	if err != nil {
		return task, false, fmt.Errorf("task get next pending: update: %w", err)
	}

	if len(rowsAffected) == 0 || rowsAffected[0] == 0 {
		// Task was already locked by another runner.
		return task, false, nil
	}

	// Get the updated task.
	qr, err = rq.Client.Query(ctx, rqlitehttp.SQLStatements{getStmt}, opts)
	if err != nil {
		return task, false, fmt.Errorf("task get next pending: get: %w", err)
	}

	if len(qr.Results) == 0 || len(qr.Results[0].Values) == 0 {
		return task, false, fmt.Errorf("task get next pending: task disappeared")
	}

	if qr.Results[0].Error != "" {
		return task, false, fmt.Errorf("task get next pending: get: %s", qr.Results[0].Error)
	}

	task, err = rq.parseTaskFromValues(qr.Results[0].Values[0])
	if err != nil {
		return task, false, fmt.Errorf("task get next pending: parse: %w", err)
	}

	return task, true, nil
}

func (rq *Rqlite) Cancel(ctx context.Context, id string) error {
	stmt := rqlitehttp.SQLStatement{
		SQL: `update tasks set status = 'cancelled' where id = :id and status in ('pending', 'running');`,
		NamedParams: map[string]any{
			"id": id,
		},
	}

	_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return fmt.Errorf("task cancel: %w", err)
	}
	return nil
}

func (rq *Rqlite) Release(ctx context.Context, id string, runnerID string, status kv.TaskStatus, errorMessage string) error {
	now := rq.Now()

	switch status {
	case kv.TaskStatusCompleted:
		stmt := rqlitehttp.SQLStatement{
			SQL: `update tasks set 
				status = 'completed',
				completed_at = :now,
				last_error = '',
				locked_by = '',
				locked_at = null,
				lock_expires_at = null
			where id = :id and locked_by = :runner_id;`,
			NamedParams: map[string]any{
				"id":        id,
				"runner_id": runnerID,
				"now":       now.Format(time.RFC3339Nano),
			},
		}

		_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
		if err != nil {
			return fmt.Errorf("task release (completed): %w", err)
		}

	case kv.TaskStatusFailed:
		// First get the current task to check retry logic.
		task, ok, err := rq.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("task release: failed to get task for retry logic: %w", err)
		}
		if !ok {
			return fmt.Errorf("task release: task not found: %s", id)
		}

		task.RetryCount++

		if task.RetryCount >= task.MaxRetries {
			// Max retries exceeded, mark as permanently failed.
			stmt := rqlitehttp.SQLStatement{
				SQL: `update tasks set 
					status = 'failed',
					completed_at = :now,
					last_error = :last_error,
					retry_count = :retry_count,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null
				where id = :id and locked_by = :runner_id;`,
				NamedParams: map[string]any{
					"id":          id,
					"runner_id":   runnerID,
					"now":         now.Format(time.RFC3339Nano),
					"last_error":  errorMessage,
					"retry_count": task.RetryCount,
				},
			}

			_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
			if err != nil {
				return fmt.Errorf("task release (failed permanently): %w", err)
			}
		} else {
			// Reset to pending for retry with exponential backoff.
			backoffSeconds := 60 * (1 << task.RetryCount) // 60s, 120s, 240s, etc.
			scheduledFor := now.Add(time.Duration(backoffSeconds) * time.Second)

			stmt := rqlitehttp.SQLStatement{
				SQL: `update tasks set 
					status = 'pending',
					scheduled_for = :scheduled_for,
					last_error = :last_error,
					retry_count = :retry_count,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null
				where id = :id and locked_by = :runner_id;`,
				NamedParams: map[string]any{
					"id":            id,
					"runner_id":     runnerID,
					"scheduled_for": scheduledFor.Format(time.RFC3339Nano),
					"last_error":    errorMessage,
					"retry_count":   task.RetryCount,
				},
			}

			_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
			if err != nil {
				return fmt.Errorf("task release (retry): %w", err)
			}
		}

	case kv.TaskStatusCancelled:
		if errorMessage != "" {
			stmt := rqlitehttp.SQLStatement{
				SQL: `update tasks set 
					status = 'cancelled',
					completed_at = :now,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null,
					last_error = :last_error
				where id = :id and locked_by = :runner_id;`,
				NamedParams: map[string]any{
					"id":         id,
					"runner_id":  runnerID,
					"now":        now.Format(time.RFC3339Nano),
					"last_error": errorMessage,
				},
			}

			_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
			if err != nil {
				return fmt.Errorf("task release (cancelled): %w", err)
			}
		} else {
			stmt := rqlitehttp.SQLStatement{
				SQL: `update tasks set 
					status = 'cancelled',
					completed_at = :now,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null
				where id = :id and locked_by = :runner_id;`,
				NamedParams: map[string]any{
					"id":        id,
					"runner_id": runnerID,
					"now":       now.Format(time.RFC3339Nano),
				},
			}

			_, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
			if err != nil {
				return fmt.Errorf("task release (cancelled): %w", err)
			}
		}

	default:
		return fmt.Errorf("task release: unsupported status: %s", status)
	}

	return nil
}

// parseTaskFromValues parses a query result row into a Task struct
// Expected columns: id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at
func (rq *Rqlite) parseTaskFromValues(values []any) (task kv.Task, err error) {
	if len(values) != 15 {
		return task, fmt.Errorf("expected 15 columns, got %d", len(values))
	}

	var ok bool

	// id (string)
	task.ID, ok = values[0].(string)
	if !ok {
		return task, fmt.Errorf("id: expected string, got %T", values[0])
	}

	// name (string)
	task.Name, ok = values[1].(string)
	if !ok {
		return task, fmt.Errorf("name: expected string, got %T", values[1])
	}

	// payload (string - base64 encoded binary data in RQLite)
	if values[2] != nil {
		payloadStr, ok := values[2].(string)
		if !ok {
			return task, fmt.Errorf("payload: expected string, got %T", values[2])
		}
		decoded, err := base64.StdEncoding.DecodeString(payloadStr)
		if err != nil {
			return task, fmt.Errorf("payload: failed to decode base64: %w", err)
		}
		task.Payload = decoded
	}

	// status (string)
	statusStr, ok := values[3].(string)
	if !ok {
		return task, fmt.Errorf("status: expected string, got %T", values[3])
	}
	task.Status = kv.TaskStatus(statusStr)

	// created (string - RFC3339Nano)
	createdStr, ok := values[4].(string)
	if !ok {
		return task, fmt.Errorf("created: expected string, got %T", values[4])
	}
	task.Created, err = time.Parse(time.RFC3339Nano, createdStr)
	if err != nil {
		return task, fmt.Errorf("created: failed to parse time: %w", err)
	}

	// scheduled_for (string - RFC3339Nano)
	scheduledForStr, ok := values[5].(string)
	if !ok {
		return task, fmt.Errorf("scheduled_for: expected string, got %T", values[5])
	}
	task.ScheduledFor, err = time.Parse(time.RFC3339Nano, scheduledForStr)
	if err != nil {
		return task, fmt.Errorf("scheduled_for: failed to parse time: %w", err)
	}

	// started_at (nullable string - RFC3339Nano)
	if values[6] != nil {
		startedAtStr, ok := values[6].(string)
		if !ok {
			return task, fmt.Errorf("started_at: expected string, got %T", values[6])
		}
		startedAt, err := time.Parse(time.RFC3339Nano, startedAtStr)
		if err != nil {
			return task, fmt.Errorf("started_at: failed to parse time: %w", err)
		}
		task.StartedAt = &startedAt
	}

	// completed_at (nullable string - RFC3339Nano)
	if values[7] != nil {
		completedAtStr, ok := values[7].(string)
		if !ok {
			return task, fmt.Errorf("completed_at: expected string, got %T", values[7])
		}
		completedAt, err := time.Parse(time.RFC3339Nano, completedAtStr)
		if err != nil {
			return task, fmt.Errorf("completed_at: failed to parse time: %w", err)
		}
		task.CompletedAt = &completedAt
	}

	// last_error (string)
	if values[8] != nil {
		task.LastError, ok = values[8].(string)
		if !ok {
			return task, fmt.Errorf("last_error: expected string, got %T", values[8])
		}
	}

	// retry_count (int)
	task.RetryCount, err = tryGetInt(values[9])
	if err != nil {
		return task, fmt.Errorf("retry_count: %w", err)
	}

	// max_retries (int)
	task.MaxRetries, err = tryGetInt(values[10])
	if err != nil {
		return task, fmt.Errorf("max_retries: %w", err)
	}

	// timeout_seconds (int)
	task.TimeoutSeconds, err = tryGetInt(values[11])
	if err != nil {
		return task, fmt.Errorf("timeout_seconds: %w", err)
	}

	// locked_by (string)
	if values[12] != nil {
		task.LockedBy, ok = values[12].(string)
		if !ok {
			return task, fmt.Errorf("locked_by: expected string, got %T", values[12])
		}
	}

	// locked_at (nullable string - RFC3339Nano)
	if values[13] != nil {
		lockedAtStr, ok := values[13].(string)
		if !ok {
			return task, fmt.Errorf("locked_at: expected string, got %T", values[13])
		}
		lockedAt, err := time.Parse(time.RFC3339Nano, lockedAtStr)
		if err != nil {
			return task, fmt.Errorf("locked_at: failed to parse time: %w", err)
		}
		task.LockedAt = &lockedAt
	}

	// lock_expires_at (nullable string - RFC3339Nano)
	if values[14] != nil {
		lockExpiresAtStr, ok := values[14].(string)
		if !ok {
			return task, fmt.Errorf("lock_expires_at: expected string, got %T", values[14])
		}
		lockExpiresAt, err := time.Parse(time.RFC3339Nano, lockExpiresAtStr)
		if err != nil {
			return task, fmt.Errorf("lock_expires_at: failed to parse time: %w", err)
		}
		task.LockExpiresAt = &lockExpiresAt
	}

	return task, nil
}
