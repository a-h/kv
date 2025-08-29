package postgreskv

import (
	"context"
	"fmt"
	"time"

	"github.com/a-h/kv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewScheduler(pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		Postgres: New(pool),
	}
}

type Scheduler struct {
	*Postgres
}

func (p *Postgres) New(ctx context.Context, task kv.Task) error {
	_, err := p.Pool.Exec(ctx, `
		insert into tasks (id, name, payload, status, created, scheduled_for, retry_count, max_retries, timeout_seconds)
		values (@id, @name, @payload, @status, @created, @scheduled_for, @retry_count, @max_retries, @timeout_seconds)`,
		pgx.NamedArgs{
			"id":              task.ID,
			"name":            task.Name,
			"payload":         task.Payload,
			"status":          string(task.Status),
			"created":         task.Created,
			"scheduled_for":   task.ScheduledFor,
			"retry_count":     task.RetryCount,
			"max_retries":     task.MaxRetries,
			"timeout_seconds": task.TimeoutSeconds,
		})
	if err != nil {
		return fmt.Errorf("task create: %w", err)
	}
	return nil
}

func (p *Postgres) Get(ctx context.Context, id string) (task kv.Task, ok bool, err error) {
	row := p.Pool.QueryRow(ctx, `
		select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at
		from tasks where id = @id limit 1`,
		pgx.NamedArgs{"id": id})

	err = row.Scan(
		&task.ID, &task.Name, &task.Payload, &task.Status,
		&task.Created, &task.ScheduledFor, &task.StartedAt, &task.CompletedAt, &task.LastError,
		&task.RetryCount, &task.MaxRetries, &task.TimeoutSeconds, &task.LockedBy,
		&task.LockedAt, &task.LockExpiresAt)

	if err == pgx.ErrNoRows {
		return task, false, nil
	}
	if err != nil {
		return task, false, err
	}

	return task, true, nil
}

func (p *Postgres) List(ctx context.Context, status kv.TaskStatus, name string, offset, limit int) ([]kv.Task, error) {
	sql, args := getTaskListQuery(status, name, offset, limit)

	rows, err := p.Pool.Query(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []kv.Task
	for rows.Next() {
		var task kv.Task

		err := rows.Scan(
			&task.ID, &task.Name, &task.Payload, &task.Status,
			&task.Created, &task.ScheduledFor, &task.StartedAt, &task.CompletedAt, &task.LastError,
			&task.RetryCount, &task.MaxRetries, &task.TimeoutSeconds, &task.LockedBy,
			&task.LockedAt, &task.LockExpiresAt)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, task)
	}

	return tasks, rows.Err()
}

// getTaskListQuery builds the SQL query and parameters for TaskList based on the given filters.
func getTaskListQuery(status kv.TaskStatus, name string, offset, limit int) (string, pgx.NamedArgs) {
	const (
		queryAll             = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks order by created desc limit @limit offset @offset`
		queryByStatus        = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where status = @status order by created desc limit @limit offset @offset`
		queryByName          = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where name = @name order by created desc limit @limit offset @offset`
		queryByStatusAndName = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where status = @status and name = @name order by created desc limit @limit offset @offset`
	)

	args := pgx.NamedArgs{
		"offset": offset,
		"limit":  limit,
	}

	if status != "" && name != "" {
		args["status"] = string(status)
		args["name"] = name
		return queryByStatusAndName, args
	}

	if status != "" {
		args["status"] = string(status)
		return queryByStatus, args
	}

	if name != "" {
		args["name"] = name
		return queryByName, args
	}

	return queryAll, args
}

// getTaskGetNextPendingQuery builds the SQL query and parameters for TaskGetNextPending based on task types.
func getTaskGetNextPendingQuery(runnerID string, now, lockExpiresAt time.Time, taskTypes []string) (string, pgx.NamedArgs) {
	const (
		queryAllTypes = `
			update tasks set 
				status = 'running',
				locked_by = @runner_id,
				locked_at = @now,
				lock_expires_at = @lock_expires_at
			where id = (
				select id from tasks 
				where status = 'pending' 
					and scheduled_for <= @now
					and (locked_by = '' or locked_by is null or lock_expires_at <= @now)
				order by scheduled_for asc
				limit 1
				for update skip locked
			)
			returning id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at`

		querySpecificTypes = `
			update tasks set 
				status = 'running',
				locked_by = @runner_id,
				locked_at = @now,
				lock_expires_at = @lock_expires_at
			where id = (
				select id from tasks 
				where status = 'pending' 
					and scheduled_for <= @now
					and name = any(@task_types)
					and (locked_by = '' or locked_by is null or lock_expires_at <= @now)
				order by scheduled_for asc
				limit 1
				for update skip locked
			)
			returning id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at`
	)

	args := pgx.NamedArgs{
		"runner_id":       runnerID,
		"now":             now,
		"lock_expires_at": lockExpiresAt,
	}

	if len(taskTypes) == 0 {
		return queryAllTypes, args
	}

	args["task_types"] = taskTypes
	return querySpecificTypes, args
}

func (p *Postgres) Lock(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (task kv.Task, locked bool, err error) {
	now := p.Now()
	lockExpiresAt := now.Add(lockDuration)

	sql, args := getTaskGetNextPendingQuery(runnerID, now, lockExpiresAt, taskTypes)

	row := p.Pool.QueryRow(ctx, sql, args)

	err = row.Scan(
		&task.ID, &task.Name, &task.Payload, &task.Status,
		&task.Created, &task.ScheduledFor, &task.StartedAt, &task.CompletedAt, &task.LastError,
		&task.RetryCount, &task.MaxRetries, &task.TimeoutSeconds, &task.LockedBy,
		&task.LockedAt, &task.LockExpiresAt)

	if err == pgx.ErrNoRows {
		return task, false, nil
	}
	if err != nil {
		return task, false, fmt.Errorf("task get next pending: %w", err)
	}

	return task, true, nil
}

func (p *Postgres) Cancel(ctx context.Context, id string) error {
	_, err := p.Pool.Exec(ctx, `
		update tasks set status = 'cancelled' 
		where id = @id and status in ('pending', 'running')`,
		pgx.NamedArgs{
			"id": id,
		})

	if err != nil {
		return fmt.Errorf("task cancel: %w", err)
	}
	return nil
}

func (p *Postgres) Release(ctx context.Context, id string, runnerID string, status kv.TaskStatus, errorMessage string) error {
	now := p.Now()

	switch status {
	case kv.TaskStatusCompleted:
		_, err := p.Pool.Exec(ctx, `
			update tasks set 
				status = 'completed',
				completed_at = @now,
				last_error = '',
				locked_by = '',
				locked_at = null,
				lock_expires_at = null
			where id = @id and locked_by = @runner_id`,
			pgx.NamedArgs{
				"id":        id,
				"runner_id": runnerID,
				"now":       now,
			})
		if err != nil {
			return fmt.Errorf("task release (completed): %w", err)
		}

	case kv.TaskStatusFailed:
		// First get the current task to check retry logic.
		task, ok, err := p.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("task release: failed to get task for retry logic: %w", err)
		}
		if !ok {
			return fmt.Errorf("task release: task not found: %s", id)
		}

		task.RetryCount++

		if task.RetryCount >= task.MaxRetries {
			// Max retries exceeded, mark as permanently failed.
			_, err := p.Pool.Exec(ctx, `
				update tasks set 
					status = 'failed',
					completed_at = @now,
					last_error = @last_error,
					retry_count = @retry_count,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null
				where id = @id and locked_by = @runner_id`,
				pgx.NamedArgs{
					"id":          id,
					"runner_id":   runnerID,
					"now":         now,
					"last_error":  errorMessage,
					"retry_count": task.RetryCount,
				})
			if err != nil {
				return fmt.Errorf("task release (failed permanently): %w", err)
			}
		} else {
			// Reset to pending for retry with exponential backoff.
			backoffSeconds := 60 * (1 << task.RetryCount) // 60s, 120s, 240s, etc.
			scheduledFor := now.Add(time.Duration(backoffSeconds) * time.Second)

			_, err := p.Pool.Exec(ctx, `
				update tasks set 
					status = 'pending',
					scheduled_for = @scheduled_for,
					last_error = @last_error,
					retry_count = @retry_count,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null
				where id = @id and locked_by = @runner_id`,
				pgx.NamedArgs{
					"id":            id,
					"runner_id":     runnerID,
					"scheduled_for": scheduledFor,
					"last_error":    errorMessage,
					"retry_count":   task.RetryCount,
				})
			if err != nil {
				return fmt.Errorf("task release (retry): %w", err)
			}
		}

	case kv.TaskStatusCancelled:
		if errorMessage != "" {
			_, err := p.Pool.Exec(ctx, `
				update tasks set 
					status = 'cancelled',
					completed_at = @now,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null,
					last_error = @last_error
				where id = @id and locked_by = @runner_id`,
				pgx.NamedArgs{
					"id":         id,
					"runner_id":  runnerID,
					"now":        now,
					"last_error": errorMessage,
				})
			if err != nil {
				return fmt.Errorf("task release (cancelled): %w", err)
			}
		} else {
			_, err := p.Pool.Exec(ctx, `
				update tasks set 
					status = 'cancelled',
					completed_at = @now,
					locked_by = '',
					locked_at = null,
					lock_expires_at = null
				where id = @id and locked_by = @runner_id`,
				pgx.NamedArgs{
					"id":        id,
					"runner_id": runnerID,
					"now":       now,
				})
			if err != nil {
				return fmt.Errorf("task release (cancelled): %w", err)
			}
		}

	default:
		return fmt.Errorf("task release: unsupported status: %s", status)
	}

	return nil
}
