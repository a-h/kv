package sqlitekv

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/a-h/kv"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func NewScheduler(pool *sqlitex.Pool) *Scheduler {
	return &Scheduler{
		Sqlite: newPool(pool),
	}
}

type Scheduler struct {
	*Sqlite
}

func (s *Scheduler) New(ctx context.Context, task kv.Task) error {
	stmt := SQLStatement{
		SQL: `insert into tasks (id, name, payload, status, created, scheduled_for, retry_count, max_retries, timeout_seconds)
values (:id, :name, :payload, :status, :created, :scheduled_for, :retry_count, :max_retries, :timeout_seconds);`,
		NamedParams: map[string]any{
			":id":              task.ID,
			":name":            task.Name,
			":payload":         task.Payload,
			":status":          string(task.Status),
			":created":         task.Created.Format(time.RFC3339Nano),
			":scheduled_for":   task.ScheduledFor.Format(time.RFC3339Nano),
			":retry_count":     task.RetryCount,
			":max_retries":     task.MaxRetries,
			":timeout_seconds": task.TimeoutSeconds,
		},
	}
	_, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("task create: %w", err)
	}
	return nil
}

func (s *Scheduler) Get(ctx context.Context, id string) (task kv.Task, ok bool, err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return task, false, err
	}
	defer s.Pool.Put(conn)

	var found bool
	opts := &sqlitex.ExecOptions{
		Named: map[string]any{":id": id},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			found = true
			task, err = parseTaskFromStmt(stmt)
			return err
		},
	}

	err = sqlitex.Execute(conn, `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where id = :id limit 1;`, opts)
	if err != nil {
		return task, false, err
	}

	return task, found, nil
}

// parseTaskFromStmt parses a query result statement into a Task struct.
// Expected columns: id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at
func parseTaskFromStmt(stmt *sqlite.Stmt) (task kv.Task, err error) {
	task.ID = stmt.GetText("id")
	task.Name = stmt.GetText("name")
	task.Payload = make([]byte, stmt.GetLen("payload"))
	stmt.GetBytes("payload", task.Payload)
	task.Status = kv.TaskStatus(stmt.GetText("status"))
	task.RetryCount = int(stmt.GetInt64("retry_count"))
	task.MaxRetries = int(stmt.GetInt64("max_retries"))
	task.TimeoutSeconds = int(stmt.GetInt64("timeout_seconds"))
	task.LastError = stmt.GetText("last_error")
	task.LockedBy = stmt.GetText("locked_by")

	task.Created, err = time.Parse(time.RFC3339Nano, stmt.GetText("created"))
	if err != nil {
		return task, fmt.Errorf("error parsing created time: %w", err)
	}

	task.ScheduledFor, err = time.Parse(time.RFC3339Nano, stmt.GetText("scheduled_for"))
	if err != nil {
		return task, fmt.Errorf("error parsing scheduled_for time: %w", err)
	}

	if startedAtStr := stmt.GetText("started_at"); startedAtStr != "" {
		startedAt, err := time.Parse(time.RFC3339Nano, startedAtStr)
		if err != nil {
			return task, fmt.Errorf("error parsing started_at time: %w", err)
		}
		task.StartedAt = &startedAt
	}

	if completedAtStr := stmt.GetText("completed_at"); completedAtStr != "" {
		completedAt, err := time.Parse(time.RFC3339Nano, completedAtStr)
		if err != nil {
			return task, fmt.Errorf("error parsing completed_at time: %w", err)
		}
		task.CompletedAt = &completedAt
	}

	if lockedAtStr := stmt.GetText("locked_at"); lockedAtStr != "" {
		lockedAt, err := time.Parse(time.RFC3339Nano, lockedAtStr)
		if err != nil {
			return task, fmt.Errorf("error parsing locked_at time: %w", err)
		}
		task.LockedAt = &lockedAt
	}

	if lockExpiresAtStr := stmt.GetText("lock_expires_at"); lockExpiresAtStr != "" {
		lockExpiresAt, err := time.Parse(time.RFC3339Nano, lockExpiresAtStr)
		if err != nil {
			return task, fmt.Errorf("error parsing lock_expires_at time: %w", err)
		}
		task.LockExpiresAt = &lockExpiresAt
	}

	return task, nil
}

func (s *Scheduler) List(ctx context.Context, status kv.TaskStatus, name string, offset, limit int) ([]kv.Task, error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Pool.Put(conn)

	var tasks []kv.Task
	sql, namedParams := getTaskListQuery(status, name, offset, limit)

	opts := &sqlitex.ExecOptions{
		Named: namedParams,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			task, err := parseTaskFromStmt(stmt)
			if err != nil {
				return err
			}
			tasks = append(tasks, task)
			return nil
		},
	}

	err = sqlitex.Execute(conn, sql, opts)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// getTaskListQuery builds the SQL query and parameters for TaskList based on the given filters.
func getTaskListQuery(status kv.TaskStatus, name string, offset, limit int) (string, map[string]any) {
	const (
		queryAll             = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks order by created desc limit :limit offset :offset;`
		queryByStatus        = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where status = :status order by created desc limit :limit offset :offset;`
		queryByName          = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where name = :name order by created desc limit :limit offset :offset;`
		queryByStatusAndName = `select id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at from tasks where status = :status and name = :name order by created desc limit :limit offset :offset;`
	)

	namedParams := map[string]any{
		":offset": offset,
		":limit":  limit,
	}

	if status != "" && name != "" {
		namedParams[":status"] = string(status)
		namedParams[":name"] = name
		return queryByStatusAndName, namedParams
	}

	if status != "" {
		namedParams[":status"] = string(status)
		return queryByStatus, namedParams
	}

	if name != "" {
		namedParams[":name"] = name
		return queryByName, namedParams
	}

	return queryAll, namedParams
}

// getTaskGetNextPendingStatement builds the SQL statement for TaskGetNextPending based on task types.
func getTaskGetNextPendingStatement(runnerID string, now, lockExpiresAt time.Time, taskTypes []string) SQLStatement {
	const (
		sqlAllTypes = `update tasks set 
    status = 'running',
    locked_by = :runner_id,
    locked_at = :now,
    lock_expires_at = :lock_expires_at
where id = (
  select id from tasks 
  where status = 'pending' 
    and scheduled_for <= :now
    and (locked_by = '' or locked_by is null or lock_expires_at <= :now)
  order by scheduled_for asc
  limit 1
)
returning id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at;`
	)

	namedParams := map[string]any{
		":runner_id":       runnerID,
		":now":             now.Format(time.RFC3339Nano),
		":lock_expires_at": lockExpiresAt.Format(time.RFC3339Nano),
	}

	if len(taskTypes) == 0 {
		return SQLStatement{
			SQL:         sqlAllTypes,
			NamedParams: namedParams,
		}
	}

	// Handle specific task types - build the IN clause.
	placeholders := make([]string, len(taskTypes))
	for i, taskType := range taskTypes {
		placeholder := fmt.Sprintf(":task_type_%d", i)
		placeholders[i] = placeholder
		namedParams[placeholder] = taskType
	}

	sqlSpecificTypes := fmt.Sprintf(`update tasks set 
    status = 'running',
    locked_by = :runner_id,
    locked_at = :now,
    lock_expires_at = :lock_expires_at
where id = (
  select id from tasks 
  where status = 'pending' 
    and scheduled_for <= :now
    and name in (%s)
    and (locked_by = '' or locked_by is null or lock_expires_at <= :now)
  order by scheduled_for asc
  limit 1
)
returning id, name, payload, status, created, scheduled_for, started_at, completed_at, last_error, retry_count, max_retries, timeout_seconds, locked_by, locked_at, lock_expires_at;`, strings.Join(placeholders, ","))

	return SQLStatement{
		SQL:         sqlSpecificTypes,
		NamedParams: namedParams,
	}
}

func (s *Scheduler) Lock(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (task kv.Task, locked bool, err error) {
	now := s.Now()
	lockExpiresAt := now.Add(lockDuration)

	stmt := getTaskGetNextPendingStatement(runnerID, now, lockExpiresAt, taskTypes)

	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return task, false, err
	}
	defer s.Pool.Put(conn)

	var found bool
	opts := &sqlitex.ExecOptions{
		Named: stmt.NamedParams,
		ResultFunc: func(sqlStmt *sqlite.Stmt) error {
			found = true
			task, err = parseTaskFromStmt(sqlStmt)
			return err
		},
	}

	err = sqlitex.Execute(conn, stmt.SQL, opts)
	if err != nil {
		return task, false, fmt.Errorf("task get next pending: %w", err)
	}

	return task, found, nil
}

func (s *Scheduler) Cancel(ctx context.Context, id string) error {
	stmt := SQLStatement{
		SQL: `update tasks set status = 'cancelled' where id = :id and status in ('pending', 'running');`,
		NamedParams: map[string]any{
			":id": id,
		},
	}

	_, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("task cancel: %w", err)
	}
	return nil
}

func (s *Scheduler) Release(ctx context.Context, id string, runnerID string, status kv.TaskStatus, errorMessage string) error {
	var stmt SQLStatement
	var err error

	switch status {
	case kv.TaskStatusCompleted:
		stmt = s.getReleaseCompletedStatement(id, runnerID)

	case kv.TaskStatusFailed:
		stmt, err = s.getReleaseFailedStatement(ctx, id, runnerID, errorMessage)
		if err != nil {
			return fmt.Errorf("task release: %w", err)
		}

	case kv.TaskStatusCancelled:
		stmt = s.getReleaseCancelledStatement(id, runnerID, errorMessage)

	default:
		return fmt.Errorf("task release: unsupported status: %s", status)
	}

	_, err = s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("task release: %w", err)
	}
	return nil
}

// getReleaseCompletedStatement returns the SQL statement for completing a task.
func (s *Scheduler) getReleaseCompletedStatement(id, runnerID string) SQLStatement {
	return SQLStatement{
		SQL: `update tasks set 
			status = :status, 
			locked_by = '', 
			locked_at = null, 
			lock_expires_at = null, 
			completed_at = :now, 
			last_error = '' 
		where id = :id and locked_by = :runner_id;`,
		NamedParams: map[string]any{
			":id":        id,
			":runner_id": runnerID,
			":status":    string(kv.TaskStatusCompleted),
			":now":       s.Now().Format(time.RFC3339Nano),
		},
	}
}

// getReleaseFailedStatement returns the SQL statement for handling a failed task.
func (s *Scheduler) getReleaseFailedStatement(ctx context.Context, id, runnerID, errorMessage string) (SQLStatement, error) {
	// First get the current task to check retry logic.
	task, ok, err := s.Get(ctx, id)
	if err != nil {
		return SQLStatement{}, fmt.Errorf("failed to get task for retry logic: %w", err)
	}
	if !ok {
		return SQLStatement{}, fmt.Errorf("task not found: %s", id)
	}

	task.RetryCount++

	if task.RetryCount >= task.MaxRetries {
		// Max retries exceeded, mark as permanently failed.
		return SQLStatement{
			SQL: `update tasks set 
				status = :status, 
				locked_by = '', 
				locked_at = null, 
				lock_expires_at = null, 
				completed_at = :now, 
				last_error = :last_error, 
				retry_count = :retry_count 
			where id = :id and locked_by = :runner_id;`,
			NamedParams: map[string]any{
				":id":          id,
				":runner_id":   runnerID,
				":status":      string(kv.TaskStatusFailed),
				":now":         s.Now().Format(time.RFC3339Nano),
				":last_error":  errorMessage,
				":retry_count": task.RetryCount,
			},
		}, nil
	}

	// Reset to pending for retry with exponential backoff.
	backoffSeconds := 60 * (1 << task.RetryCount) // 60s, 120s, 240s, etc.
	scheduledFor := s.Now().Add(time.Duration(backoffSeconds) * time.Second)

	return SQLStatement{
		SQL: `update tasks set 
			status = :status, 
			locked_by = '', 
			locked_at = null, 
			lock_expires_at = null, 
			scheduled_for = :scheduled_for, 
			last_error = :last_error, 
			retry_count = :retry_count 
		where id = :id and locked_by = :runner_id;`,
		NamedParams: map[string]any{
			":id":            id,
			":runner_id":     runnerID,
			":status":        string(kv.TaskStatusPending),
			":scheduled_for": scheduledFor.Format(time.RFC3339Nano),
			":last_error":    errorMessage,
			":retry_count":   task.RetryCount,
		},
	}, nil
}

// getReleaseCancelledStatement returns the SQL statement for cancelling a task.
func (s *Scheduler) getReleaseCancelledStatement(id, runnerID, errorMessage string) SQLStatement {
	if errorMessage != "" {
		return SQLStatement{
			SQL: `update tasks set 
				status = :status, 
				locked_by = '', 
				locked_at = null, 
				lock_expires_at = null, 
				completed_at = :now, 
				last_error = :last_error 
			where id = :id and locked_by = :runner_id;`,
			NamedParams: map[string]any{
				":id":         id,
				":runner_id":  runnerID,
				":status":     string(kv.TaskStatusCancelled),
				":now":        s.Now().Format(time.RFC3339Nano),
				":last_error": errorMessage,
			},
		}
	}

	return SQLStatement{
		SQL: `update tasks set 
			status = :status, 
			locked_by = '', 
			locked_at = null, 
			lock_expires_at = null, 
			completed_at = :now 
		where id = :id and locked_by = :runner_id;`,
		NamedParams: map[string]any{
			":id":        id,
			":runner_id": runnerID,
			":status":    string(kv.TaskStatusCancelled),
			":now":       s.Now().Format(time.RFC3339Nano),
		},
	}
}
