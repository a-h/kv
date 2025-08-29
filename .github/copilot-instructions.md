## Instructions

Comments MUST be complete sentences with proper punctuation.

### Control Flow

- **Use early returns over if/else**: Prefer early returns instead of nested if/else structures to maintain "line of sight" where the happy path is not indented.

### Comments

- Do not write comments, except for public functions, methods, and types.
- If you are writing comments, use proper punctuation - complete sentences, terminating in a full stop.
- Never write trailing comments after a statement.

### Variable Declaration
- **Use idiomatic zero value initialization**: Use `var count int` instead of `count := 0`, and `var found bool` instead of `found := false` when initializing to zero values.

### Modern Go Features
- **Use modern Go types**: Use `any` instead of `interface{}` in modern Go (Go 1.18+).
- **Use Go 1.22+ iterators**: Prefer `iter.Seq` and `iter.Seq2` for streaming APIs and range-over functions instead of channels or callback patterns. Remember to check `ctx.Err()` in iterator loops for proper context cancellation.

### Context Handling
- **Proper context cancellation**: Check `ctx.Err()` for context cancellation instead of using select statements when appropriate.

### API Design
- **Remove unsafe APIs**: Eliminate APIs that require external locking or are unsafe to use concurrently rather than working around them.
- **Consolidate functionality**: Prefer simpler, consolidated APIs over complex multi-type architectures when the simpler approach provides the same functionality safely.

These guidelines focus on writing clean, readable, and maintainable Go code that follows community conventions.

### SQL
- Use use lowercase for SQL queries and SQL statements.
- Always use constant strings for SQL, never concatenate strings to build SQL queries.