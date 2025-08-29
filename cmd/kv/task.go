package main

type TaskCommand struct {
	New    TaskNewCommand    `cmd:"new" help:"Create a new task."`
	List   TaskListCommand   `cmd:"list" help:"List tasks."`
	Get    TaskGetCommand    `cmd:"get" help:"Get a task by ID."`
	Cancel TaskCancelCommand `cmd:"cancel" help:"Cancel a task by ID."`
	Run    TaskRunCommand    `cmd:"run" help:"Run task runner."`
}
