package main

type BenchmarkCommand struct {
	Get   BenchmarkGetCommand   `cmd:"get" help:"Benchmark getting records."`
	Put   BenchmarkPutCommand   `cmd:"put" help:"Benchmark putting records."`
	Patch BenchmarkPatchCommand `cmd:"patch" help:"Benchmark patching records."`
	Mixed BenchmarkMixedCommand `cmd:"mixed" help:"Benchmark mixed operations."`
}
