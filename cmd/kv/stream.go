package main

// StreamCommand groups stream subcommands: get, seq, trim.
type StreamCommand struct {
	Get  StreamGetCommand  `cmd:"get" help:"Get a batch of records from the stream."`
	Seq  StreamSeqCommand  `cmd:"seq" help:"Show the current stream sequence number."`
	Trim StreamTrimCommand `cmd:"trim" help:"Trim the stream to a given sequence number."`
}
