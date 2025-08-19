package main

type ConsumerCommand struct {
	Get    ConsumerGetCommand    `cmd:"get" help:"Get a batch of records for a consumer."`
	Status ConsumerStatusCommand `cmd:"status" help:"Show the status of a consumer."`
	Commit ConsumerCommitCommand `cmd:"commit" help:"Commit the consumer position to a sequence number."`
}
