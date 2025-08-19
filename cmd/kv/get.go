package main

type GetCommand struct {
	Key    GetKeyCommand    `cmd:"key" help:"Get a key."`
	Prefix GetPrefixCommand `cmd:"prefix" help:"Get all keys with a given prefix."`
	Range  GetRangeCommand  `cmd:"range" help:"Get a range of keys."`
}
