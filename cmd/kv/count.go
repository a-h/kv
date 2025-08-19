package main

type CountCommand struct {
	All    CountAllCommand    `cmd:"all" help:"Count the number of keys."`
	Prefix CountPrefixCommand `cmd:"prefix" help:"Count the number of keys with a given prefix."`
	Range  CountRangeCommand  `cmd:"range" help:"Count the number of keys in a range."`
}
