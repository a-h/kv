package main

type GraphCommand struct {
	AddEdge     GraphAddEdgeCommand     `cmd:"add-edge" help:"Add an edge between two entities"`
	GetEdge     GraphGetEdgeCommand     `cmd:"get-edge" help:"Get a specific edge"`
	GetOutgoing GraphGetOutgoingCommand `cmd:"get-outgoing" help:"Get outgoing edges from an entity"`
	GetIncoming GraphGetIncomingCommand `cmd:"get-incoming" help:"Get incoming edges to an entity"`
	RemoveEdge  GraphRemoveEdgeCommand  `cmd:"remove-edge" help:"Remove an edge between two entities"`
	FindPath    GraphFindPathCommand    `cmd:"find-path" help:"Find shortest path between two entities"`
	View        GraphViewCommand        `cmd:"view" help:"Generate graph visualization output"`
}
