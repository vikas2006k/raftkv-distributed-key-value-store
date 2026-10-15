package main

import (
	"fmt"
	"net"
	"net/rpc"
)

// NewCluster creates a group of Raft nodes for our simulation.
func NewCluster(size int) []*Raft {
	nodes := make([]*Raft, size)

	for i := 0; i < size; i++ {
		nodes[i] = newRaft(i + 1)
	}

	return nodes
}

// PrintCluster displays the current state of each node.
func PrintCluster(nodes []*Raft) {
	fmt.Println("\nFinal Node States:")

	for _, node := range nodes {
		fmt.Printf(
			"Node %d | State: %s | Term: %d | VotedFor: %d | LeaderID: %d\n",
			node.ID,
			node.State,
			node.CurrentTerm,
			node.VotedFor,
			node.LeaderID,
		)
	}
}

func StartRPCServer(node *Raft, address string) error {
	server := rpc.NewServer()

	err := server.RegisterName("Raft", &RaftRPC{Node: node})
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Println("Raft RPC server listening on", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go server.ServeConn(conn)
	}
}
