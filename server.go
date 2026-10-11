
package main

import "fmt"

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
