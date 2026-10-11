package main

import "fmt"

func main() {
	const clustersize = 5

	nodes := make([]*Raft, clustersize)

	for i := 0; i < clustersize; i++ {
		nodes[i] = newRaft(i + 1)
	}

	candidate := nodes[0]
	votes := RunElection(nodes, candidate)
	majority := (clustersize / 2) + 1
	fmt.Printf("Node %d finished election for term %d\n", candidate.ID, candidate.CurrentTerm)

	if candidate.State == Leader {
		fmt.Printf("Node %d becomes the leader with %d votes\n", candidate.ID, votes)
	} else {
		fmt.Printf("Election not won: %d votes;%d required\n", votes, majority)
	}

	fmt.Println("\nFinal Node States")

	for _, node := range nodes {
		fmt.Printf("Node %d | State: %s | Term: %d | VotedFor: %d\n", node.ID, node.State, node.CurrentTerm, node.VotedFor)
	}

}
