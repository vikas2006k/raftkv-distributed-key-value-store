package main

import "fmt"

func main() {
	const clusterSize = 5

	nodes := NewCluster(clusterSize)
	candidate := nodes[0]

	votes := RunElection(nodes, candidate)
	majority := clusterSize/2 + 1

	fmt.Printf(
		"Node %d finished election for term %d\n",
		candidate.ID,
		candidate.CurrentTerm,
	)

	if candidate.State == Leader {
		fmt.Printf(
			"Node %d becomes the leader with %d votes\n",
			candidate.ID,
			votes,
		)
	} else {
		fmt.Printf(
			"Election not won: %d votes; %d required\n",
			votes,
			majority,
		)
	}

	PrintCluster(nodes)
}
