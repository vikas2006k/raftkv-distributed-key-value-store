package main

import (
	"fmt"
	"os"
)

func main() {
	// RPC client mode
	if len(os.Args) > 1 && os.Args[1] == "rpc-client" {
		request := VoteRequest{
			Term:         1,
			CandidateID:  1,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}

		response, err := RequestVoteOverRPC("localhost:9001", request)
		if err != nil {
			fmt.Println("RPC client error:", err)
			os.Exit(1)
		}

		fmt.Println("RPC request sent successfully!")
		fmt.Printf("Response term: %d\n", response.Term)
		fmt.Printf("Vote granted: %t\n", response.VoteGranted)
		return
	}

	// Configurable RPC server mode
	if len(os.Args) > 1 && os.Args[1] == "rpc-server" {
		nodeID := 2
		address := ":9001"

		if len(os.Args) > 2 {
			if _, err := fmt.Sscanf(os.Args[2], "%d", &nodeID); err != nil {
				fmt.Println("Invalid node ID:", err)
				os.Exit(1)
			}
		}

		if len(os.Args) > 3 {
			address = os.Args[3]
		}

		node := newRaft(nodeID)

		if err := StartRPCServer(node, address); err != nil {
			fmt.Println("RPC server error:", err)
			os.Exit(1)
		}
		return
	}

	// RPC-based election mode
	if len(os.Args) > 1 && os.Args[1] == "rpc-election" {
		if len(os.Args) < 3 {
			fmt.Println("Usage: go run . rpc-election address1 address2 ...")
			os.Exit(1)
		}

		candidate := newRaft(1)
		peerAddresses := os.Args[2:]

		RunElectionOverRPC(candidate, peerAddresses)
		return
	}

	// Original in-memory election simulation
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
