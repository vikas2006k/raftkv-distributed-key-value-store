package main

import "testing"

func makeTestCluster(size int) []*Raft {
	nodes := make([]*Raft, size)

	for i := 0; i < size; i++ {
		nodes[i] = newRaft(i + 1)
	}
	return nodes
}

func TestRequestVoteGrantsOnlyOnePerTerm(t *testing.T) {
	node := newRaft(2)

	first := node.RequestVote(VoteRequest{
		Term:        1,
		CandidateID: 1,
	})

	if !first.VoteGranted {
		t.Fatal("Expected first candidate to receive a vote")
	}

	second := node.RequestVote(VoteRequest{
		Term:        1,
		CandidateID: 3,
	})

	if second.VoteGranted {
		t.Fatal("Node must not vote for two candidates in one term")
	}
}

func TestRquestVoteRejectOlderTerm(t *testing.T) {
	node := newRaft(2)
	node.CurrentTerm = 2

	response := node.RequestVote(VoteRequest{
		Term:        1,
		CandidateID: 1,
	})

	if response.VoteGranted {
		t.Fatal("expected older term request to be rejected")
	}

	if response.Term != 2 {
		t.Fatalf("expected term 2,got %d", response.Term)
	}
}

func TestElectionWinsWithMajority(t *testing.T) {
	nodes := makeTestCluster(5)

	votes := RunElection(nodes, nodes[0])

	if votes < 3 {
		t.Fatalf("expected atleast 3 votes,got %d", votes)
	}

	if nodes[0].State != Leader {
		t.Fatal("expected candidate to become leader")
	}

}

func TestElectionFailsWithoutMajority(t *testing.T) {
	nodes := makeTestCluster(5)

	// Three peers have already voted for another candidate
	// in term 1, so they cannot vote for Node 1.
	for i := 2; i < 5; i++ {
		nodes[i].CurrentTerm = 1
		nodes[i].VotedFor = 99
	}

	votes := RunElection(nodes, nodes[0])

	if votes >= 3 {
		t.Fatalf("expected fewer than 3 votes, got %d", votes)
	}

	if nodes[0].State != Candidate {
		t.Fatal("candidate must not become leader without a majority")
	}
}

func TestElectionRecordsLeaderID(t *testing.T) {
	nodes := makeTestCluster(5)
	candidate := nodes[0]

	RunElection(nodes, candidate)

	if candidate.State != Leader {
		t.Fatal("expected candidate to become leader")
	}

	if candidate.LeaderID != candidate.ID {
		t.Fatalf(
			"expected LeaderID %d, got %d",
			candidate.ID,
			candidate.LeaderID,
		)
	}
}
