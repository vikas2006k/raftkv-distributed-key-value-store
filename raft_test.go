package main

import (
	"net"
	"net/rpc"
	"testing"
)

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

func TestRequestVoteRejectOlderTerm(t *testing.T) {
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

func TestProposeCommandAppendsToLeaderLog(t *testing.T) {
	node := newRaft(1)
	node.State = Leader
	node.CurrentTerm = 2

	err := node.ProposeCommand("SET x=10")
	if err != nil {
		t.Fatalf("expected command to be accepted, got error: %v", err)
	}

	if len(node.log) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(node.log))
	}

	entry := node.log[0]

	if entry.Command != "SET x=10" {
		t.Fatalf("expected command %q, got %q", "SET x=10", entry.Command)
	}

	if entry.Term != 2 {
		t.Fatalf("expected entry term 2, got %d", entry.Term)
	}
}

func TestProposeCommandRejectsFollower(t *testing.T) {
	node := newRaft(2)
	node.State = Follower

	err := node.ProposeCommand("SET x=10")
	if err == nil {
		t.Fatal("expected follower to reject the command")
	}

	if len(node.log) != 0 {
		t.Fatalf("expected follower log to remain empty, got %d entries", len(node.log))
	}
}

func TestAppendEntriesReplicatesLogEntry(t *testing.T) {
	follower := newRaft(2)

	// Start an RPC server on an available local port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Close()

	server := rpc.NewServer()
	err = server.RegisterName("Raft", &RaftRPC{Node: follower})
	if err != nil {
		t.Fatalf("failed to register Raft RPC: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	address := listener.Addr().String()

	// Send one entry from a simulated leader.
	request := AppendEntriesRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Command: "SET x=10"},
		},
		LeaderCommit: 0,
	}

	response, err := SendHeartbeatOverRPC(address, request)
	if err != nil {
		t.Fatalf("AppendEntries RPC failed: %v", err)
	}

	if !response.Success {
		t.Fatal("expected follower to accept the log entry")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if len(follower.log) != 1 {
		t.Fatalf("expected 1 follower log entry, got %d", len(follower.log))
	}

	if follower.log[0].Command != "SET x=10" {
		t.Fatalf("unexpected follower command: %q", follower.log[0].Command)
	}

	if follower.log[0].Term != 1 {
		t.Fatalf("expected term 1, got %d", follower.log[0].Term)
	}
}

func TestReplicateToPeerUpdatesIndexes(t *testing.T) {
	leader := newRaft(1)
	leader.State = Leader
	leader.CurrentTerm = 1
	leader.log = []LogEntry{
		{Term: 1, Command: "SET x=10"},
	}

	leader.nextIndex = map[int]int{2: 1}
	leader.matchIndex = map[int]int{2: 0}

	follower := newRaft(2)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Close()

	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &RaftRPC{Node: follower}); err != nil {
		t.Fatalf("failed to register RPC: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	peer := Peer{
		ID:      2,
		Address: listener.Addr().String(),
	}

	replicateToPeer(leader, peer)

	leader.mu.Lock()
	defer leader.mu.Unlock()

	if leader.matchIndex[2] != 1 {
		t.Fatalf("expected matchIndex 1, got %d", leader.matchIndex[2])
	}

	if leader.nextIndex[2] != 2 {
		t.Fatalf("expected nextIndex 2, got %d", leader.nextIndex[2])
	}
}

func TestAdvanceCommitIndexWithMajority(t *testing.T) {
	leader := newRaft(1)
	leader.State = Leader
	leader.CurrentTerm = 2

	leader.log = []LogEntry{
		{Term: 2, Command: "SET x=10"},
	}

	leader.nextIndex = map[int]int{
		2: 2,
		3: 1,
	}

	leader.matchIndex = map[int]int{
		2: 1,
		3: 0,
	}

	peers := []Peer{
		{ID: 2, Address: "unused"},
		{ID: 3, Address: "unused"},
	}

	advanceCommitIndex(leader, peers)

	leader.mu.Lock()
	defer leader.mu.Unlock()

	if leader.CommitIndex != 1 {
		t.Fatalf(
			"expected CommitIndex 1 with majority replication, got %d",
			leader.CommitIndex,
		)
	}
}

func TestAdvanceCommitIndexWithoutMajority(t *testing.T) {
	leader := newRaft(1)
	leader.State = Leader
	leader.CurrentTerm = 2

	leader.log = []LogEntry{
		{Term: 2, Command: "SET x=10"},
	}

	leader.nextIndex = map[int]int{
		2: 1,
		3: 1,
	}

	leader.matchIndex = map[int]int{
		2: 0,
		3: 0,
	}

	peers := []Peer{
		{ID: 2, Address: "unused"},
		{ID: 3, Address: "unused"},
	}

	advanceCommitIndex(leader, peers)

	leader.mu.Lock()
	defer leader.mu.Unlock()

	if leader.CommitIndex != 0 {
		t.Fatalf(
			"expected CommitIndex to remain 0 without a majority, got %d",
			leader.CommitIndex,
		)
	}
}
