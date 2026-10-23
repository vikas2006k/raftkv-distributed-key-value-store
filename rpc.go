package main

import (
	"fmt"
	"net/rpc"
	"time"
)

type Peer struct {
	ID      int
	Address string
}

type RaftRPC struct {
	Node *Raft
}

type VoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

func (s *RaftRPC) RequestVote(
	args *VoteRequest,
	reply *VoteResponse,
) error {
	*reply = s.Node.RequestVote(*args)
	return nil
}

func (s *RaftRPC) AppendEntries(
	args *AppendEntriesRequest,
	reply *AppendEntriesResponse,
) error {
	*reply = s.Node.AppendEntries(*args)
	return nil
}

func RequestVoteOverRPC(
	address string,
	request VoteRequest,
) (VoteResponse, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return VoteResponse{}, err
	}
	defer client.Close()

	var response VoteResponse
	err = client.Call("Raft.RequestVote", &request, &response)
	if err != nil {
		return VoteResponse{}, err
	}

	return response, nil
}

func RunElectionOverRPC(
	candidate *Raft,
	peers []Peer,
) int {
	candidate.StartElection()

	candidate.mu.Lock()
	term := candidate.CurrentTerm
	id := candidate.ID
	candidate.mu.Unlock()

	votes := 1
	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1

	fmt.Printf(
		"Node %d starts election for term %d\n",
		id,
		term,
	)

	for _, peer := range peers {
		candidate.mu.Lock()
		stillCandidate := candidate.State == Candidate &&
			candidate.CurrentTerm == term
		candidate.mu.Unlock()

		if !stillCandidate {
			return votes
		}

		request := VoteRequest{
			Term:         term,
			CandidateID:  id,
			LastLogIndex: candidate.LastLogIndex(),
			LastLogTerm:  candidate.LastLogTerm(),
		}

		response, err := RequestVoteOverRPC(peer.Address, request)
		if err != nil {
			fmt.Printf(
				"Could not get vote from %s: %v\n",
				peer.Address,
				err,
			)
			continue
		}

		if candidate.stepDownIfHigherTerm(response.Term) {
			fmt.Printf(
				"Node %d discovered higher term %d and stepped down\n",
				id,
				response.Term,
			)
			return votes
		}

		if response.VoteGranted {
			votes++
			fmt.Printf("Received vote from %s\n", peer.Address)
		}

		if votes >= majority {
			peerIDs := make([]int, 0, len(peers))
			for _, p := range peers {
				peerIDs = append(peerIDs, p.ID)
			}

			if candidate.becomeLeaderIfCurrentTerm(term, peerIDs) {
				fmt.Printf(
					"Node %d becomes leader with %d votes\n",
					id,
					votes,
				)
				go StartHeartbeats(candidate, peers)
			}

			return votes
		}
	}

	fmt.Printf(
		"Election not won: %d votes; %d required\n",
		votes,
		majority,
	)

	return votes
}

func SendHeartbeatOverRPC(
	address string,
	request AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer client.Close()

	var response AppendEntriesResponse
	err = client.Call(
		"Raft.AppendEntries",
		&request,
		&response,
	)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	return response, nil
}

func StartHeartbeats(leader *Raft, peers []Peer) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		leader.mu.Lock()
		isLeader := leader.State == Leader
		leader.mu.Unlock()

		if !isLeader {
			return
		}

		for _, peer := range peers {
			replicateToPeer(leader, peer)
		}

		advanceCommitIndex(leader, peers)
	}
}

func replicateToPeer(leader *Raft, peer Peer) {
	leader.mu.Lock()

	if leader.State != Leader {
		leader.mu.Unlock()
		return
	}

	term := leader.CurrentTerm
	next := leader.nextIndex[peer.ID]

	// Ensure nextIndex is a valid log position.
	if next < 1 {
		next = 1
		leader.nextIndex[peer.ID] = next
	}

	prevLogIndex := next - 1
	prevLogTerm := 0

	if prevLogIndex > 0 {
		prevLogTerm = leader.log[prevLogIndex-1].Term
	}

	// Copy the entries that this follower needs.
	entries := append([]LogEntry(nil), leader.log[next-1:]...)

	request := AppendEntriesRequest{
		Term:         term,
		LeaderID:     leader.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leader.CommitIndex,
	}

	leader.mu.Unlock()

	// Never hold the Raft mutex while making a network call.
	response, err := SendHeartbeatOverRPC(peer.Address, request)
	if err != nil {
		return
	}

	if response.Term > term {
		leader.stepDownIfHigherTerm(response.Term)
		return
	}

	leader.mu.Lock()
	defer leader.mu.Unlock()

	// The leader may have changed while the RPC was running.
	if leader.State != Leader || leader.CurrentTerm != term {
		return
	}

	if response.Success {
		match := prevLogIndex + len(entries)

		if match > leader.matchIndex[peer.ID] {
			leader.matchIndex[peer.ID] = match
		}

		leader.nextIndex[peer.ID] = leader.matchIndex[peer.ID] + 1
	} else {
		// Back up and retry on a later heartbeat.
		if leader.nextIndex[peer.ID] > 1 {
			leader.nextIndex[peer.ID]--
		}
	}
}

func advanceCommitIndex(leader *Raft, peers []Peer) {
	leader.mu.Lock()
	defer leader.mu.Unlock()

	if leader.State != Leader {
		return
	}

	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1

	for index := len(leader.log); index > leader.CommitIndex; index-- {
		// Raft only advances commitment this way for entries
		// from the leader's current term.
		if leader.log[index-1].Term != leader.CurrentTerm {
			continue
		}

		replicated := 1 // The leader has its own entry.

		for _, peer := range peers {
			if leader.matchIndex[peer.ID] >= index {
				replicated++
			}
		}

		if replicated >= majority {
			leader.CommitIndex = index
			break
		}
	}
}
