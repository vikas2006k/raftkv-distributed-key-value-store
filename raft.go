package main

import "sync"

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command string
}

type Raft struct {
	mu sync.Mutex

	ID          int
	CurrentTerm int
	State       State
	VotedFor    int

	log         []LogEntry
	CommitIndex int
	LastApplied int
	LeaderID    int
}

func newRaft(id int) *Raft {
	return &Raft{
		ID:          id,
		CurrentTerm: 0,
		State:       Follower,
		VotedFor:    -1,
		log:         make([]LogEntry, 0),
		CommitIndex: 0,
		LastApplied: 0,
		LeaderID:    -1,
	}
}

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"

	case Leader:
		return "Leader"

	default:
		return "unknown"
	}
}

func (r *Raft) StartElection() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.CurrentTerm++
	r.State = Candidate
	r.VotedFor = r.ID
	r.LeaderID = -1
}

func (r *Raft) stepDownIfHigherTerm(term int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term <= r.CurrentTerm {
		return false
	}

	r.CurrentTerm = term
	r.State = Follower
	r.VotedFor = -1
	r.LeaderID = -1

	return true
}

func (r *Raft) LastLogIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.log)
}

func (r *Raft) LastLogTerm() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lastLogTermLocked()
}

// Caller must already hold r.mu.
func (r *Raft) lastLogTermLocked() int {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func (r *Raft) RequestVote(args VoteRequest) VoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	response := VoteResponse{
		Term:        r.CurrentTerm,
		VoteGranted: false,
	}

	if args.Term < r.CurrentTerm {
		return response
	}

	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.State = Follower
		r.VotedFor = -1
		r.LeaderID = -1
	}

	response.Term = r.CurrentTerm

	lastLogIndex := len(r.log)
	lastLogTerm := r.lastLogTermLocked()

	candidateLogIsUpToDate :=
		args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm &&
				args.LastLogIndex >= lastLogIndex)

	if candidateLogIsUpToDate &&
		(r.VotedFor == -1 || r.VotedFor == args.CandidateID) {
		r.VotedFor = args.CandidateID
		response.VoteGranted = true
	}

	return response
}

func RunElection(nodes []*Raft, candidate *Raft) int {
	candidate.StartElection()

	votes := 1 // Candidate votes for itself.

	for _, peer := range nodes {
		if peer == nil || peer.ID == candidate.ID {
			continue
		}

		request := VoteRequest{
			Term:         candidate.CurrentTerm,
			CandidateID:  candidate.ID,
			LastLogIndex: candidate.LastLogIndex(),
			LastLogTerm:  candidate.LastLogTerm(),
		}

		response := peer.RequestVote(request)

		// A newer term means this candidate must step down.
		if response.Term > candidate.CurrentTerm {
			candidate.CurrentTerm = response.Term
			candidate.State = Follower
			candidate.VotedFor = -1
			return votes
		}

		if response.VoteGranted {
			votes++
		}
	}

	majority := len(nodes)/2 + 1

	// Check the result only after collecting the votes.
	if candidate.State == Candidate && votes >= majority {
		candidate.State = Leader
		candidate.LeaderID = candidate.ID
	}

	return votes
}
