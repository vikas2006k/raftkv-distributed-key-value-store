package main

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
	ID          int
	CurrentTerm int
	State       State
	VotedFor    int

	log         []LogEntry
	CommitIndex int
	LastApplied int
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
	}
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
	r.CurrentTerm++
	r.State = Candidate
	r.VotedFor = r.ID
}

func (r *Raft) LastLogIndex() int {
	return len(r.log)
}

func (r *Raft) LastLogTerm() int {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func (r *Raft) RequestVote(args VoteRequest) VoteResponse {
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
	}

	response.Term = r.CurrentTerm

	candidateLogIsUpToDate := (args.LastLogTerm > r.LastLogTerm()) ||
		(args.LastLogTerm == r.LastLogTerm() && args.LastLogIndex >= r.LastLogIndex())

	if candidateLogIsUpToDate && (r.VotedFor == -1 || r.VotedFor == args.CandidateID) {
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
	}

	return votes
}
