package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

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

	electionResetCh chan struct{}

	nextIndex  map[int]int
	matchIndex map[int]int
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

		electionResetCh: make(chan struct{}, 1),
	}
}

// resetElectionTimerLocked signals that the election timer
// should restart. The caller must hold r.mu.
func (r *Raft) resetElectionTimerLocked() {
	select {
	case r.electionResetCh <- struct{}{}:
	default:
		// A reset signal is already pending.
	}
}

// RunElectionTimer waits for a randomized timeout.
// onTimeout is called when no reset signal arrives in time.
func (r *Raft) RunElectionTimer(onTimeout func()) {
	for {
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		timer := time.NewTimer(timeout)

		select {
		case <-r.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

		case <-timer.C:
			onTimeout()
		}
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

func (r *Raft) becomeLeaderIfCurrentTerm(term int, peerIDs []int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != Candidate || r.CurrentTerm != term {
		return false
	}

	r.State = Leader
	r.LeaderID = r.ID

	r.nextIndex = make(map[int]int)
	r.matchIndex = make(map[int]int)

	next := len(r.log) + 1

	for _, id := range peerIDs {
		if id == r.ID {
			continue
		}

		r.nextIndex[id] = next
		r.matchIndex[id] = 0
	}

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
		r.resetElectionTimerLocked()

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

func (r *Raft) AppendEntries(args AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	response := AppendEntriesResponse{
		Term:    r.CurrentTerm,
		Success: false,
	}

	// Reject requests from an outdated leader.
	if args.Term < r.CurrentTerm {
		return response
	}

	// A newer term means we must update our term and step down.
	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.VotedFor = -1
	}

	// A valid leader's request makes us a follower.
	r.State = Follower
	r.LeaderID = args.LeaderID
	r.resetElectionTimerLocked()

	response.Term = r.CurrentTerm

	// Check whether the previous log entry matches.
	if args.PrevLogIndex > len(r.log) {
		return response
	}

	if args.PrevLogIndex > 0 {
		previousEntry := r.log[args.PrevLogIndex-1]

		if previousEntry.Term != args.PrevLogTerm {
			return response
		}
	}

	// Append incoming entries, removing conflicting entries if needed.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i

		if index < len(r.log) {
			if r.log[index].Term != entry.Term {
				r.log = r.log[:index]
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
		} else {
			r.log = append(r.log, args.Entries[i:]...)
			break
		}
	}

	// Advance the follower's commit index without exceeding its log.
	if args.LeaderCommit > r.CommitIndex {
		r.CommitIndex = args.LeaderCommit
		if r.CommitIndex > len(r.log) {
			r.CommitIndex = len(r.log)
		}
	}

	response.Success = true
	return response
}

func (r *Raft) ProposeCommand(command string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != Leader {
		return fmt.Errorf("node %d is not the leader", r.ID)
	}

	entry := LogEntry{
		Term:    r.CurrentTerm,
		Command: command,
	}

	r.log = append(r.log, entry)

	fmt.Printf(
		"Leader %d appended command %q at log index %d\n",
		r.ID,
		command,
		len(r.log),
	)

	return nil
}
