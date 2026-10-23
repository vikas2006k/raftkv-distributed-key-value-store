package main

import (
	"fmt"
	"math/rand"
	"strings"
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

	kvStore  map[string]string
	stateDir string
}

func newRaft(id int) *Raft {
	return &Raft{
		ID:              id,
		CurrentTerm:     0,
		State:           Follower,
		VotedFor:        -1,
		log:             make([]LogEntry, 0),
		CommitIndex:     0,
		LastApplied:     0,
		LeaderID:        -1,
		electionResetCh: make(chan struct{}, 1),
		kvStore:         make(map[string]string),
		stateDir:        "raft-data",
	}
}

// The caller must hold r.mu.
func (r *Raft) resetElectionTimerLocked() {
	select {
	case r.electionResetCh <- struct{}{}:
	default:
	}
}

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

	oldTerm := r.CurrentTerm
	oldState := r.State
	oldVote := r.VotedFor
	oldLeader := r.LeaderID

	r.CurrentTerm++
	r.State = Candidate
	r.VotedFor = r.ID
	r.LeaderID = -1

	if err := r.savePersistentStateLocked(); err != nil {
		r.CurrentTerm = oldTerm
		r.State = oldState
		r.VotedFor = oldVote
		r.LeaderID = oldLeader

		fmt.Printf(
			"Node %d: failed to persist election state: %v\n",
			r.ID,
			err,
		)
		return
	}
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

// The caller must hold r.mu.
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

	// Persist a higher term before processing the vote request.
	if args.Term > r.CurrentTerm {
		oldTerm := r.CurrentTerm
		oldVote := r.VotedFor
		oldState := r.State
		oldLeader := r.LeaderID

		r.CurrentTerm = args.Term
		r.State = Follower
		r.VotedFor = -1
		r.LeaderID = -1

		if err := r.savePersistentStateLocked(); err != nil {
			r.CurrentTerm = oldTerm
			r.VotedFor = oldVote
			r.State = oldState
			r.LeaderID = oldLeader

			response.Term = r.CurrentTerm
			return response
		}
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

		oldVote := r.VotedFor
		r.VotedFor = args.CandidateID

		// Save the vote before granting it.
		if err := r.savePersistentStateLocked(); err != nil {
			r.VotedFor = oldVote
			response.Term = r.CurrentTerm
			return response
		}

		response.VoteGranted = true
		r.resetElectionTimerLocked()
	}

	return response
}

func RunElection(nodes []*Raft, candidate *Raft) int {
	candidate.StartElection()

	candidate.mu.Lock()
	term := candidate.CurrentTerm
	candidateID := candidate.ID
	lastLogIndex := len(candidate.log)
	lastLogTerm := candidate.lastLogTermLocked()
	candidate.mu.Unlock()

	votes := 1

	for _, peer := range nodes {
		if peer == nil || peer.ID == candidateID {
			continue
		}

		request := VoteRequest{
			Term:         term,
			CandidateID:  candidateID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		response := peer.RequestVote(request)

		if response.Term > term {
			candidate.stepDownIfHigherTerm(response.Term)
			return votes
		}

		if response.VoteGranted {
			votes++
		}
	}

	majority := len(nodes)/2 + 1

	candidate.mu.Lock()
	defer candidate.mu.Unlock()

	if candidate.State == Candidate &&
		candidate.CurrentTerm == term &&
		votes >= majority {
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

	if args.Term < r.CurrentTerm {
		return response
	}

	// Save the old persistent state so we can roll back if saving fails.
	oldTerm := r.CurrentTerm
	oldVote := r.VotedFor
	oldLog := append([]LogEntry(nil), r.log...)

	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.VotedFor = -1
	}

	r.State = Follower
	r.LeaderID = args.LeaderID
	r.resetElectionTimerLocked()

	response.Term = r.CurrentTerm

	// Persist a newer term before processing the log.
	if args.Term > oldTerm {
		if err := r.savePersistentStateLocked(); err != nil {
			r.CurrentTerm = oldTerm
			r.VotedFor = oldVote
			response.Term = r.CurrentTerm
			return response
		}
	}

	// Verify the previous log entry.
	if args.PrevLogIndex > len(r.log) {
		return response
	}

	if args.PrevLogIndex > 0 {
		previousEntry := r.log[args.PrevLogIndex-1]

		if previousEntry.Term != args.PrevLogTerm {
			return response
		}
	}

	// Append new entries and remove conflicting suffixes.
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

	// Persist the modified log before acknowledging replication.
	if err := r.savePersistentStateLocked(); err != nil {
		r.CurrentTerm = oldTerm
		r.VotedFor = oldVote
		r.log = oldLog
		response.Term = r.CurrentTerm
		return response
	}

	// Update the follower's commit index.
	if args.LeaderCommit > r.CommitIndex {
		r.CommitIndex = args.LeaderCommit

		if r.CommitIndex > len(r.log) {
			r.CommitIndex = len(r.log)
		}
	}

	// Apply committed entries to the follower's state machine.
	r.applyCommittedEntriesLocked()

	response.Term = r.CurrentTerm
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

	// Remember the current log length so we can roll back on failure.
	oldLogLength := len(r.log)

	r.log = append(r.log, entry)

	// Persist the new entry before accepting the proposal.
	if err := r.savePersistentStateLocked(); err != nil {
		r.log = r.log[:oldLogLength]
		return fmt.Errorf("persist command on node %d: %w", r.ID, err)
	}

	fmt.Printf(
		"Leader %d appended command %q at log index %d\n",
		r.ID,
		command,
		len(r.log),
	)

	return nil
}

// The caller must hold r.mu.
// Apply committed commands in log order.
func (r *Raft) applyCommittedEntriesLocked() {
	if r.kvStore == nil {
		r.kvStore = make(map[string]string)
	}

	for r.LastApplied < r.CommitIndex {
		// Log indexes start at 1; slice indexes start at 0.
		entry := r.log[r.LastApplied]

		command := strings.TrimSpace(entry.Command)
		operation, argument, found := strings.Cut(command, " ")

		if found && operation == "SET" {
			key, value, valid := strings.Cut(
				strings.TrimSpace(argument),
				"=",
			)

			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)

			if valid && key != "" {
				r.kvStore[key] = value
			}
		}

		// Advance after processing this entry.
		r.LastApplied++
	}
}
