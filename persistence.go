package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type PersistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    int        `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

func (r *Raft) savePersistentState() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.savePersistentStateLocked()
}

// savePersistentStateLocked must only be called while r.mu is locked.
func (r *Raft) savePersistentStateLocked() error {
	state := PersistentState{
		CurrentTerm: r.CurrentTerm,
		VotedFor:    r.VotedFor,
		Log:         append([]LogEntry(nil), r.log...),
	}

	if err := os.MkdirAll(r.stateDir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(r.stateDir, fmt.Sprintf("node-%d.json", r.ID))
	tempPath := path + ".tmp"

	if err := os.WriteFile(tempPath, data, 0600); err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}

func (r *Raft) loadPersistentState() error {
	// Use the same directory as savePersistentStateLocked.
	path := filepath.Join(r.stateDir, fmt.Sprintf("node-%d.json", r.ID))

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // First start: no saved state yet.
		}
		return err
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf(
			"decode persisted state for node %d: %w",
			r.ID,
			err,
		)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.CurrentTerm = state.CurrentTerm
	r.VotedFor = state.VotedFor
	r.log = state.Log

	// Reinitialize runtime state after restart.
	r.State = Follower
	r.LeaderID = -1
	r.CommitIndex = 0
	r.LastApplied = 0
	r.kvStore = make(map[string]string)

	return nil
}
