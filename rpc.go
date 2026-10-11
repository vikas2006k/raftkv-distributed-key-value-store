package main

// VoteRequest contains the information a candidate
// sends to another node when requesting a vote.
type VoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// VoteResponse contains the result of a vote request.
type VoteResponse struct {
	Term        int
	VoteGranted bool
}
