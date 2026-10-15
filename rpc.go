package main

import (
	"net/rpc"
)

type RaftRPC struct {
	Node *Raft
}

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

func (s *RaftRPC) RequestVote(
	args *VoteRequest,
	reply *VoteResponse,
) error {
	*reply = s.Node.RequestVote(*args)
	return nil
}

// RequestVoteOverRPC calls RequestVote on a remote node.
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
