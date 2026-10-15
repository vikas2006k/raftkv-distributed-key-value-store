package main

import (
	"fmt"
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


func RunElectionOverRPC(candidate *Raft, peerAddresses []string) int {
    candidate.StartElection()

    votes := 1 // The candidate votes for itself.
    clusterSize := len(peerAddresses) + 1
    majority := clusterSize/2 + 1

    fmt.Printf(
        "Node %d starts election for term %d\n",
        candidate.ID,
        candidate.CurrentTerm,
    )

    for _, address := range peerAddresses {
        request := VoteRequest{
            Term:         candidate.CurrentTerm,
            CandidateID:  candidate.ID,
            LastLogIndex: candidate.LastLogIndex(),
            LastLogTerm:  candidate.LastLogTerm(),
        }

        response, err := RequestVoteOverRPC(address, request)
        if err != nil {
            fmt.Printf("Could not get vote from %s: %v\n", address, err)
            continue
        }

        if response.Term > candidate.CurrentTerm {
            candidate.CurrentTerm = response.Term
            candidate.State = Follower
            candidate.VotedFor = -1
            candidate.LeaderID = -1

            fmt.Println("Discovered a higher term; stepping down.")
            return votes
        }

        if response.VoteGranted {
            votes++
            fmt.Printf("Received vote from %s\n", address)
        }

        if votes >= majority {
            candidate.State = Leader
            candidate.LeaderID = candidate.ID
            fmt.Printf(
                "Node %d becomes leader with %d votes\n",
                candidate.ID,
                votes,
            )
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

