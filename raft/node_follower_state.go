package raft

import "math"

// doFollower implements the logic for a Raft node in the follower state.
func (r *Node) doFollower() stateFunction {
	r.Out("Transitioning to FollowerState")
	r.State = FollowerState

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.

	//INITIAL WORK
	timeout := randomTimeout(r.config.ElectionTimeout)

	//EVALUATE CHANNELS
	for {
		//random timeout representing timeout to switch to candidate state
		select {
		case _ = <-timeout:
			return r.doCandidate
		case appendEntriesMsg := <-r.appendEntries:
			//update term
			_ = r.updateTerm(appendEntriesMsg.request.Term)
			//handle appendEntriesMsg
			resetTimeout, _ := r.handleAppendEntries(appendEntriesMsg)
			if resetTimeout {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case requestVoteMsg := <-r.requestVote:
			//update term
			_ = r.updateTerm(requestVoteMsg.request.Term)
			//handle appendEntriesMsg
			voteCasted := r.handleRequestVote(&requestVoteMsg)
			if voteCasted {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case registerClientMsg := <-r.registerClient:
			registerClientMsg.reply <- RegisterClientReply{
				Status:     ClientStatus_NOT_LEADER,
				ClientId:   0,
				LeaderHint: r.Leader,
			}
		case clientRequestMsg := <-r.clientRequest:
			clientRequestMsg.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   nil,
				LeaderHint: r.Leader,
			}
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

// handleRequestVote handles an incoming RequestVoteMsg.
func (r *Node) handleRequestVote(requestVoteMsg *RequestVoteMsg) (voteCasted bool) {
	//handle vote request from other node
	request := requestVoteMsg.request
	//get currentTerm and votedFor of follower from stable storage
	currentTerm := r.GetCurrentTerm()
	votedFor := r.GetVotedFor()
	//validCandidate undergoes several checks to determine vote value
	var validCandidate bool = true
	//check if term is valid
	if request.Term < currentTerm {
		validCandidate = false
	}
	//check if vote has already been casted
	if votedFor != "" && votedFor != request.Candidate.Id {
		validCandidate = false
	}
	//check if candidate is at least as up to date with voter
	if !isUpToDate(request.LastLogIndex, request.LastLogTerm, r.GetLog(r.LastLogIndex())) {
		validCandidate = false
	}
	//issue requestVoteMsg accordingly and update currentTerm
	if validCandidate {
		//votedFor update
		r.setVotedFor(request.Candidate.Id)
		//requestVote reply
		requestVoteMsg.reply <- RequestVoteReply{
			Term:                 currentTerm,
			VoteGranted:          true,
		}
		return true
	} else {
		requestVoteMsg.reply <- RequestVoteReply{
			Term:                 currentTerm,
			VoteGranted:          false,
		}
		return false
	}
}

func isUpToDate(candidateIndex uint64, candidateTerm uint64, log *LogEntry) (upToDate bool) {
	if log == nil {
		return true
	}
	if candidateTerm > log.TermId {
		return true
	}
	if candidateTerm == log.TermId && candidateIndex >= log.Index {
		return true
	}
	return false
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (r *Node) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	//get currentTerm from local node
	currentTerm := r.GetCurrentTerm()
	//check if bad request
	if request.Term < currentTerm || r.GetLog(request.PrevLogIndex).TermId != request.PrevLogTerm {
		msg.reply <- AppendEntriesReply{
			Term:                 r.GetCurrentTerm(),
			Success:              false,
		}
		//TODO: I am unsure about these return values
		return false, false
	}
	//update log entries in two steps
	for _, entry := range request.Entries {
		ind := entry.Index
		term := entry.TermId
		if entry := r.GetLog(ind); entry != nil && entry.TermId != term {
			r.TruncateLog(ind)
		}
	}
	//TODO: Idempotent?
	for _, entry := range request.Entries {
		r.StoreLog(entry)
	}
	//update commitIndex
	//TODO: Not sure if I am accessing correct entry here
	if request.LeaderCommit > r.commitIndex {
		r.commitIndex = uint64(math.Min(float64(request.LeaderCommit),
			float64(request.Entries[len(request.Entries) - 1].Index)))
	}
	r.checkCommitment()

	msg.reply <- AppendEntriesReply{
		Term:                 r.GetCurrentTerm(),
		Success:              true,
	}
	//TODO: I am unsure about these return values
	return true, true

}
