package raft

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
			resetTimeout, _ := r.handleAppendEntries(appendEntriesMsg)
			if resetTimeout {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case requestVoteMsg := <-r.requestVote:
			voteCasted := r.handleRequestVote(&requestVoteMsg)
			if voteCasted {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case registerClientMsg := <-r.registerClient:
			registerClientMsg.reply <- RegisterClientReply{
				Status:               ClientStatus_NOT_LEADER,
				ClientId:             0,
				LeaderHint:           r.Leader,
			}
		case clientRequestMsg := <-r.clientRequest:
			clientRequestMsg.reply <- ClientReply{
				Status:               ClientStatus_NOT_LEADER,
				Response:             nil,
				LeaderHint:           r.Leader,
			}
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

// updateTerm updates term of node when appropriate
func (r *Node) updateTermFollower(request *RequestVoteRequest) {
	if request.Term > r.GetCurrentTerm() {
		r.setCurrentTerm(request.Term)
		r.setVotedFor("")
	}
}

// handleRequestVote handles an incoming RequestVoteMsg.
func (r *Node) handleRequestVote(requestVoteMsg *RequestVoteMsg) (voteCasted bool) {
	//handle vote request from other node
	request := requestVoteMsg.request
	//check for term and commitment updates
	r.updateTermFollower(request)
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
	return
}
