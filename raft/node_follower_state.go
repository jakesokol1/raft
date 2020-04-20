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
		case <-timeout:
			return r.doCandidate
		case appendEntriesMsg := <-r.appendEntries:
			//update term
			//r.Out("Receiving Message")
			_ = r.updateTerm(appendEntriesMsg.request.Term)
			//handle appendEntriesMsg
			resetTimeout, _ := r.handleAppendEntries(appendEntriesMsg)
			//r.Out("Reset timeout: %v", resetTimeout)
			if resetTimeout {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case requestVoteMsg := <-r.requestVote:
			//update term
			_ = r.updateTerm(requestVoteMsg.request.Term)
			//handle appendEntriesMsg
			voteCasted := r.handleRequestVote(&requestVoteMsg)
			r.Out("Received vote request from %v, granted: %v", requestVoteMsg.request.Candidate.Id, voteCasted)
			if voteCasted {
				//TODO: Amy says no, I say why not
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case registerClientMsg := <-r.registerClient:
			registerClientMsg.reply <- RegisterClientReply{
				Status:     ClientStatus_NOT_LEADER,
				ClientId:   0,
				LeaderHint: r.getLeader(),
			}
		case clientRequestMsg := <-r.clientRequest:
			clientRequestMsg.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   nil,
				LeaderHint: r.getLeader(),
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
	//handle request from remote node
	request := msg.request
	//get currentTerm from local node
	currentTerm := r.GetCurrentTerm()
	//check if bad request
	if request.Term < currentTerm {
		msg.reply <- AppendEntriesReply{
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		return false, false
	}
	//request must be from a valid leader, set leader if it is new
	r.setLeader(request.Leader)
	//check if heartbeat message
	if request.Entries == nil {
		//TODO: ask michael if should return success as true or false
		msg.reply <- AppendEntriesReply{
			Term:                 r.GetCurrentTerm(),
			Success:              false,
		}
		return true, true
	}
	//check if indexing is proper for log update
	if entry := r.GetLog(request.PrevLogIndex); entry == nil || entry.TermId != request.PrevLogTerm {
		msg.reply <- AppendEntriesReply {
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		return true, true
	}
	//truncate inconsistencies
	for _, newEntry := range request.Entries {
		ind := newEntry.Index
		term := newEntry.TermId
		if entry := r.GetLog(ind); entry != nil && entry.TermId != term {
			r.TruncateLog(ind)
		}
	}
	//store all logs in entries
	for _, entry := range request.Entries {
		r.StoreLog(entry)
	}
	//handle new commits and state machine work
	go r.updateCommitment(request.LeaderCommit)
	//successful update, respond to leader
	msg.reply <- AppendEntriesReply{
		Term:    r.GetCurrentTerm(),
		Success: true,
	}
	return true, true
}
