package raft

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *Node) doCandidate() stateFunction {
	r.Out("Transitioning to CandidateState")
	r.State = CandidateState
	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	//INITIAL WORK
	//increment current term
	r.updateTerm(r.GetCurrentTerm() + 1)
	//vote for self
	r.setVotedFor(r.Self.Id)
	//reset timeout
	timeout := randomTimeout(r.config.ElectionTimeout)
	//send vote requests
	electionResults, fallback := make(chan bool), make(chan bool)
	r.requestVotes(electionResults, fallback, r.GetCurrentTerm())
	//EVALUATE CHANNELS
	for {
		//random timeout representing timeout to switch to candidate state
		select {
		case wonElection := <- electionResults:
			if wonElection {
				return r.doLeader
			}
		case shouldFallback := <-fallback:
			if shouldFallback {
				return r.doFollower
			}
		case <-timeout:
			return r.doCandidate
		case appendEntriesMsg := <-r.appendEntries:
			//update term
			updated := r.updateTerm(appendEntriesMsg.request.Term)
			//handle appendEntriesMsg
			if _, fallback := r.handleAppendEntries(appendEntriesMsg); fallback || updated {
				return r.doFollower
			}
		case requestVoteMsg := <-r.requestVote:
			//update term
			updated := r.updateTerm(requestVoteMsg.request.Term)
			//handle appendEntriesMsg
			_ = r.handleRequestVote(&requestVoteMsg)
			//only fallback case is if term is updated
			if updated {
				return r.doFollower
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
	return nil
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (r *Node) requestVotes(electionResults chan bool, fallback chan bool, currTerm uint64) {
	//create request struct to send to peers
	request := RequestVoteRequest{
		Term:         currTerm,
		Candidate:    r.Self,
		LastLogIndex: r.LastLogIndex(),
		LastLogTerm:  r.GetLog(r.LastLogIndex()).TermId,
	}
	//vote counter for all OTHER nodes and implied vote from itself
	var votesGranted = 1
	//channel for replies
	var replies = make(chan *RequestVoteReply)
	//get current slice of peers
	var peers = r.getPeers()
	//iterate through peers
	for _, node := range peers {
		//parallel remote queries
		go func(node *RemoteNode, replies chan *RequestVoteReply) {
			reply, err := node.RequestVoteRPC(r, &request)
			if err != nil {
				//possible network error, non-breaking
				r.Error(err.Error())
			}
			replies <- reply
		}(node, replies)
	}
	//iterate number of peer times grabbing reply from remote node replies and asses reply
	for range peers {
		//grab reply from replies channel
		reply := <- replies
		if reply == nil {
			continue
		}
		//update term from reply
		if updated := r.updateTerm(reply.Term); updated {
			fallback <- true
			electionResults <- false
			return
		}
		if reply.VoteGranted {
			votesGranted += 1
		}
	}
	//grant vote depending on count
	if votesGranted >= (r.config.ClusterSize / 2) + 1 {
		fallback <- false
		electionResults <- true
	} else {
		fallback <- true
		electionResults <- false
	}
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *Node) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {
	//Don't need to do this for our implementation I think
	return
}
