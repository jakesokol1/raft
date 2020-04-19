package raft

import "math"

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
	go r.requestVotes(electionResults, fallback, r.GetCurrentTerm())
	//EVALUATE CHANNELS
	for {
		//random timeout representing timeout to switch to candidate state
		select {
		case wonElection := <- electionResults:
			if wonElection {
				r.setLeader(r.Self)
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
			//TODO: Amy says to check if leader is nil and read piazza
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
	var yesVotes, noVotes = 1, 0
	//channel for replies
	var replies = make(chan *RequestVoteReply)
	//lock down peers while accessing
	r.nodeMutex.Lock()
	//get current slice of peers
	//iterate through peers
	for _, node := range r.Peers {
		if node.Id == r.Self.Id {
			continue
		}
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
	r.nodeMutex.Unlock()
	//iterate number of peer times grabbing reply from remote node replies and asses reply
	for  i := 0; i < r.config.ClusterSize - 1; i++ {
		reply := <- replies
		if reply == nil {
			noVotes += 1
			continue
		}
		//update term from reply
		if updated := r.updateTerm(reply.Term); updated {
			fallback <- true
			electionResults <- false
			return
		}
		//update and check vote counts for completion
		if reply.VoteGranted {
			yesVotes += 1
		} else {
			noVotes += 1
		}
		if yesVotes >= int(math.Ceil(float64(r.config.ClusterSize) / 2.)) {
			fallback <- false
			electionResults <- true
			return
		}
		if noVotes > int(math.Floor(float64(r.config.ClusterSize) / 2.)) {
			fallback <- true
			electionResults <- false
			return
		}
	}
	r.Error("Impossible vote condition, reverting to follower")
	fallback <- true
	electionResults <- false
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *Node) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {
	//Don't need to do this for our implementation I think
	return
}
