package raft

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *Node) doCandidate() stateFunction {
	r.Out("Transitioning to CandidateState")
	r.State = CandidateState
	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.
	return nil
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (r *Node) requestVotes(electionResults chan bool, fallback chan bool, currTerm uint64) {
	// TODO: Students should implement this method
	return
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *Node) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	return true
}
