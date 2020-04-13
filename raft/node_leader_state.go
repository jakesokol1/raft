package raft

// doLeader implements the logic for a Raft node in the leader state.
func (r *Node) doLeader() stateFunction {
	r.Out("Transitioning to LeaderState")
	r.State = LeaderState

	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.

	// initial work
	r.sendHeartbeats()
	// receive messages on all channels
	for {
		//random timeout representing timeout to switch to candidate state
		select {
		case appendEntriesMsg := <-r.appendEntries:
			// todo: how to handle another leader?
			println("Leader got append entries message: " + string(appendEntriesMsg.request.Term))
		case requestVoteMsg := <-r.requestVote:
			// todo @722. step down and vote if candidate is more up to date, otherwise reject vote
			println("leader got requestVote: " + string(requestVoteMsg.request.Term))
		case registerClientMsg := <-r.registerClient:
			// todo
			// store a RegisterClient log
			println(registerClientMsg.request.String())
			log := &LogEntry{
				Index:  r.stableStore.LastLogIndex() + 1,
				TermId: r.GetCurrentTerm(),
				Type:   CommandType_CLIENT_REGISTRATION,
				// ignore
				Command: 0,
				Data:    nil,
				// todo?
				CacheId: "",
			}
			r.stableStore.StoreLog(log)
			fallback, _ := r.sendHeartbeats()
			if fallback {
				return r.doFollower()
			}

			// todo respond with id? ??????
			r.processLogEntry(*log)
		case clientRequestMsg := <-r.clientRequest:
			// todo: append entry to local log, respond after entry applied to state machine
			//r.GetCachedReply(clientRequestMsg.request)
			println(clientRequestMsg.request.Data)
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (r *Node) sendHeartbeats() (fallback, sentToMajority bool) {
	// TODO: Send asynchronously?
	for _, node := range r.Peers {
		reply, _ := node.AppendEntriesRPC(r, &AppendEntriesRequest{ // todo, handle error
			Term:         r.GetCurrentTerm(),
			Leader:       r.Self,
			PrevLogIndex: r.lastApplied, // todo, confirm
			PrevLogTerm:  0,             //todo
			Entries:      nil,           //todo
			LeaderCommit: r.commitIndex,
		})
		succ := reply.Success
		return succ, true
		//reply.Term
	}
	return true, true
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (r *Node) processLogEntry(entry LogEntry) ClientReply {
	r.Out("Processing log entry: %v", entry)

	status := ClientStatus_OK
	response := []byte{}
	var err error

	// Apply command on state machine
	if entry.Type == CommandType_STATE_MACHINE_COMMAND {
		response, err = r.stateMachine.ApplyCommand(entry.Command, entry.Data)
		if err != nil {
			status = ClientStatus_REQ_FAILED
			response = []byte(err.Error())
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		Response:   response,
		LeaderHint: r.Self,
	}

	// Add reply to cache
	if entry.CacheId != "" {
		r.CacheClientReply(entry.CacheId, reply)
	}

	// Send reply to client
	r.requestsMutex.Lock()
	replyChan, exists := r.requestsByCacheID[entry.CacheId]
	if exists {
		replyChan <- reply
		delete(r.requestsByCacheID, entry.CacheId)
	}
	r.requestsMutex.Unlock()

	return reply
}
