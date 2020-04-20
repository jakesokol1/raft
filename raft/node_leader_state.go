package raft

import (
	"go.uber.org/atomic"
	"sync"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (r *Node) doLeader() stateFunction {
	r.Out("Transitioning to LeaderState")
	r.State = LeaderState

	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.

	// initial work
	for _, node := range r.Peers {
		i := r.LastLogIndex() + 1
		if i <= 0 {
			i = 0
		}
		r.matchIndex[node.GetAddr()] = 0
	}
	inauguralEntry := LogEntry{
		Index:  r.LastLogIndex(),
		TermId: r.GetCurrentTerm(),
		Type:   CommandType_NOOP,
	}
	r.stableStore.StoreLog(&inauguralEntry)
	r.processLogEntry(inauguralEntry)

	//create a channel for communication with concurrent heartbeats
	fallback := make(chan bool)
	go r.sendHeartbeatsHandler(fallback)
	//r.sendHeartbeats()
	heartbeatTimeout := leaderTimeout()
	// receive messages on all channels
	for {
		select {
		case shouldFallback := <-fallback:
			if shouldFallback {
				r.Out("Falling back to follower")
				r.doFollower()
			}
		case _ = <-heartbeatTimeout:

			go r.sendHeartbeatsHandler(fallback)
			//fallback, commit := r.sendHeartbeats()
			//if fallback {
			//	r.doFollower()
			//}

			heartbeatTimeout = leaderTimeout()
		case appendEntriesMsg := <-r.appendEntries:
			r.Out("Leader got append entries message: " + string(appendEntriesMsg.request.Term))
			if appendEntriesMsg.request.Leader == r.Self {
				appendEntriesMsg.reply <- AppendEntriesReply{
					Term:    r.GetCurrentTerm(),
					Success: true,
				}
			} else {
				if appendEntriesMsg.request.Term > r.GetCurrentTerm() {
					r.Leader = appendEntriesMsg.request.Leader
					r.handleAppendEntries(appendEntriesMsg)
					r.doFollower()
				} else {
					appendEntriesMsg.reply <- AppendEntriesReply{
						Term:    r.GetCurrentTerm(),
						Success: false,
					}
				}
			}
		case requestVoteMsg := <-r.requestVote:
			// todo @722. step down and vote if candidate is more up to date, otherwise reject vote
			r.Out("leader got requestVote: " + string(requestVoteMsg.request.Term))
			if requestVoteMsg.request.Term > r.GetCurrentTerm() {
				r.handleRequestVote(&requestVoteMsg)
				r.doFollower()
			} else {
				requestVoteMsg.reply <- RequestVoteReply{
					Term:        r.GetCurrentTerm(),
					VoteGranted: false,
				}
			}
		case registerClientMsg := <-r.registerClient:
			replyChan := registerClientMsg.reply
			// store a RegisterClient log
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
			fallback, sentToMajority := r.sendHeartbeats()
			heartbeatTimeout = leaderTimeout()
			if fallback {
				return r.doFollower()
			}
			if sentToMajority {
				replyChan <- RegisterClientReply{
					Status:     ClientStatus_OK,
					ClientId:   r.stableStore.LastLogIndex(),
					LeaderHint: r.Self,
				}
				// COMMIT ENTRY
				r.processLogEntry(*log)
			} else {
				replyChan <- RegisterClientReply{
					Status:     ClientStatus_REQ_FAILED,
					ClientId:   0,
					LeaderHint: r.Self,
				}
			}
		case clientRequestMsg := <-r.clientRequest:
			// todo: append entry to local log, respond after entry applied to state machine
			cachedReply, success := r.GetCachedReply(*clientRequestMsg.request)
			if success {
				// todo: reply to old reply channel, if exists
				clientRequestMsg.reply <- *cachedReply
			} else {
				log := &LogEntry{
					Index:   r.stableStore.LastLogIndex() + 1,
					TermId:  r.GetCurrentTerm(),
					Type:    CommandType_STATE_MACHINE_COMMAND,
					Command: clientRequestMsg.request.StateMachineCmd,
					Data:    clientRequestMsg.request.Data,
					// todo
					CacheId: "",
				}
				r.stableStore.StoreLog(log)
				r.processLogEntry(*log)
			}
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

func (r *Node) sendHeartbeatsHandler(fallChan chan bool) {

	fallback, sentToMajority := r.sendHeartbeats()
	if sentToMajority {

		//using helper in node
		r.updateCommitment(r.LastLogIndex())

		//prevCommit := r.commitIndex
		//r.commitIndex = r.stableStore.LastLogIndex()
		//for _, entry := range r.stableStore.AllLogs()[prevCommit+1:] {
		//	r.processLogEntry(*entry)
		//}
		//r.lastApplied = r.commitIndex
	}
	fallChan <- fallback
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
	// todo: handle hanging rpcs
	var wg sync.WaitGroup
	var mtx sync.Mutex
	fallback = false
	numSuccess := atomic.NewInt32(0)
	r.Out("Sending heartbeats...")
	for _, node := range r.Peers {
		if node.Id == r.Self.Id {
			numSuccess.Add(1)
			continue
		}
		wg.Add(1)
		go func(wg *sync.WaitGroup, node *RemoteNode, numSuccess *atomic.Int32) {
			defer wg.Done()
			prevIndex := r.nextIndex[node.GetAddr()] - 1
			reply, err := node.AppendEntriesRPC(r, &AppendEntriesRequest{ // todo, handle error
				Term:         r.GetCurrentTerm(),
				Leader:       r.Self,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  r.stableStore.GetLog(prevIndex).GetTermId(), //todo, test
				Entries:      r.stableStore.AllLogs()[prevIndex+1:],
				LeaderCommit: r.commitIndex,
			})
			if reply == nil {
				if err == nil {
					r.Error("Weird case where reply is nil but error isn't")
				}
				return
			}
			mtx.Lock()
			if reply.Term > r.GetCurrentTerm() {
				fallback = true
			}
			if reply.GetSuccess() {
				println("append success")
				numSuccess.Add(1)
			} else {
				if r.nextIndex[node.GetAddr()] != 0 {
					r.nextIndex[node.GetAddr()]--
				}
			}
			mtx.Unlock()
		}(&wg, node, numSuccess)
	}
	wg.Wait()
	//println(numSuccess.Load())
	r.Out("Finished")
	return fallback, int(numSuccess.Load()) > len(r.Peers)/2
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

func leaderTimeout() <-chan time.Time {
	return time.After(DefaultConfig().HeartbeatTimeout)
}
