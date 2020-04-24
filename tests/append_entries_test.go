package tests

import (
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/raft"
	"testing"
	"time"
)

//Test handle append entries of different types to the follower
func TestHandleAppendEntriesFollower(t *testing.T) {
	//create cluster
	config := raft.DefaultConfig()
	cluster, err := raft.CreateLocalCluster(config)
	defer raft.CleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * raft.WaitPeriod)
	//find leader
	leader, err := raft.FindLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	var follower *raft.Node
	for _, node := range cluster{
		if node.Self.Id != leader.Self.Id {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatal("nil follower")
	}
	if follower.GetCurrentTerm() < 1 {
		t.Fatal("invalid follower term")
	}

	fakeLeader := raft.RemoteNode{
		Addr:                 "test",
		Id:                   "fakeLeader",
	}

	requestOutOfDate := raft.AppendEntriesRequest{
		Term:                 follower.GetCurrentTerm() - 1,
		Leader:               &fakeLeader,
		PrevLogIndex:         0,
		PrevLogTerm:          0,
		Entries:              nil,
		LeaderCommit:		  0,
	}

	requestValid := raft.AppendEntriesRequest{
		Term:                 follower.GetCurrentTerm() + 1,
		Leader:               &fakeLeader,
		PrevLogIndex:         0,
		PrevLogTerm:          0,
		Entries:              nil,
		LeaderCommit:         0,
	}

	reply := follower.AppendEntries(&requestOutOfDate)

	if reply.Success {
		t.Fatal("follower fell back to lower term leader")
	}

	reply = follower.AppendEntries(&requestValid)

	if !reply.Success {
		t.Fatal("follower didn't accept valid request")
	}
}

func TestHandleAppendEntriesCandidate(t *testing.T) {
	config := raft.DefaultConfig()
	cluster, err := raft.CreateLocalCluster(config)
	defer raft.CleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * raft.WaitPeriod)
	//find leader
	leader, err := raft.FindLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	var candidate *raft.Node
	for _, node := range cluster{
		if node.Self.Id != leader.Self.Id {
			candidate = node
			break
		}
	}
	if candidate == nil {
		t.Fatal("nil follower")
	}
	if candidate.GetCurrentTerm() < 1 {
		t.Fatal("invalid follower term")
	}

	//partition candidate to bring it to candidate state
	candidate.NetworkPolicy.PauseWorld(true)
	// wait for a candidate to change state
	time.Sleep(time.Second * 2)

	if candidate.State != raft.CandidateState {
		t.Fatal("Invalid candidate state")
	}

	fakeLeader := raft.RemoteNode{
		Addr:                 "test",
		Id:                   "fakeLeader",
	}

	requestOutOfDate := raft.AppendEntriesRequest{
		Term:                 candidate.GetCurrentTerm() - 1,
		Leader:               &fakeLeader,
		PrevLogIndex:         0,
		PrevLogTerm:          0,
		Entries:              nil,
		LeaderCommit:		  0,
	}

	requestValid := raft.AppendEntriesRequest{
		Term:                 candidate.GetCurrentTerm() + 1,
		Leader:               &fakeLeader,
		PrevLogIndex:         0,
		PrevLogTerm:          0,
		Entries:              nil,
		LeaderCommit:         0,
	}

	response := candidate.AppendEntries(&requestOutOfDate)
	if response.Success {
		t.Fatal("Invalid response to out of date request")
	}

	response = candidate.AppendEntries(&requestValid)
	if !response.Success {
		t.Fatal()
	}
}

func TestHandleAppendEntriesLeader(t *testing.T) {
	config := raft.DefaultConfig()
	cluster, err := raft.CreateLocalCluster(config)
	defer raft.CleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * raft.WaitPeriod)
	//find leader
	leader, err := raft.FindLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	if leader.State != raft.LeaderState {
		t.Fatal("Invalid candidate state")
	}

	if leader.GetCurrentTerm() < 1 {
		t.Fatal("Invalid leader term")
	}

	fakeLeader := raft.RemoteNode{
		Addr:                 "test",
		Id:                   "fakeLeader",
	}

	requestOutOfDate := raft.AppendEntriesRequest{
		Term:                 leader.GetCurrentTerm() - 1,
		Leader:               &fakeLeader,
		PrevLogIndex:         0,
		PrevLogTerm:          0,
		Entries:              nil,
		LeaderCommit:		  0,
	}

	requestValid := raft.AppendEntriesRequest{
		Term:                 leader.GetCurrentTerm() + 1,
		Leader:               &fakeLeader,
		PrevLogIndex:         0,
		PrevLogTerm:          0,
		Entries:              nil,
		LeaderCommit:         0,
	}

	response := leader.AppendEntries(&requestOutOfDate)
	if response.Success {
		t.Fatal("Invalid response to out of date request")
	}

	response = leader.AppendEntries(&requestValid)
	if !response.Success {
		t.Fatal()
	}
}