package raft

import (
	"testing"
	"time"
)

// Tests that nodes can successfully join a cluster and elect a leader.
func TestConvertToCandidate(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)



	followers, candidates, leaders := 0, 0, 0
	for i := 0; i < config.ClusterSize; i++ {
		node := cluster[i]
		switch node.State {
		case FollowerState:
			followers++
		case CandidateState:
			candidates++
		case LeaderState:
			leaders++
		}
	}

	if followers != config.ClusterSize-1 {
		t.Errorf("follower count mismatch, expected %v, got %v", config.ClusterSize-1, followers)
	}

	if candidates != 0 {
		t.Errorf("candidate count mismatch, expected %v, got %v", 0, candidates)
	}

	if leaders != 1 {
		t.Errorf("leader count mismatch, expected %v, got %v", 1, leaders)
	}
}

