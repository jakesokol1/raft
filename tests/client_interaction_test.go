package tests

import (
	"fmt"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/client"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/hashmachine"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/raft"
	"testing"
	"time"
)

//Test handle append entries of different types to the follower
func TestClientInteractionFollower(t *testing.T) {
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
	for _, node := range cluster {
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

	reply, _ := follower.RegisterClientCaller(nil, &raft.RegisterClientRequest{})
	if reply.LeaderHint != follower.Leader && reply.Status != raft.ClientStatus_NOT_LEADER {
		t.Fatal("bad follower reply to client register")
	}

	reply2, _ := follower.ClientRequestCaller(nil, &raft.ClientRequest{})
	if reply2.LeaderHint != follower.Leader && reply2.Status != raft.ClientStatus_NOT_LEADER {
		t.Fatal("bad follower reply to client request")
	}
}

func TestClientInteractionCandidate(t *testing.T) {
	//create cluster
	config := raft.DefaultConfig()
	config.ClusterSize = 5
	cluster, err := raft.CreateLocalCluster(config)
	defer raft.CleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * raft.WaitPeriod)

	leader, err := raft.FindLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	var iso *raft.Node
	for _, node := range cluster {
		if node.Self.Id != leader.Self.Id {
			iso = node
			iso.NetworkPolicy.PauseWorld(true)
			break
		}
	}

	time.Sleep(time.Second * raft.WaitPeriod)

	if iso.State != raft.CandidateState {
		t.Fatal()
	}

	reply, _ := iso.RegisterClientCaller(nil, &raft.RegisterClientRequest{})
	if reply.LeaderHint != iso.Leader && reply.Status != raft.ClientStatus_NOT_LEADER {
		t.Fatal("bad follower reply to client register")
	}

	reply2, _ := iso.ClientRequestCaller(nil, &raft.ClientRequest{})
	if reply2.LeaderHint != iso.Leader && reply2.Status != raft.ClientStatus_NOT_LEADER {
		t.Fatal("bad follower reply to client request")
	}
}

func TestLeaderClientInteraction(t *testing.T) {
	//create cluster
	config := raft.DefaultConfig()
	cluster, err := raft.CreateLocalCluster(config)
	defer raft.CleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * raft.WaitPeriod)
	//connect client
	cp, err := client.Connect(cluster[0].Self.Addr)
	if err != nil {
		t.Fatal(err)
	}
	cp2, err := client.Connect(cluster[0].Self.Addr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cp.SendRequest(hashmachine.HashChainInit, []byte{1, 2, 3, 4})
	_, err = cp2.SendRequest(hashmachine.HashChainInit, []byte{9})
	for i := 0; i < 5; i++ {
		_, err = cp2.SendRequest(hashmachine.HashChainAdd, []byte{2})
		_, err = cp.SendRequest(hashmachine.HashChainAdd, []byte{1, 2, 3, 4})
		if err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("Done with requests")
	//wait for replication
	time.Sleep(time.Second * raft.WaitPeriod)

	if leader, err := raft.FindLeader(cluster); !raft.LogsMatch(leader, cluster) || err != nil {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("logs do not match")
	}
	raft.PrintCluster(cluster)
}
