package tests

import (
	"fmt"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/client"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/hashmachine"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/raft"
	"testing"
	"time"

)

//Test consistency with leader without failure (this also tests normal behavior when interacting with client)
func TestFollowerConsistencyForStandardLeader(t *testing.T) {
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

	for i := 0; i < 5; i++ {
		_, err = cp.SendRequest(hashmachine.HashChainAdd, nil)
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
}

//Test consistency with leader following network partition, also test appropriate change of term
func TestFollowerConsistencyForPartitionedLeader(t *testing.T) {
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
	//find leader
	leader, err := raft.FindLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	var isolatedNode *raft.Node
	//isolate some node
	for _, node := range cluster {
		if node.Self.Id != leader.Self.Id {
			isolatedNode = node
			node.NetworkPolicy.PauseWorld(true)
			break
		}
	}
	//perform some work on the leader and the non-paused node
	for i := 0; i < 5; i++ {
		_, err = cp.SendRequest(hashmachine.HashChainAdd, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	//wait for replication
	time.Sleep(time.Second * raft.WaitPeriod)
	//isolated node shouldn't match
	if raft.LogsMatch(leader, cluster) {
		t.Fatal("Isolated node matches non-isolated nodes")
	}
	if isolatedNode.State != raft.CandidateState {
		t.Fatal("Isolated node should be in candidate state")
	}
	//unpause node
	isolatedNode.NetworkPolicy.PauseWorld(false)
	//wait for replication
	time.Sleep(time.Second * raft.WaitPeriod)

	if !raft.LogsMatch(leader, cluster) {
		t.Fatal("Replication failed")
	}

	if cluster[0].GetCurrentTerm() < 2 {
		t.Fatal("Term update failure")
	}
}
