package tests

import (
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/client"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/hashmachine"
	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/raft"
	"math/rand"
	"testing"
	"time"
)

//Test appropriate leader after partition where leadership should change
func TestAppropriateLeaderFollowingMinorityPartition(t *testing.T) {
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

	leader.NetworkPolicy.PauseWorld(true)
	//wait for new leader
	time.Sleep(time.Second * raft.WaitPeriod)

	leader.NetworkPolicy.PauseWorld(false)
	//wait for new newLeader update
	time.Sleep(time.Second * raft.WaitPeriod)
	newLeader, err := raft.FindLeader(cluster)

	if newLeader.Self.Id == leader.Self.Id {
		t.Error("Should have changed leadership following partition")
	}
}

func TestAppropriateLeaderFollowingMajorityPartition(t *testing.T) {
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

	var liability *raft.Node
	for _, node := range cluster {
		if node != leader {
			liability = node
			break
		}
	}

	liability.NetworkPolicy.PauseWorld(true)

	cp, err := client.Connect(leader.Self.Addr)
	if err != nil {
		t.Fatal(err)
	}


	cp.SendRequest(hashmachine.HashChainAdd, nil)


	//time for replication
	time.Sleep(time.Second * raft.WaitPeriod)

	//time for new election
	liability.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * raft.WaitPeriod)

	newLeader, err := raft.FindLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	if newLeader.Self.Id == liability.Self.Id {
		t.Error("Leader should not be partitioned node")
	}
}

//tests existence of leader in a hectic network
func TestStochasticHecticNetwork(t *testing.T) {

	//create cluster
	config := raft.DefaultConfig()
	config.ClusterSize = 5
	cluster, err := raft.CreateLocalCluster(config)
	defer raft.CleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 40; i++ {
		//make random pause for every node
		for _, node := range cluster {
			node.NetworkPolicy.PauseWorld(rand.Float32() < 0.5)
		}
		trials := 3
		for {
			if _, err := raft.FindLeader(cluster); err != nil && trials == 0{
				if trials == 0 {
					t.Fatal(err)
				}
				trials--
				time.Sleep(time.Millisecond * 200)
				continue
			}
			break
		}
		//wait for election activity
		time.Sleep(time.Second)
	}
}
