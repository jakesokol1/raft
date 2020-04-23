package raft

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"time"

	"google.golang.org/grpc/grpclog"
)

// WaitPeriod is...
const (
	WaitPeriod = 6
)

func suppressLoggers() {
	Out.SetOutput(ioutil.Discard)
	Error.SetOutput(ioutil.Discard)
	Debug.SetOutput(ioutil.Discard)
	grpclog.SetLogger(Out)
}

// Creates a cluster of nodes at specific ports, with a
// more lenient election timeout for testing.
func createTestCluster(ports []int) ([]*Node, error) {
	SetDebug(false)
	config := DefaultConfig()
	config.ClusterSize = len(ports)
	config.ElectionTimeout = time.Millisecond * 400

	return CreateDefinedLocalCluster(config, ports)
}

// Returns the leader in a raft cluster, and an error otherwise.
func FindLeader(nodes []*Node) (*Node, error) {
	leaders := make([]*Node, 0)
	for _, node := range nodes {
		if node.State == LeaderState {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) == 0 {
		return nil, fmt.Errorf("No leader found in slice of nodes")
	} else if len(leaders) == 1 {
		return leaders[0], nil
	} else {
		return nil, fmt.Errorf("Found too many leaders in slice of nodes: %v", len(leaders))
	}
}

// Returns whether all logs in a cluster match the leader's.
func LogsMatch(leader *Node, nodes []*Node) bool {
	for _, node := range nodes {
		if node.State != LeaderState {
			if bytes.Compare(node.stateMachine.GetState().([]byte), leader.stateMachine.GetState().([]byte)) != 0 {
				return false
			}
		}
	}
	return true
}

// Given a slice of RaftNodes representing a cluster,
// exits each node and removes its logs.
func CleanupCluster(nodes []*Node) {
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		node.server.Stop()
		go func(node *Node) {
			node.GracefulExit()
			node.RemoveLogs()
		}(node)
	}
	time.Sleep(5 * time.Second)
}
