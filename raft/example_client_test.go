package raft

import (
	"testing"
	"time"

	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/hashmachine"
	"golang.org/x/net/context"
)

// Example test making sure leaders can register the client and process the request from clients properly
func TestClientInteraction_Leader(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// First make sure we can register a client correctly
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Fatal("Could not register client")
	}
	println("register client successful")
	clientid := reply.ClientId

	// Hash initialization request
	initReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}
	println("commit client request successful")
	// Make sure further request is correct processed
	ClientReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     2,
		StateMachineCmd: hashmachine.HashChainAdd,
		Data:            []byte{},
	}
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}
}

// Example test making sure the follower would reject the registration and requests from clients with correct messages
// The test on candidates can be similar with these sample tests
func TestClientInteraction_Follower(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	// set the ElectionTimeout long enough to keep nodes in the state of follower
	config.ElectionTimeout = 60 * time.Second
	config.ClusterSize = 3
	config.HeartbeatTimeout = 500 * time.Millisecond
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	// make sure the client get the correct response while registering itself with a follower
	reply, _ := cluster[0].RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a follower")
	}

	// make sure the client get the correct response while sending a request to a follower
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := cluster[0].ClientRequestCaller(context.Background(), &req)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a follower")
	}
}
