package raft

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// RPCTimeout is default timeout for rpc client calls
const RPCTimeout = 2 * time.Second

var dialOptions []grpc.DialOption

func init() {
	dialOptions = []grpc.DialOption{grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(2 * time.Second), grpc.WithUnaryInterceptor(clientUnaryInterceptor)}
}

// clientUnaryInterceptor is a client unary interceptor that injects a default timeout
func clientUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx, cancel := context.WithTimeout(ctx, RPCTimeout)
	defer cancel()

	return invoker(ctx, method, req, reply, cc, opts...)
}

// RPC Invocation functions

var connMap = make(map[string]*grpc.ClientConn)
var connMapLock = &sync.RWMutex{}

// makeClientConn creates a new client connection to the given remote node
func makeClientConn(remote *RemoteNode) (*grpc.ClientConn, error) {
	return grpc.Dial(remote.Addr, dialOptions...)
}

// ClientConn creates or returns a cached RPC client for the given remote node
func (remote *RemoteNode) ClientConn() (RaftRPCClient, error) {
	connMapLock.RLock()
	if cc, ok := connMap[remote.Addr]; ok {
		connMapLock.RUnlock()
		return NewRaftRPCClient(cc), nil
	}
	connMapLock.RUnlock()

	cc, err := makeClientConn(remote)
	if err != nil {
		return nil, err
	}
	connMapLock.Lock()
	connMap[remote.Addr] = cc
	connMapLock.Unlock()

	return NewRaftRPCClient(cc), err
}

// RemoveClientConn removes the client connection to the given node, if present
func (remote *RemoteNode) RemoveClientConn() {
	connMapLock.Lock()
	defer connMapLock.Unlock()
	if cc, ok := connMap[remote.Addr]; ok {
		cc.Close()
		delete(connMap, remote.Addr)
	}
}

// connCheck checks the given error and removes the client connection if it's not nil
func (remote *RemoteNode) connCheck(err error) error {
	if err != nil {
		time.Sleep(RPCTimeout)
		remote.RemoveClientConn()
	}
	return err
}

// JoinRPC tells the given remote node that we (a new Raft node) want to join the cluster
func (remote *RemoteNode) JoinRPC(local *Node) error {
	// Get a client connection for the remote node
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}

	ok, err := cc.JoinCaller(context.Background(), local.Self)
	if err == nil && !ok.GetOk() {
		return fmt.Errorf("unable to join Raft cluster")
	}

	// Check for an RPC error, and close the client connection if necessary.
	// Be sure to use this in the rest of the functions in this file!
	return remote.connCheck(err)
}

// StartNodeRPC tells the remote node to start execution with the given list
// of RemoteNodes as the list of all the nodes in the cluster.
func (remote *RemoteNode) StartNodeRPC(local *Node, nodeList []*RemoteNode) error {
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}

	request := StartNodeRequest{FromNode: local.Self, NodeList: nodeList}

	ok, err := cc.StartNodeCaller(context.Background(), &request)
	if !ok.GetOk() {
		return fmt.Errorf("unable to start node")
	}

	return remote.connCheck(err)
}

// AppendEntriesRPC is called by a leader in the cluster attempting to append
// entries to one of its followers. Be sure to pass in a pointer to the Node
// making the request.
func (remote *RemoteNode) AppendEntriesRPC(local *Node, request *AppendEntriesRequest) (*AppendEntriesReply, error) {
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return nil, ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.AppendEntriesCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}

// RequestVoteRPC asks the given remote node for a vote, using the provided
// RequestVoteRequest struct as the request. Note that calling nodes should
// pass in a pointer to their own Node struct.
func (remote *RemoteNode) RequestVoteRPC(local *Node, request *RequestVoteRequest) (*RequestVoteReply, error) {
	if local.NetworkPolicy.IsDenied(*local.Self, *remote) {
		return nil, ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.RequestVoteCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}

// RegisterClientRPC is called by a new client trying to register itself with
// the given Raft node in the cluster.
func (remote *RemoteNode) RegisterClientRPC() (*RegisterClientReply, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	request := RegisterClientRequest{}

	reply, err := cc.RegisterClientCaller(context.Background(), &request)
	return reply, remote.connCheck(err)
}

// ClientRequestRPC is executed by a client trying to make a request to the
// given Raft node in the cluster.
func (remote *RemoteNode) ClientRequestRPC(request *ClientRequest) (*ClientReply, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.ClientRequestCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}
