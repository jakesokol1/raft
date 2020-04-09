package raft

import (
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/brown-csci1380-s20/raft-jsokol2-mlitt2/hashmachine"
)

// CreateLocalCluster creates a new Raft cluster with the given config in the
// current process.
func CreateLocalCluster(config *Config) ([]*Node, error) {
	err := CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, config.ClusterSize)

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	nodes[0], err = CreateNode(OpenPort(0), nil, config, new(hashmachine.HashMachine), stableStore)
	if err != nil {
		Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore StableStore
		if config.InMemory {
			stableStore = NewMemoryStore()
		} else {
			stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
		}
		nodes[i], err = CreateNode(OpenPort(0), nodes[0].Self, config, new(hashmachine.HashMachine), stableStore)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalCluster creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalCluster(config *Config, ports []int) ([]*Node, error) {
	err := CheckConfig(config)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, config.ClusterSize)

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[0])))
	}
	nodes[0], err = CreateNode(OpenPort(ports[0]), nil, config, new(hashmachine.HashMachine), stableStore)
	if err != nil {
		Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore StableStore
		if config.InMemory {
			stableStore = NewMemoryStore()
		} else {
			stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[i])))
		}
		nodes[i], err = CreateNode(OpenPort(ports[i]), nodes[0].Self, config, new(hashmachine.HashMachine), stableStore)
		if err != nil {
			Error.Printf("Error creating %v-th node: %v", i, err)
			return nil, err
		}
	}

	return nodes, nil
}
