package raft

// StateMachine is a general interface defining the methods a Raft state machine
// should implement. For this project, we use a HashMachine as our state machine.
type StateMachine interface {
	GetState() (state interface{}) // Useful for testing once you use type assertions to convert the state
	ApplyCommand(command uint64, data []byte) ([]byte, error)
}
