# Raft

Bugs: We have no known bugs which have been discovered from our testing. 

Our implementation follows the guidelines set in the project handout. One potentially significant difference from standard implementation is that we were able to work around using two different implementations for handleRequestVotes between followers and candidates/leaders. All states call handleRequestVotes in the follower.

Testing

append_entries_test.go tests how followers, candidates, and leaders respond to append entries messages for two cases. The first case is when the message comes from an out of date leader, and we test appropriate responses from all states. The second case is when the message comes from a valid leader, and we test appropriate responses from all states (note, we do not perform any consistency checks here which depend on proper response to append entries messages). There are three test functions, each for each respective state.

node_candidate_test.go tests how nodes respond to certain partition conditions. The main contribution tests in nod_candidate_tests lends to our testing framework is appropriate responses in leadership due to different possible partition cases. The test function TestAppropriateLeaderFollowingMinorityPartition tests how leadership responds when the leader is put into a partition of a minority of the cluster. It tests such things as the initial leader converting to a follower, and one of the majority nodes winning an election and converting to a leader. TestAppropriateLeaderFollowingMajorityPartition is similar to the previous test, except it tests what happens when the leader is put into a majority partition. It tests such things as the minority node becoming a candidate but not the leader when it rejoins the network. TestStochasticHecticNetwork tests general desireable behavior given random failures in the network such as the existence of a leader. 

node_follower_test.go tests a variety of follower consistency properties given certain network conditions. TestFollowerConsistencyForStandardLeader tests the most basic consistency properties of a fault-free network (followers become consistent with the leader). TestFollowerConsistencyForPartitionedLeader tests consistency properties of the network under partition conditions. Nodes partitioned into a minority cluster away from the leader do not become consistent though those in the majority do. We also test eventual consistency for minority nodes which are eventually moved into the majority. 

