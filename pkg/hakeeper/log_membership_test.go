// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hakeeper

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestLogMembershipTargetsRequireReportedCurrentAndProjectedMajority(t *testing.T) {
	cfg := Config{}
	cfg.Fill()
	const tick = uint64(10000)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 5, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
	stores := make(map[string]pb.LogStoreInfo)
	for id, uuid := range shard.Replicas {
		stores[uuid] = pb.LogStoreInfo{Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
	}
	stores["f"] = pb.LogStoreInfo{Tick: tick}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: stores}
	expired := stores["a"]
	expired.Tick = 0
	stores["a"] = expired
	expired = stores["b"]
	expired.Tick = 0
	stores["b"] = expired
	remove := pb.Replica{ShardID: 1, ReplicaID: 1, UUID: "a", Epoch: 5}
	require.Equal(t, []string{"c", "d", "e"}, LogMembershipTargets(cfg, state, tick, pb.RemoveReplica, remove),
		"the executor must be a reporting retained voter, not the first configured UUID")
	remove.ReplicaID, remove.UUID = 3, "c"
	require.Empty(t, LogMembershipTargets(cfg, state, tick, pb.RemoveReplica, remove),
		"removing a live voter would leave less than the projected majority")
	add := pb.Replica{ShardID: 1, ReplicaID: 6, UUID: "f", Epoch: 5}
	require.Empty(t, LogMembershipTargets(cfg, state, tick, pb.AddReplica, add),
		"an unstarted sixth voter raises the majority")
	require.Equal(t, []string{"c", "d", "e"}, LogMembershipTargets(cfg, state, tick, pb.AddNonVotingReplica, add))

	// A live store reporting another replica, or role cannot
	// be counted as an available voting member.
	c := stores["c"]
	c.Replicas[0].ReplicaID = 99
	stores["c"] = c
	remove.ReplicaID, remove.UUID = 1, "a"
	require.Empty(t, LogMembershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	require.Empty(t, LogMembershipTargets(cfg, state, tick, pb.AddNonVotingReplica, add))
	c.Replicas[0].ReplicaID = 3
	c.Replicas[0].IsNonVoting = true
	stores["c"] = c
	require.Empty(t, LogMembershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	c.Replicas[0].IsNonVoting = false
	stores["c"] = c
	require.Equal(t, []string{"c", "d", "e"}, LogMembershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
	remove.Epoch--
	require.Empty(t, LogMembershipTargets(cfg, state, tick, pb.RemoveReplica, remove))
}

func TestHAKeeperMembershipTargetsPreserveCommitQuorum(t *testing.T) {
	cfg := Config{}
	cfg.Fill()
	const tick = uint64(10000)
	cases := []struct {
		name    string
		size    int
		shard   uint64
		change  pb.ConfigChangeType
		allowed bool
	}{
		{name: "singleton cannot add an unstarted voter", size: 1, change: pb.AddReplica},
		{name: "two can add", size: 2, change: pb.AddReplica, allowed: true},
		{name: "two can remove", size: 2, change: pb.RemoveReplica, allowed: true},
		{name: "three cannot add", size: 3, change: pb.AddReplica},
		{name: "three cannot remove", size: 3, change: pb.RemoveReplica},
		{name: "four can add", size: 4, change: pb.AddReplica, allowed: true},
		{name: "four can remove", size: 4, change: pb.RemoveReplica, allowed: true},
		{name: "five cannot remove", size: 5, change: pb.RemoveReplica},
		{name: "adding a non-voter leaves the majority unchanged", size: 3, change: pb.AddNonVotingReplica, allowed: true},
		{name: "removing a non-voter leaves the majority unchanged", size: 3, change: pb.RemoveNonVotingReplica, allowed: true},
		{name: "ordinary shard can remove with enough reports", size: 3, shard: 1, change: pb.RemoveReplica, allowed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shard := pb.LogShardInfo{ShardID: tc.shard, Epoch: 1, Replicas: make(map[uint64]string, tc.size)}
			stores := map[string]pb.LogStoreInfo{"spare": {Tick: tick}}
			for i, uuid := range []string{"a", "b", "c", "d", "e"}[:tc.size] {
				id := uint64(i + 1)
				shard.Replicas[id] = uuid
				stores[uuid] = pb.LogStoreInfo{Tick: tick, Replicas: []pb.LogReplicaInfo{
					{LogShardInfo: pb.LogShardInfo{ShardID: tc.shard}, ReplicaID: id},
				}}
			}
			if tc.change == pb.RemoveNonVotingReplica {
				shard.NonVotingReplicas = map[uint64]string{6: "spare"}
			}
			state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{tc.shard: shard}, Stores: stores}
			replica := pb.Replica{ShardID: tc.shard, Epoch: shard.Epoch, ReplicaID: 6, UUID: "spare"}
			if tc.change == pb.RemoveReplica {
				replica.ReplicaID, replica.UUID = 1, "a"
			}
			targets := LogMembershipTargets(cfg, state, tick, tc.change, replica)
			require.Equal(t, tc.allowed, len(targets) != 0)
			require.NotContains(t, targets, replica.UUID)
		})
	}
}

func TestLogScheduleQueryIsScopedAndIndependent(t *testing.T) {
	machine := NewStateMachine(0, 1).(*stateMachine)
	machine.state.Tick = 30
	machine.state.State = pb.HAKeeperRunning
	machine.state.LogState.Shards[1] = pb.LogShardInfo{ShardID: 1, Epoch: 4, Replicas: map[uint64]string{1: "a"}, NonVotingReplicas: map[uint64]string{2: "b"}}
	machine.state.LogState.Shards[2] = pb.LogShardInfo{ShardID: 2, Replicas: map[uint64]string{9: "other"}}
	machine.state.LogState.Stores["a"] = pb.LogStoreInfo{Tick: 29, Replicas: []pb.LogReplicaInfo{{LogShardInfo: pb.LogShardInfo{ShardID: 1}, ReplicaID: 1}, {LogShardInfo: pb.LogShardInfo{ShardID: 2}, ReplicaID: 9}}}
	machine.state.LogState.Stores["b"] = pb.LogStoreInfo{Tick: 28, Replicas: []pb.LogReplicaInfo{{LogShardInfo: pb.LogShardInfo{ShardID: 1}, ReplicaID: 2, IsNonVoting: true}}}
	machine.state.LogState.Stores["target"] = pb.LogStoreInfo{Tick: 29}
	machine.state.LogState.Stores["unrelated"] = pb.LogStoreInfo{Tick: 29}
	value, err := machine.Lookup(&StateQuery{LogSchedule: &pb.LogScheduleQuery{ShardID: 1, TargetStore: "target"}})
	require.NoError(t, err)
	result := value.(*pb.CheckerState)
	require.Equal(t, uint64(30), result.Tick)
	require.Equal(t, pb.HAKeeperRunning, result.State)
	require.Len(t, result.LogState.Shards, 1)
	require.Len(t, result.LogState.Stores, 3)
	require.Len(t, result.LogState.Stores["a"].Replicas, 1)
	require.True(t, result.LogState.Stores["b"].Replicas[0].IsNonVoting)
	require.Empty(t, result.CNState.Stores)
	result.LogState.Shards[1].Replicas[1] = "changed"
	require.Equal(t, "a", machine.state.LogState.Shards[1].Replicas[1], "query must not expose replicated state")
	value, err = machine.Lookup(&StateQuery{LogSchedule: &pb.LogScheduleQuery{ShardID: 99}})
	require.NoError(t, err)
	require.Empty(t, value.(*pb.CheckerState).LogState.Shards)
}
