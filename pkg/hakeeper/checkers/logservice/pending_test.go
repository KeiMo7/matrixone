// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestPendingRemovalWaitsForInFlightStart(t *testing.T) {
	for _, nonVoting := range []bool{false, true} {
		name := "voting"
		if nonVoting {
			name = "nonvoting"
		}
		t.Run(name, func(t *testing.T) {
			cfg := hakeeper.Config{}
			cfg.Fill()
			fields := hakeeper.NewCheckerCommonFields("", cfg, nil,
				pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
				pb.TaskTableUser{}, 1)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, LeaderID: 1,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			if nonVoting {
				shard.NonVotingReplicas = map[uint64]string{5: "e"}
			} else {
				shard.Replicas[5] = "e"
			}
			state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{"e": {Tick: 1}}}
			for id, uuid := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
				state.Stores[uuid] = pb.LogStoreInfo{Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
			}
			replica := operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 7}
			var start, remove *operator.Operator
			starting := operator.ExecutingReplicas{Starting: map[uint64][]uint64{1: {5}}}
			var voting, observers operator.ExecutingReplicas
			if nonVoting {
				start = operator.CreateStartNonVotingReplica("", "e", 1, 5)
				remove = operator.NewOperator("", 1, 7, operator.RemoveNonVotingLogService{Target: "a", Replica: replica})
				observers = starting
			} else {
				start = operator.CreateStartReplica("", "e", 1, 5)
				remove = operator.NewOperator("", 1, 7, operator.RemoveLogService{Target: "a", Replica: replica})
				voting = starting
			}
			checker := NewLogServiceChecker(fields, state, pb.TNState{}, voting, observers, 0, pb.Locality{}, false)
			for _, op := range checker.Check() {
				for _, step := range op.OpSteps() {
					switch step := step.(type) {
					case operator.RemoveLogService:
						require.NotEqual(t, uint64(5), step.ReplicaID)
					case operator.RemoveNonVotingLogService:
						require.NotEqual(t, uint64(5), step.ReplicaID)
					}
				}
			}
			for _, ops := range [][]*operator.Operator{{start, remove}, {remove, start}} {
				got := PendingOperators(fields, state, voting, observers, 0, ops, nil)
				require.Empty(t, got, "neither previously delivered command is safe to resend")
			}
			oldReplica := replica
			oldReplica.UUID = "old-store"
			var oldRemove *operator.Operator
			if nonVoting {
				oldRemove = operator.NewOperator("", 1, 7, operator.RemoveNonVotingLogService{Target: "a", Replica: oldReplica})
			} else {
				oldRemove = operator.NewOperator("", 1, 7, operator.RemoveLogService{Target: "a", Replica: oldReplica})
			}
			require.Empty(t, PendingOperators(fields, state, voting, observers, 0, []*operator.Operator{start, oldRemove}, nil),
				"conflicts use shard and replica ID even if an old command has another UUID")
			got := PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{remove}, nil)
			require.Equal(t, []*operator.Operator{remove}, got, "removal resumes when no Start is tracked")
			if !nonVoting {
				shard.Replicas[4] = "d"
				state.Shards[1] = shard
				state.Stores["d"] = pb.LogStoreInfo{Tick: 1}
				other := operator.NewOperator("", 1, 7, operator.RemoveLogService{
					Target: "a", Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4, Epoch: 7},
				})
				got = PendingOperators(fields, state, voting, observers, 0, []*operator.Operator{start, other}, nil)
				require.ElementsMatch(t, []*operator.Operator{start, other}, got,
					"a Start must not block a different safe removal")
			}
		})
	}
}

func TestPendingOperatorsFollowCurrentLogCandidates(t *testing.T) {
	tests := []struct {
		name              string
		step              operator.OpStep
		normal, nonVoting uint32
		mutate            func(*pb.LogState)
		want              bool
	}{
		{name: "start", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}, normal: 3, want: true},
		{name: "nonvoting_start", step: operator.StartNonVotingLogService{Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4}}, normal: 3, nonVoting: 1, want: true},
		{name: "add", step: operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 4, nonVoting: 1, mutate: reportThirdVoter, want: true},
		{name: "nonvoting_add", step: operator.AddNonVotingLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 3, nonVoting: 2, want: true},
		{name: "nonvoting_add_loses_quorum", step: operator.AddNonVotingLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 3, nonVoting: 2, mutate: func(s *pb.LogState) { v := s.Stores["b"]; v.Replicas = nil; s.Stores["b"] = v }},
		{name: "remove", step: operator.RemoveLogService{Target: "a", Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 1}}, normal: 2, nonVoting: 1, mutate: reportThirdVoter, want: true},
		{name: "nonvoting_remove", step: operator.RemoveNonVotingLogService{Target: "a", Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4, Epoch: 1}}, normal: 3, want: true},
		{name: "nonvoting_remove_loses_quorum", step: operator.RemoveNonVotingLogService{Target: "a", Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4, Epoch: 1}}, normal: 3, mutate: func(s *pb.LogState) { v := s.Stores["b"]; v.Replicas = nil; s.Stores["b"] = v }},
		{name: "partial_bootstrap", step: operator.BootstrapShard{UUID: "c", ShardID: 1, ReplicaID: 3, InitialMembers: map[uint64]string{1: "a", 2: "b", 3: "c"}}, normal: 3, want: true},
		{name: "empty_bootstrap", step: operator.BootstrapShard{UUID: "c", ShardID: 1, ReplicaID: 3}, normal: 3, mutate: func(s *pb.LogState) { v := s.Shards[1]; v.Replicas = nil; s.Shards[1] = v }, want: true},
		{name: "stale_epoch", step: operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 0}}, normal: 4},
		{name: "recovered_add_capacity", step: operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 3},
		{name: "stale_remove_epoch", step: operator.RemoveLogService{Target: "a", Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 0}}, normal: 2},
		{name: "obsolete_remove", step: operator.RemoveLogService{Target: "a", Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3, Epoch: 1}}, normal: 3},
		{name: "wrong_role", step: operator.StartLogService{Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4}}, normal: 3},
		{name: "wrong_replica", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 99}}, normal: 3},
		{name: "missing_target", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}, normal: 3, mutate: func(s *pb.LogState) { delete(s.Stores, "c") }},
		{name: "expired_target", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}, normal: 3, mutate: func(s *pb.LogState) { v := s.Stores["c"]; v.Tick = 0; s.Stores["c"] = v }},
		{name: "obsolete_bootstrap", step: operator.BootstrapShard{UUID: "e", ShardID: 1, ReplicaID: 5}, normal: 3},
		{name: "no_destructive_tn_retry", step: operator.RemoveTnReplica{StoreID: "tn", ShardID: 1, ReplicaID: 99}, normal: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := hakeeper.Config{}
			cfg.Fill()
			tick := uint64(10000)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, LeaderID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, NonVotingReplicas: map[uint64]string{4: "d"}}
			state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
				"a": {Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"b": {Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
				"c": {Tick: tick}, "d": {Tick: tick}, "e": {Tick: tick},
			}}
			if tt.mutate != nil {
				tt.mutate(&state)
			}
			fields := hakeeper.NewCheckerCommonFields("", cfg, nil, pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: uint64(tt.normal)}}}, pb.TaskTableUser{}, tick)
			op := operator.NewOperator("", 1, 1, tt.step)
			got := PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, uint64(tt.nonVoting), []*operator.Operator{nil, op, op}, nil)
			if tt.want {
				require.Equal(t, []*operator.Operator{op}, got)
			} else {
				require.Empty(t, got)
			}
			for _, terminal := range []operator.OpStatus{operator.SUCCESS, operator.CANCELED, operator.EXPIRED} {
				op.SetStatus(terminal)
				require.Empty(t, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, uint64(tt.nonVoting), []*operator.Operator{op}, nil))
			}
		})
	}
}

func reportThirdVoter(s *pb.LogState) {
	store := s.Stores["c"]
	store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: s.Shards[1], ReplicaID: 3}}
	s.Stores["c"] = store
}

func TestPendingAddSlotsAndStepProgress(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	fields := hakeeper.NewCheckerCommonFields("", cfg, nil, pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}}, pb.TaskTableUser{}, 1)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, LeaderID: 1, Replicas: map[uint64]string{1: "a", 2: "d"}}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
		"a": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
		"d": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
		"b": {Tick: 1}, "c": {Tick: 1},
	}}
	add := operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 3, Epoch: 1}}
	start := operator.StartLogService{Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 3}}
	op := operator.NewOperator("", 1, 1, add, start)
	extra := operator.NewOperator("", 1, 1, operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 4, Epoch: 1}})
	require.Equal(t, []*operator.Operator{op}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{op, extra}, nil))
	controller := operator.NewController()
	cmds := controller.Dispatch([]*operator.Operator{op}, state, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
	require.Equal(t, pb.AddReplica, cmds[0].ConfigChange.ChangeType)
	// Only after membership progress may the current step become StartReplica.
	shard.Replicas[3] = "b"
	shard.Epoch = 2
	state.Shards[1] = shard
	require.Equal(t, []*operator.Operator{op}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{op, extra}, nil))
	cmds = controller.Dispatch([]*operator.Operator{op}, state, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
	require.Equal(t, pb.StartReplica, cmds[0].ConfigChange.ChangeType)
	store := state.Stores["b"]
	store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}
	state.Stores["b"] = store
	require.Empty(t, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{op}, nil))
	require.Equal(t, operator.SUCCESS, op.Status())
}

func TestPendingStartsRespectStoreIncarnation(t *testing.T) {
	for _, nonVoting := range []bool{false, true} {
		cfg := hakeeper.Config{}
		cfg.Fill()
		fields := hakeeper.NewCheckerCommonFields("", cfg, nil, pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 1}}}, pb.TaskTableUser{}, 1)
		shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: map[uint64]string{1: "a"}, ReplicaStoreIncarnations: map[uint64]string{1: "old"}}
		var step operator.OpStep = operator.StartLogService{Replica: operator.Replica{UUID: "a", ShardID: 1, ReplicaID: 1}}
		n := uint64(0)
		if nonVoting {
			shard.NonVotingReplicas = shard.Replicas
			shard.Replicas = nil
			n = 1
			step = operator.StartNonVotingLogService{Replica: operator.Replica{UUID: "a", ShardID: 1, ReplicaID: 1}}
		}
		op := operator.NewOperator("", 1, 1, step)
		state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{"a": {Tick: 1, StoreIncarnation: "old"}}}
		require.Equal(t, []*operator.Operator{op}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, n, []*operator.Operator{op}, nil))
		store := state.Stores["a"]
		store.StoreIncarnation = "new"
		state.Stores["a"] = store
		require.Empty(t, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, n, []*operator.Operator{op}, nil))
		require.Equal(t, operator.STARTED, op.Status(), "an obsolete projection is not a fabricated completion")
	}
}

func TestPendingMembershipSingleChangePerShard(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	fields := hakeeper.NewCheckerCommonFields("", cfg, nil,
		pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 7}}},
		pb.TaskTableUser{}, 10000)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
		"a": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
		"b": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
		"c": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}},
		"d": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 4}}},
		"e": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 5}}},
		"f": {Tick: 10000}, "g": {Tick: 10000},
	}}
	addOnA := operator.NewOperator("", 1, 1, operator.AddLogService{
		Target: "a", Replica: operator.Replica{UUID: "f", ShardID: 1, ReplicaID: 6, Epoch: 1},
	})
	addOnB := operator.NewOperator("", 1, 1, operator.AddLogService{
		Target: "b", Replica: operator.Replica{UUID: "f", ShardID: 1, ReplicaID: 6, Epoch: 1},
	})
	// One old operator cannot be retried through a live store that stopped
	// reporting its configured voter identity.
	store := state.Stores["a"]
	store.Replicas = nil
	state.Stores["a"] = store
	require.Empty(t, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{addOnA}, nil))

	// Replacing it with a current reporter restores retry, and a sibling change
	// for the same shard and epoch is withheld until the first is observed.
	otherAdd := operator.NewOperator("", 1, 1, operator.AddLogService{
		Target: "b", Replica: operator.Replica{UUID: "g", ShardID: 1, ReplicaID: 7, Epoch: 1},
	})
	require.Equal(t, []*operator.Operator{addOnB}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{addOnB}, nil))
	require.Equal(t, []*operator.Operator{otherAdd}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{otherAdd}, nil))
	require.Equal(t, []*operator.Operator{addOnB}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{addOnB, otherAdd}, nil))
	require.Equal(t, []*operator.Operator{addOnB}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{addOnA, addOnB}, nil),
		"an ineligible older owner must not consume the retry slot")
	require.Empty(t, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, []*operator.Operator{addOnB}, []*operator.Operator{otherAdd}),
		"fresh and retried changes share the same slot")
	learner := operator.NewOperator("", 1, 1, operator.AddNonVotingLogService{
		Target: "b", Replica: operator.Replica{UUID: "g", ShardID: 1, ReplicaID: 7, Epoch: 1},
	})
	require.Equal(t, []*operator.Operator{addOnB}, PendingOperators(fields, state, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 2, []*operator.Operator{addOnB, learner}, nil),
		"voters and non-voters share the same Raft configuration")
}
