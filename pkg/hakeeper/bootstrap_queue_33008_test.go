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
	"bytes"
	"fmt"
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func newPendingBootstrapState(t *testing.T, commandDelivery bool) (*stateMachine, map[uint64]string) {
	t.Helper()
	s := NewStateMachine(0, 1).(*stateMachine)
	installMembershipPolicy(t, s, 100)
	members := map[uint64]string{11: "log-a", 12: "log-b", 13: "log-c"}
	s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 77, NumberOfReplicas: 3}}
	for _, storeID := range members {
		s.state.LogState.Stores[storeID] = pb.LogStoreInfo{StoreIncarnation: "inc-" + storeID}
	}
	s.state.CommandDeliveryEnabled = commandDelivery
	return s, members
}

func pendingBootstrapCommand(storeID string, replicaID uint64, members map[uint64]string) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID:        storeID,
		ServiceType: pb.LogService,
		BootstrapShard: &pb.BootstrapShard{
			ShardID:        77,
			ReplicaID:      replicaID,
			InitialMembers: members,
		},
	}
}

func TestPendingBootstrapSurvivesScheduleReplacement(t *testing.T) {
	for _, mode := range []struct {
		name      string
		preparing bool
		enabled   bool
	}{
		{name: "legacy"},
		{name: "preparing", preparing: true},
		{name: "enabled", enabled: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			s, members := newPendingBootstrapState(t, false)
			s.state.CommandDeliveryPreparing = mode.preparing
			s.state.CommandDeliveryEnabled = mode.enabled
			bootstrapB := pendingBootstrapCommand("log-b", 12, members)
			bootstrapC := pendingBootstrapCommand("log-c", 13, members)
			bootstrapIDB := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 0}
			bootstrapIDC := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 1}
			s.state.ScheduleCommands["log-b"] = pb.CommandBatch{
				BatchID: 7, Commands: []pb.ScheduleCommand{bootstrapB},
			}
			s.state.ScheduleCommands["log-c"] = pb.CommandBatch{
				BatchID: 7, Commands: []pb.ScheduleCommand{bootstrapC},
			}
			if mode.preparing || mode.enabled {
				s.state.ScheduleCommands["log-b"] = pb.CommandBatch{
					BatchID: 7, Commands: []pb.ScheduleCommand{bootstrapB},
					CommandIDs: []pb.ScheduleCommandID{bootstrapIDB},
				}
				s.state.ScheduleCommands["log-c"] = pb.CommandBatch{
					BatchID: 7, Commands: []pb.ScheduleCommand{bootstrapC},
					CommandIDs: []pb.ScheduleCommandID{bootstrapIDC},
				}
			}

			tnTask := pb.ScheduleCommand{
				UUID: "tn-a", ServiceType: pb.TNService,
				CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"},
			}
			unhappyQueue(t, s, tnTask)
			require.Equal(t, []pb.ScheduleCommand{bootstrapB}, s.state.ScheduleCommands["log-b"].Commands)
			require.Equal(t, []pb.ScheduleCommand{bootstrapC}, s.state.ScheduleCommands["log-c"].Commands)

			startB := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica:    pb.Replica{UUID: "log-b", ShardID: 77, ReplicaID: 12},
			}}
			startC := pb.ScheduleCommand{UUID: "log-c", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica:    pb.Replica{UUID: "log-c", ShardID: 77, ReplicaID: 13},
			}}
			otherShard := pb.ScheduleCommand{UUID: "log-d", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica:    pb.Replica{UUID: "log-d", ShardID: 88, ReplicaID: 14},
			}}
			unhappyQueue(t, s, startB, startC, otherShard)
			require.Equal(t, []pb.ScheduleCommand{bootstrapB}, s.state.ScheduleCommands["log-b"].Commands)
			require.Equal(t, []pb.ScheduleCommand{bootstrapC}, s.state.ScheduleCommands["log-c"].Commands)
			require.Equal(t, []pb.ScheduleCommand{otherShard}, s.state.ScheduleCommands["log-d"].Commands)
			if mode.preparing || mode.enabled {
				require.Equal(t, []pb.ScheduleCommandID{bootstrapIDB}, s.state.ScheduleCommands["log-b"].CommandIDs)
				require.Equal(t, []pb.ScheduleCommandID{bootstrapIDC}, s.state.ScheduleCommands["log-c"].CommandIDs)
			}
		})
	}
}

func TestPendingBootstrapKeepsLegacyPrePolicyReplacement(t *testing.T) {
	s := NewStateMachine(0, 1).(*stateMachine)
	members := map[uint64]string{11: "log-a", 12: "log-b", 13: "log-c"}
	s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 77, NumberOfReplicas: 3}}
	s.state.ScheduleCommands["log-b"] = pb.CommandBatch{
		Commands: []pb.ScheduleCommand{pendingBootstrapCommand("log-b", 12, members)},
	}

	tnTask := pb.ScheduleCommand{
		UUID: "tn-a", ServiceType: pb.TNService,
		CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"},
	}
	unhappyQueue(t, s, tnTask)
	require.NotContains(t, s.state.ScheduleCommands, "log-b")
	require.Equal(t, []pb.ScheduleCommand{tnTask}, s.state.ScheduleCommands["tn-a"].Commands)
}

func TestPendingBootstrapWaitsForStopAndRejectsNewStoreIncarnation(t *testing.T) {
	t.Run("same-batch stop", func(t *testing.T) {
		s, members := newPendingBootstrapState(t, false)
		bootstrap := pendingBootstrapCommand("log-b", 12, members)
		stop := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StopReplica,
			Replica:    pb.Replica{UUID: "log-b", ShardID: 77, ReplicaID: 12},
		}}
		s.state.ScheduleCommands["log-b"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{bootstrap}}
		unhappyQueue(t, s, stop)

		hb := pb.LogStoreHeartbeat{UUID: "log-b", StoreIncarnation: "inc-log-b"}
		got := membershipHeartbeat(t, s, hb, true)
		require.Equal(t, []pb.ScheduleCommand{stop}, got.Commands)
		require.Equal(t, []pb.ScheduleCommand{bootstrap}, s.state.ScheduleCommands["log-b"].Commands)

		got = membershipHeartbeat(t, s, hb, true)
		require.Equal(t, []pb.ScheduleCommand{bootstrap}, got.Commands)
	})

	t.Run("new store incarnation", func(t *testing.T) {
		s, members := newPendingBootstrapState(t, false)
		bootstrap := pendingBootstrapCommand("log-b", 12, members)
		s.state.ScheduleCommands["log-b"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{bootstrap}}

		got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
			UUID: "log-b", StoreIncarnation: "inc-replaced",
		}, true)
		require.Empty(t, got.Commands)
		require.NotContains(t, s.state.ScheduleCommands, "log-b")
	})
}

func TestPendingBootstrapWaitsForUnambiguousCohort(t *testing.T) {
	s, firstMembers := newPendingBootstrapState(t, false)
	secondMembers := map[uint64]string{11: "log-a", 13: "log-c", 14: "log-d"}
	s.state.LogState.Stores["log-d"] = pb.LogStoreInfo{StoreIncarnation: "inc-log-d"}
	first := pendingBootstrapCommand("log-b", 12, firstMembers)
	second := pendingBootstrapCommand("log-c", 13, secondMembers)
	s.state.ScheduleCommands["log-b"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{first}}
	s.state.ScheduleCommands["log-c"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{second}}

	got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
		UUID: "log-b", StoreIncarnation: "inc-log-b",
	}, true)
	require.Empty(t, got.Commands)
	require.Equal(t, []pb.ScheduleCommand{first}, s.state.ScheduleCommands["log-b"].Commands)

	s.state.LogState.Shards[77] = pb.LogShardInfo{ShardID: 77, Epoch: 1, Replicas: firstMembers}
	got = membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
		UUID: "log-b", StoreIncarnation: "inc-log-b",
	}, true)
	require.Equal(t, []pb.ScheduleCommand{first}, got.Commands)

	got = membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
		UUID: "log-c", StoreIncarnation: "inc-log-c",
	}, true)
	require.Empty(t, got.Commands, "the unobserved conflicting cohort is obsolete once a full map is authoritative")
	require.NotContains(t, s.state.ScheduleCommands, "log-c")
}

func TestPendingBootstrapSurvivesUndeliverableMembershipProposal(t *testing.T) {
	s, members := newPendingBootstrapState(t, false)
	shard := pb.LogShardInfo{ShardID: 77, Epoch: 1, Replicas: members}
	s.state.LogState.Shards[77] = shard
	s.state.NonVotingReplicaNum = 1
	for replicaID, storeID := range members {
		store := s.state.LogState.Stores[storeID]
		store.Tick = s.state.Tick
		if storeID == "log-a" || storeID == "log-b" {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: replicaID}}
		}
		s.state.LogState.Stores[storeID] = store
	}
	s.state.LogState.Stores["log-d"] = pb.LogStoreInfo{Tick: s.state.Tick, StoreIncarnation: "inc-log-d"}
	bootstrap := pendingBootstrapCommand("log-c", 13, members)
	s.state.ScheduleCommands["log-c"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{bootstrap}}

	addObserver := pb.ScheduleCommand{UUID: "log-a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.AddNonVotingReplica,
		Replica:    pb.Replica{UUID: "log-d", ShardID: 77, ReplicaID: 14, Epoch: 1},
	}}
	unhappyQueue(t, s, addObserver)
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, s.state.ScheduleCommands["log-c"].Commands)
	require.Equal(t, []pb.ScheduleCommand{addObserver}, s.state.ScheduleCommands["log-a"].Commands)

	// One voter stops reporting after the proposal is queued. The guarded
	// heartbeat rejects the now-unexecutable change without erasing C's
	// original, still-authoritative bootstrap.
	got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "log-a", StoreIncarnation: "inc-log-a"}, true)
	require.Empty(t, got.Commands)
	require.NotContains(t, s.state.ScheduleCommands, "log-a")
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, s.state.ScheduleCommands["log-c"].Commands)

	got = membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "log-c", StoreIncarnation: "inc-log-c"}, true)
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, got.Commands)
}

func TestPendingBootstrapOnlySuppressesStartForItsTarget(t *testing.T) {
	s, members := newPendingBootstrapState(t, false)
	shard := pb.LogShardInfo{ShardID: 77, Epoch: 1, Replicas: members}
	s.state.LogState.Shards[77] = shard
	bootstrapC := pendingBootstrapCommand("log-c", 13, members)
	s.state.ScheduleCommands["log-c"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{bootstrapC}}

	startB := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.StartReplica,
		Replica:    pb.Replica{UUID: "log-b", ShardID: 77, ReplicaID: 12, Epoch: 1},
	}}
	unhappyQueue(t, s, startB)
	require.Equal(t, []pb.ScheduleCommand{startB}, s.state.ScheduleCommands["log-b"].Commands)
	require.Equal(t, []pb.ScheduleCommand{bootstrapC}, s.state.ScheduleCommands["log-c"].Commands)

	got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
		UUID: "log-b", StoreIncarnation: "inc-log-b",
		Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 12}},
	}, true)
	require.Equal(t, []pb.ScheduleCommand{startB}, got.Commands)
}

func TestPendingBootstrapSurvivesHistoricalShutdownAndSnapshot(t *testing.T) {
	s, members := newPendingBootstrapState(t, true)
	bootstrap := pendingBootstrapCommand("log-b", 12, members)
	bootstrapID := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 0}
	s.state.ScheduleCommands["log-b"] = pb.CommandBatch{
		BatchID: 7, Commands: []pb.ScheduleCommand{bootstrap},
		CommandIDs: []pb.ScheduleCommandID{bootstrapID},
	}
	shutdown := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService,
		ShutdownStore: &pb.ShutdownStore{StoreID: "log-b"}}
	unhappyQueue(t, s, shutdown)
	require.Equal(t, []pb.ScheduleCommand{bootstrap, shutdown}, s.state.ScheduleCommands["log-b"].Commands)
	require.Equal(t, []pb.ScheduleCommandID{bootstrapID}, s.state.ScheduleCommands["log-b"].CommandIDs[:1])

	var snapshot bytes.Buffer
	require.NoError(t, s.SaveSnapshot(&snapshot, nil, nil))
	restored := NewStateMachine(0, 44).(*stateMachine)
	require.NoError(t, restored.RecoverFromSnapshot(&snapshot, nil, nil))
	got := membershipHeartbeat(t, restored, pb.LogStoreHeartbeat{
		UUID: "log-b", StoreIncarnation: "inc-log-b", CommandDeliverySupported: true,
	}, true)
	require.Equal(t, []pb.ScheduleCommand{bootstrap, shutdown}, got.Commands)
	require.Equal(t, bootstrapID, got.CommandIDs[0])
}

func TestPendingBootstrapStopOrderingDoesNotDependOnBatchOrder(t *testing.T) {
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprintf("reversed=%t", reversed), func(t *testing.T) {
			s, members := newPendingBootstrapState(t, false)
			bootstrap := pendingBootstrapCommand("log-b", 12, members)
			stop := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StopReplica,
				Replica:    pb.Replica{UUID: "log-b", ShardID: 77, ReplicaID: 12},
			}}
			shutdown := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService,
				ShutdownStore: &pb.ShutdownStore{StoreID: "log-b"}}
			commands := []pb.ScheduleCommand{stop, shutdown}
			if reversed {
				commands[0], commands[1] = commands[1], commands[0]
			}
			s.state.ScheduleCommands["log-b"] = pb.CommandBatch{Commands: []pb.ScheduleCommand{bootstrap}}
			unhappyQueue(t, s, commands...)
			got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "log-b", StoreIncarnation: "inc-log-b"}, true)
			require.NotContains(t, got.Commands, bootstrap)
			require.Contains(t, got.Commands, stop)
			require.Contains(t, got.Commands, shutdown)
			require.Equal(t, []pb.ScheduleCommand{bootstrap}, s.state.ScheduleCommands["log-b"].Commands)
		})
	}
}

func TestPendingBootstrapSupersedesAlreadyQueuedStart(t *testing.T) {
	s, members := newPendingBootstrapState(t, true)
	shard := pb.LogShardInfo{ShardID: 77, Epoch: 1, Replicas: members}
	s.state.LogState.Shards[77] = shard
	s.state.NonVotingReplicaNum = 1
	for replicaID, storeID := range members {
		store := s.state.LogState.Stores[storeID]
		store.Tick = s.state.Tick
		if storeID == "log-b" {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: replicaID}}
		}
		s.state.LogState.Stores[storeID] = store
	}
	s.state.LogState.Stores["log-d"] = pb.LogStoreInfo{Tick: s.state.Tick, StoreIncarnation: "inc-log-d"}
	start := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.StartReplica,
		Replica:    pb.Replica{UUID: "log-b", ShardID: 77, ReplicaID: 12, Epoch: 1},
	}}
	bootstrap := pendingBootstrapCommand("log-b", 12, members)
	addObserver := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.AddNonVotingReplica,
		Replica:    pb.Replica{UUID: "log-d", ShardID: 77, ReplicaID: 14, Epoch: 1},
	}}
	startID := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 0}
	bootstrapID := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 1}
	s.state.ScheduleCommands["log-b"] = pb.CommandBatch{
		BatchID:    7,
		Commands:   []pb.ScheduleCommand{start, bootstrap, addObserver},
		CommandIDs: []pb.ScheduleCommandID{startID, bootstrapID, {OriginBatchID: 7, CommandIndex: 2}},
	}

	hb := pb.LogStoreHeartbeat{
		UUID: "log-b", StoreIncarnation: "inc-log-b", CommandDeliverySupported: true,
		Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 12}},
	}
	got := membershipHeartbeat(t, s, hb, true)
	require.Empty(t, got.Commands, "the quorum-losing observer add is dropped and its companion start is suppressed")
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, s.state.ScheduleCommands["log-b"].Commands)
	require.Equal(t, []pb.ScheduleCommandID{bootstrapID}, s.state.ScheduleCommands["log-b"].CommandIDs)

	got = membershipHeartbeat(t, s, hb, true)
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, got.Commands)
	require.Equal(t, []pb.ScheduleCommandID{bootstrapID}, got.CommandIDs)
	require.NotContains(t, got.Commands, start)
}

func TestQueuedStartCannotPreemptPendingBootstrap(t *testing.T) {
	s, members := newPendingBootstrapState(t, true)
	shard := pb.LogShardInfo{ShardID: 77, Epoch: 1, Replicas: members}
	s.state.LogState.Shards[77] = shard
	s.state.NonVotingReplicaNum = 1
	for replicaID, storeID := range members {
		store := s.state.LogState.Stores[storeID]
		store.Tick = s.state.Tick
		if storeID == "log-b" {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: replicaID}}
		}
		s.state.LogState.Stores[storeID] = store
	}
	s.state.LogState.Stores["log-d"] = pb.LogStoreInfo{Tick: s.state.Tick, StoreIncarnation: "inc-log-d"}
	start := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.StartReplica,
		Replica:    pb.Replica{UUID: "log-b", ShardID: 77, ReplicaID: 12, Epoch: 1},
	}}
	bootstrap := pendingBootstrapCommand("log-b", 12, members)
	addObserver := pb.ScheduleCommand{UUID: "log-b", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.AddNonVotingReplica,
		Replica:    pb.Replica{UUID: "log-d", ShardID: 77, ReplicaID: 14, Epoch: 1},
	}}
	startID := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 0}
	bootstrapID := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 1}
	observerID := pb.ScheduleCommandID{OriginBatchID: 7, CommandIndex: 2}
	s.state.ScheduleCommands["log-b"] = pb.CommandBatch{
		BatchID:    7,
		Commands:   []pb.ScheduleCommand{start, bootstrap, addObserver},
		CommandIDs: []pb.ScheduleCommandID{startID, bootstrapID, observerID},
	}

	hb := pb.LogStoreHeartbeat{
		UUID: "log-b", StoreIncarnation: "inc-log-b", CommandDeliverySupported: true,
		Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 12}},
	}
	got := membershipHeartbeat(t, s, hb, true)
	require.Empty(t, got.Commands, "the quorum-losing observer add is dropped and its companion start is suppressed")
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, s.state.ScheduleCommands["log-b"].Commands)
	require.Equal(t, []pb.ScheduleCommandID{bootstrapID}, s.state.ScheduleCommands["log-b"].CommandIDs)

	got = membershipHeartbeat(t, s, hb, true)
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, got.Commands)
	require.Equal(t, []pb.ScheduleCommandID{bootstrapID}, got.CommandIDs)
	require.NotContains(t, got.Commands, start)
}
