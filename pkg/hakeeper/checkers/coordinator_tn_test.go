// Copyright 2021 - 2026 Matrix Origin
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

package checkers

import (
	"fmt"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	tncheck "github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/tnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func tnStartScenario(t *testing.T) (*Coordinator, pb.CheckerState, *util.TestIDAllocator) {
	t.Helper()
	// The existing coordinator fixture owns this runtime. Restore its check state.
	rt := runtime.ServiceRuntime("")
	previous, exists := rt.GetGlobalVariables("log_service_init_state")
	tncheck.InitCheckState("")
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables("log_service_init_state", previous)
		} else {
			tncheck.InitCheckState("")
		}
	})
	state := pb.CheckerState{
		Tick:        1,
		ClusterInfo: pb.ClusterInfo{TNShards: []metadata.TNShardRecord{{ShardID: 2, LogShardID: 1}}},
		TNState:     pb.TNState{Stores: map[string]pb.TNStoreInfo{"tn-a": {Tick: 1, TaskServiceCreated: true}}},
	}
	return NewCoordinator("", hakeeper.Config{TNStoreTimeout: time.Minute}), state, util.NewTestIDAllocator(100)
}

func TestTNStartRescheduledAfterLegacyCommandLoss(t *testing.T) {
	for _, overwrite := range []bool{false, true} {
		name := "lost_response"
		if overwrite {
			name = "overwritten_batch"
		}
		t.Run(name, func(t *testing.T) {
			c, state, alloc := tnStartScenario(t)
			rsm := hakeeper.NewStateMachine(0, 1)
			var index uint64
			apply := func(cmd []byte) sm.Result {
				t.Helper()
				index++
				result, err := rsm.Update(sm.Entry{Index: index, Cmd: cmd})
				require.NoError(t, err)
				return result
			}
			heartbeat := func() pb.CommandBatch {
				t.Helper()
				hb, err := (&pb.TNStoreHeartbeat{UUID: "tn-a"}).Marshal()
				require.NoError(t, err)
				var b pb.CommandBatch
				require.NoError(t, b.Unmarshal(apply(hakeeper.GetTNStoreHeartbeatCmd(hb)).Data))
				return b
			}
			first := c.Check(alloc, state, false)
			require.Len(t, first, 1)
			require.False(t, first[0].Bootstrapping)
			op := c.OperatorController.GetOperators(2)[0]
			apply(hakeeper.GetUpdateCommandsCmd(1, first))
			if overwrite {
				apply(hakeeper.GetUpdateCommandsCmd(1, []pb.ScheduleCommand{{UUID: "cn-other", ServiceType: pb.CNService}}))
			} else {
				require.Equal(t, first, heartbeat().Commands) // Discard this committed response.
			}
			require.Empty(t, heartbeat().Commands, "legacy RSM consumption remains historical")
			for i := 0; i < 3; i++ {
				state.Tick++
				retried := c.Check(alloc, state, false)
				require.Equal(t, first, retried)
				require.Equal(t, []*operator.Operator{op}, c.OperatorController.GetOperators(2))
				apply(hakeeper.GetUpdateCommandsCmd(1, retried))
				require.Equal(t, first, heartbeat().Commands)
			}
			nextID, ok := alloc.Next()
			require.True(t, ok)
			require.Equal(t, uint64(102), nextID, "retry did not allocate another ID")
			info := state.TNState.Stores["tn-a"]
			info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: first[0].ConfigChange.Replica.ReplicaID}}
			state.TNState.Stores["tn-a"] = info
			require.Empty(t, c.Check(alloc, state, false))
			require.Empty(t, c.OperatorController.GetOperators(2))
		})
	}
}

func TestTNStartReconcilesReportsAndTargets(t *testing.T) {
	for _, name := range []string{"same_id", "different_id", "other_live_target", "wrong_shard", "expired_report", "missing_target", "expired_target", "no_live_target", "canceled", "canceled_siblings", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			c, state, alloc := tnStartScenario(t)
			first := c.Check(alloc, state, false)
			require.Len(t, first, 1)
			op := c.OperatorController.GetOperators(2)[0]
			id := first[0].ConfigChange.Replica.ReplicaID
			state.Tick = 2
			info := state.TNState.Stores["tn-a"]
			switch name {
			case "same_id":
				info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: id}}
			case "different_id":
				info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: id + 10}}
			case "other_live_target":
				state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: 2, TaskServiceCreated: true, Shards: []pb.TNShardInfo{{ShardID: 2, ReplicaID: id + 10}}}
			case "wrong_shard":
				info.Shards = []pb.TNShardInfo{{ShardID: 99, ReplicaID: id}}
			case "expired_report":
				state.Tick = 10000
				info.Tick = state.Tick
				state.TNState.Stores["tn-old"] = pb.TNStoreInfo{Shards: []pb.TNShardInfo{{ShardID: 2, ReplicaID: id + 10}}}
			case "missing_target":
				delete(state.TNState.Stores, "tn-a")
				state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: 2, TaskServiceCreated: true}
			case "expired_target":
				state.Tick = 10000
				state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: state.Tick, TaskServiceCreated: true}
			case "no_live_target":
				state.Tick = 10000
			case "canceled":
				op.Cancel()
			case "canceled_siblings":
				siblings := []*operator.Operator{op}
				op.Cancel()
				for i := uint64(1); i <= 2; i++ {
					canceled := operator.NewOperator("tnservice", 2, operator.NoopEpoch, operator.AddTnReplica{StoreID: "tn-a", ShardID: 2, ReplicaID: id + i, LogShardID: 1})
					c.OperatorController.Dispatch([]*operator.Operator{canceled}, state.LogState, state.TNState, state.CNState, state.ProxyState)
					canceled.Cancel()
					siblings = append(siblings, canceled)
				}
				require.Equal(t, siblings, c.OperatorController.GetOperators(2), "all three adjacent canceled entries must be present before reconciliation")
			case "duplicate":
				duplicate := operator.NewOperator("tnservice", 2, operator.NoopEpoch, operator.AddTnReplica{StoreID: "tn-a", ShardID: 2, ReplicaID: id + 10, LogShardID: 1})
				c.OperatorController.Dispatch([]*operator.Operator{duplicate}, state.LogState, state.TNState, state.CNState, state.ProxyState)
			}
			if name != "missing_target" {
				state.TNState.Stores["tn-a"] = info
			}
			commands := c.Check(alloc, state, false)
			switch name {
			case "same_id", "different_id", "other_live_target", "no_live_target", "canceled", "canceled_siblings":
				require.Empty(t, commands)
				require.Empty(t, c.OperatorController.GetOperators(2))
				if name == "same_id" {
					require.Equal(t, operator.SUCCESS, op.Status())
				} else {
					require.Equal(t, operator.CANCELED, op.Status())
				}
			case "missing_target":
				require.Empty(t, commands)
				require.Equal(t, operator.CANCELED, op.Status())
				require.Empty(t, c.OperatorController.GetOperators(2))
				// Reuse the established eligibility policy after abandoning this target.
				state.Tick = 10000
				info := state.TNState.Stores["tn-b"]
				info.Tick = state.Tick
				state.TNState.Stores["tn-b"] = info
				next := c.Check(alloc, state, false)
				require.Len(t, next, 1)
				require.Equal(t, "tn-b", next[0].UUID)
			case "expired_target":
				require.Len(t, commands, 1)
				require.Equal(t, "tn-b", commands[0].UUID)
				require.Equal(t, operator.CANCELED, op.Status())
			default:
				require.Equal(t, first, commands)
				require.Equal(t, []*operator.Operator{op}, c.OperatorController.GetOperators(2))
			}
		})
	}
}

func TestTNStartReservationsAndPartialReports(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	first := c.Check(alloc, state, false)
	require.Len(t, first, 1)
	state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: state.Tick, TaskServiceCreated: true}
	state.ClusterInfo.TNShards = append(state.ClusterInfo.TNShards, metadata.TNShardRecord{ShardID: 3, LogShardID: 1})
	// The existing initial-shard wait remains in force for newly discovered work.
	require.Len(t, c.Check(alloc, state, false), 1)
	state.Tick = 10000
	for id, info := range state.TNState.Stores {
		info.Tick = state.Tick
		state.TNState.Stores[id] = info
	}
	both := c.Check(alloc, state, false)
	require.Len(t, both, 2)
	require.Equal(t, "tn-a", both[0].UUID)
	require.Equal(t, "tn-b", both[1].UUID)
	info := state.TNState.Stores["tn-a"]
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: first[0].ConfigChange.Replica.ReplicaID}}
	state.TNState.Stores["tn-a"] = info
	remaining := c.Check(alloc, state, false)
	require.Equal(t, both[1:], remaining)
	require.Empty(t, c.OperatorController.GetOperators(2))
	require.Len(t, c.OperatorController.GetOperators(3), 1)
}

func TestTNStartRebuildAndRecovery(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	first := c.Check(alloc, state, false)
	require.Len(t, first, 1)
	old := c.OperatorController.GetOperators(2)[0]
	// A new coordinator rebuilds missing intent from current cluster records.
	c = NewCoordinator("", hakeeper.Config{TNStoreTimeout: time.Minute})
	state.Tick = 10000
	info := state.TNState.Stores["tn-a"]
	info.Tick = state.Tick
	state.TNState.Stores["tn-a"] = info
	restarted := c.Check(alloc, state, false)
	require.Len(t, restarted, 1)
	require.NotEqual(t, first[0].ConfigChange.Replica.ReplicaID, restarted[0].ConfigChange.Replica.ReplicaID)
	// An old leader's retained attempt must yield to a current different-ID report.
	stale := NewCoordinator("", hakeeper.Config{TNStoreTimeout: time.Minute})
	stale.OperatorController.Dispatch([]*operator.Operator{old}, state.LogState, state.TNState, state.CNState, state.ProxyState)
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: restarted[0].ConfigChange.Replica.ReplicaID}}
	state.TNState.Stores["tn-a"] = info
	require.Empty(t, stale.Check(alloc, state, false))
	require.Empty(t, stale.OperatorController.GetOperators(2))
	// A temporary shortage of Log replica reports leaves the TN start in flight.
	info.Shards = nil
	state.TNState.Stores["tn-a"] = info
	pending := c.OperatorController.GetOperators(2)[0]
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
	state.LogState = pb.LogState{Stores: map[string]pb.LogStoreInfo{}}
	for i, id := range []string{"log-a", "log-b"} {
		state.LogState.Stores[id] = pb.LogStoreInfo{Replicas: []pb.LogReplicaInfo{{LogShardInfo: pb.LogShardInfo{ShardID: 1}, ReplicaID: uint64(i + 1)}}}
	}
	for _, cmd := range c.Check(alloc, state, false) {
		require.Nil(t, cmd.ShutdownStore)
	}
	require.Same(t, pending, c.OperatorController.GetOperators(2)[0])
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: restarted[0].ConfigChange.Replica.ReplicaID}}
	state.TNState.Stores["tn-a"] = info
	c.Check(alloc, state, false)
	require.Empty(t, c.OperatorController.GetOperators(2))
}

func TestTNRetryPreservesPendingLogStart(t *testing.T) {
	for _, tt := range []struct {
		name              string
		pending, consumed bool
	}{
		{name: "tn_already_running"}, {name: "tn_start_pending", pending: true}, {name: "log_response_consumed", pending: true, consumed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			coordinator, state, alloc := tnStartScenario(t)
			shard := pb.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1, LeaderID: 1, Term: 1}
			state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
			state.LogState = pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
				"a": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"b": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
				"c": {Tick: 1, TaskServiceCreated: true},
			}}
			if !tt.pending {
				info := state.TNState.Stores["tn-a"]
				info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: 99}}
				state.TNState.Stores["tn-a"] = info
			}
			rsm := hakeeper.NewStateMachine(0, 1)
			var index uint64
			apply := func(cmd []byte) sm.Result {
				index++
				r, err := rsm.Update(sm.Entry{Index: index, Cmd: cmd})
				require.NoError(t, err)
				return r
			}
			// The delivery RSM must observe the same membership as the checker.
			for _, id := range []string{"a", "b"} {
				report, err := (&pb.LogStoreHeartbeat{UUID: id, Replicas: state.LogState.Stores[id].Replicas}).Marshal()
				require.NoError(t, err)
				apply(hakeeper.GetLogStoreHeartbeatCmd(report))
			}
			first := coordinator.Check(alloc, state, false)
			var logStart *pb.ScheduleCommand
			for i := range first {
				cmd := &first[i]
				if cmd.UUID == "c" && cmd.ServiceType == pb.LogService && cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.StartReplica {
					logStart = cmd
				}
			}
			require.NotNil(t, logStart, "fixture must schedule the actual missing log replica")
			t.Logf("first batch: %+v", first)
			apply(hakeeper.GetUpdateCommandsCmd(1, first))
			hb, err := (&pb.LogStoreHeartbeat{UUID: "c"}).Marshal()
			require.NoError(t, err)
			if tt.consumed {
				var original pb.CommandBatch
				require.NoError(t, original.Unmarshal(apply(hakeeper.GetLogStoreHeartbeatCmd(hb)).Data))
				require.Contains(t, original.Commands, *logStart)
			}
			state.Tick++
			second := coordinator.Check(alloc, state, false)
			t.Logf("second batch: %+v", second)
			if len(second) > 0 {
				apply(hakeeper.GetUpdateCommandsCmd(1, second))
			}
			var received pb.CommandBatch
			require.NoError(t, received.Unmarshal(apply(hakeeper.GetLogStoreHeartbeatCmd(hb)).Data))
			t.Logf("log heartbeat commands: %+v", received.Commands)
			require.Contains(t, received.Commands, *logStart, "a TN-only retry must not erase an unrelated undelivered log start")
			// A real report ends the retained Log intent, even while TN still retries.
			store := state.LogState.Stores["c"]
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}
			state.LogState.Stores["c"] = store
			next := coordinator.Check(alloc, state, false)
			for _, cmd := range next {
				require.NotEqual(t, pb.LogService, cmd.ServiceType)
			}
			require.Empty(t, coordinator.OperatorController.GetOperators(1))
		})
	}
}

func TestTNRetryDoesNotResurrectObsoleteRemoval(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	state.ClusterInfo.TNShards = append(state.ClusterInfo.TNShards, metadata.TNShardRecord{ShardID: 4, LogShardID: 1})
	info := state.TNState.Stores["tn-a"]
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: 99}}
	state.TNState.Stores["tn-a"] = info
	old := operator.NewOperator("", 2, operator.NoopEpoch, operator.RemoveTnReplica{StoreID: "tn-a", ShardID: 2, ReplicaID: 99, LogShardID: 1})
	// The removal was scheduled while another replica existed. That peer has
	// since disappeared; tn-a is now the sole working replica of shard 2.
	c.OperatorController.Dispatch([]*operator.Operator{old}, state.LogState, state.TNState, state.CNState, state.ProxyState)
	first := c.Check(alloc, state, false)
	require.Len(t, first, 1)
	require.Equal(t, uint64(4), first[0].ConfigChange.Replica.ShardID)
	next := c.Check(alloc, state, false)
	require.Equal(t, first, next)
	require.Equal(t, operator.STARTED, old.Status())
}

func TestCoordinatorRepairsAfterMissingReplicaReport(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	state.Tick = 10000
	state.TNState.Stores["tn-a"] = pb.TNStoreInfo{Tick: state.Tick, TaskServiceCreated: true}
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{
		{ShardID: 0, NumberOfReplicas: 3}, {ShardID: 1, NumberOfReplicas: 3},
	}
	members := map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c"}
	data := pb.LogShardInfo{ShardID: 1, Replicas: members, LeaderID: 1, Epoch: 1}
	control := pb.LogShardInfo{Replicas: members, LeaderID: 1, Epoch: 1}
	state.LogState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{0: control, 1: data},
		Stores: map[string]pb.LogStoreInfo{
			"log-a": {Tick: state.Tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: control, ReplicaID: 1}, {LogShardInfo: data, ReplicaID: 1}}},
			"log-b": {Replicas: []pb.LogReplicaInfo{{LogShardInfo: control, ReplicaID: 2}, {LogShardInfo: data, ReplicaID: 2}}},
			"log-c": {Tick: state.Tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: control, ReplicaID: 3}}},
			"log-d": {Tick: state.Tick},
		},
	}
	recoveryCommands := func() []pb.ScheduleCommand {
		t.Helper()
		cmds := c.Check(alloc, state, false)
		for _, cmd := range cmds {
			require.Nil(t, cmd.ShutdownStore)
		}
		return cmds
	}
	hasChange := func(cmds []pb.ScheduleCommand, change pb.ConfigChangeType, shard uint64, target string) bool {
		for _, cmd := range cmds {
			if cc := cmd.ConfigChange; cmd.ServiceType == pb.LogService && cc != nil &&
				cc.ChangeType == change && cc.Replica.ShardID == shard && cc.Replica.UUID == target {
				return true
			}
		}
		return false
	}
	cmds := recoveryCommands()
	require.True(t, hasChange(cmds, pb.StartReplica, 1, "log-c"), "start the missing configured replica")
	require.False(t, hasChange(cmds, pb.RemoveReplica, 0, "log-b"), "do not risk the HAKeeper commit quorum")
	require.False(t, hasChange(cmds, pb.RemoveReplica, 1, "log-b"))
	report := state.LogState.Stores["log-c"]
	report.Replicas = append(report.Replicas, pb.LogReplicaInfo{LogShardInfo: data, ReplicaID: 3, IsNonVoting: true})
	state.LogState.Stores["log-c"] = report
	require.False(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "a non-voting report is insufficient")

	report.Replicas[1].IsNonVoting = false
	report.Replicas[1].ReplicaID = 4
	require.False(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "a different replica ID is insufficient")
	report.Replicas[1].ReplicaID = 3
	require.True(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "resume on the same coordinator when the voter reports")
	data.Replicas = map[uint64]string{1: "log-a", 3: "log-c"}
	data.Epoch++
	report.Replicas[1].LogShardInfo = data
	state.LogState.Shards[1] = data
	require.True(t, hasChange(recoveryCommands(), pb.AddReplica, 1, "log-d"), "replace the expired voter after removal")
}

func TestCoordinatorSerializesRepairThroughReportingVoters(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	state.Tick = 10000
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 5}}
	members := map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c", 4: "log-d", 5: "log-e"}
	state.LogState = pb.LogState{
		Shards: make(map[uint64]pb.LogShardInfo),
		Stores: make(map[string]pb.LogStoreInfo),
	}
	for _, uuid := range []string{"log-a", "log-b", "log-c", "log-d", "log-e", "log-f", "log-g"} {
		tick := state.Tick
		if uuid == "log-a" || uuid == "log-b" {
			tick = 0
		}
		state.LogState.Stores[uuid] = pb.LogStoreInfo{Tick: tick}
	}

	observe := func(epoch uint64, voters map[uint64]string, reporting []uint64) {
		data := pb.LogShardInfo{ShardID: 1, Epoch: epoch, Replicas: voters}
		state.LogState.Shards[1] = data
		for _, uuid := range voters {
			store := state.LogState.Stores[uuid]
			if store.Tick == 0 { // Keep expired stores' last-known reports; they are not evidence.
				continue
			}
			store.Replicas = nil
			state.LogState.Stores[uuid] = store
		}
		for _, id := range reporting {
			uuid := voters[id]
			store := state.LogState.Stores[uuid]
			store.Replicas = append(store.Replicas, pb.LogReplicaInfo{LogShardInfo: data, ReplicaID: id})
			state.LogState.Stores[uuid] = store
		}
	}
	observe(1, members, []uint64{3, 4, 5})
	for id, uuid := range members {
		store := state.LogState.Stores[uuid]
		if store.Tick == 0 {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: state.LogState.Shards[1], ReplicaID: id}}
			state.LogState.Stores[uuid] = store
		}
	}

	membershipChanges := func() []pb.ScheduleCommand {
		commands := c.Check(alloc, state, false)
		var changes []pb.ScheduleCommand
		for _, command := range commands {
			if command.ServiceType != pb.LogService || command.ConfigChange == nil {
				continue
			}
			switch command.ConfigChange.ChangeType {
			case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
				changes = append(changes, command)
			}
		}
		return changes
	}
	assertLiveExecutor := func(command pb.ScheduleCommand) {
		require.Contains(t, []string{"log-c", "log-d", "log-e"}, command.UUID)
	}

	changes := membershipChanges()
	require.Len(t, changes, 1, "only one config change may use the observed shard epoch")
	require.Equal(t, pb.RemoveReplica, changes[0].ConfigChange.ChangeType)
	require.Equal(t, "log-a", changes[0].ConfigChange.Replica.UUID)
	assertLiveExecutor(changes[0])

	members = map[uint64]string{2: "log-b", 3: "log-c", 4: "log-d", 5: "log-e"}
	observe(2, members, []uint64{3, 4, 5})
	changes = membershipChanges()
	require.Len(t, changes, 1, "the second removal waits for the observed epoch advance")
	require.Equal(t, pb.RemoveReplica, changes[0].ConfigChange.ChangeType)
	require.Equal(t, "log-b", changes[0].ConfigChange.Replica.UUID)
	assertLiveExecutor(changes[0])

	members = map[uint64]string{3: "log-c", 4: "log-d", 5: "log-e"}
	observe(3, members, []uint64{3, 4, 5})
	changes = membershipChanges()
	require.Len(t, changes, 1, "only one replacement is proposed per observed epoch")
	require.Equal(t, pb.AddReplica, changes[0].ConfigChange.ChangeType)
	assertLiveExecutor(changes[0])
	added := changes[0].ConfigChange.Replica
	require.Contains(t, []string{"log-f", "log-g"}, added.UUID)

	members[added.ReplicaID] = added.UUID
	observe(4, members, []uint64{3, 4, 5})
	commands := c.Check(alloc, state, false)
	started := false
	for _, command := range commands {
		if command.ConfigChange != nil && command.ConfigChange.ChangeType == pb.StartReplica &&
			command.ConfigChange.Replica.UUID == added.UUID {
			started = true
		}
	}
	require.True(t, started, "the configured replacement is started after membership is observed")

	observe(4, members, []uint64{3, 4, 5, added.ReplicaID})
	commands = c.Check(alloc, state, false)
	for _, command := range commands {
		if command.ConfigChange != nil {
			require.False(t, command.ConfigChange.ChangeType == pb.StartReplica && command.ConfigChange.Replica.UUID == added.UUID,
				"a reported replacement is no longer started repeatedly")
		}
	}
}

func TestCoordinatorPreservesVotingQuorumAfterRemoval(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failedID   uint64
		reporters  uint64
		wantRemove string
	}{
		{name: "remove unavailable voter before losing quorum", failedID: 5, reporters: 3, wantRemove: "log-e"},
		{name: "remove a live surplus voter without a replica report", reporters: 3, wantRemove: "log-d"},
		{name: "wait without a reporting majority", reporters: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			currentTick := uint64(1)
			if tc.failedID != 0 {
				currentTick = expiredTick + 1
			}
			c := NewCoordinator("", hakeeper.Config{})
			state := pb.CheckerState{
				Tick: currentTick,
				ClusterInfo: pb.ClusterInfo{
					LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
				},
				LogState: pb.LogState{
					Shards: map[uint64]pb.LogShardInfo{},
					Stores: map[string]pb.LogStoreInfo{},
				},
			}
			voters := map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c", 4: "log-d", 5: "log-e"}
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: voters}
			state.LogState.Shards[1] = shard
			for id, uuid := range voters {
				tick := currentTick
				if id == tc.failedID {
					tick = 0
				}
				store := pb.LogStoreInfo{Tick: tick}
				if id <= tc.reporters {
					store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}
				}
				state.LogState.Stores[uuid] = store
			}

			commands := c.Check(util.NewTestIDAllocator(100), state, false)
			var membership []pb.ScheduleCommand
			for _, command := range commands {
				if command.ServiceType == pb.LogService && command.ConfigChange != nil {
					switch command.ConfigChange.ChangeType {
					case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
						membership = append(membership, command)
					}
				}
			}
			if tc.wantRemove == "" {
				require.Empty(t, membership)
				return
			}
			require.Len(t, membership, 1)
			require.Equal(t, pb.RemoveReplica, membership[0].ConfigChange.ChangeType)
			require.Equal(t, tc.wantRemove, membership[0].ConfigChange.Replica.UUID)
			require.Contains(t, []string{"log-a", "log-b", "log-c"}, membership[0].UUID)
		})
	}
}

func TestCoordinatorWaitsForInFlightStartBeforeRemovingReplica(t *testing.T) {
	for _, finish := range []string{"reported", "expired"} {
		t.Run(finish, func(t *testing.T) {
			c := NewCoordinator("", hakeeper.Config{})
			alloc := util.NewTestIDAllocator(100)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
			state := pb.CheckerState{Tick: 1,
				ClusterInfo: pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
				LogState:    pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{}},
			}
			observe := func(reportE bool) {
				state.LogState.Shards[1] = shard
				for id, uuid := range shard.Replicas {
					store := pb.LogStoreInfo{Tick: 1, TaskServiceCreated: true}
					if id <= 3 || (id == 5 && reportE) {
						store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}
					}
					state.LogState.Stores[uuid] = store
				}
			}
			commandsOf := func(commands []pb.ScheduleCommand, kind pb.ConfigChangeType) []uint64 {
				var ids []uint64
				for _, command := range commands {
					if command.ConfigChange != nil && command.ConfigChange.ChangeType == kind {
						ids = append(ids, command.ConfigChange.Replica.ReplicaID)
					}
				}
				return ids
			}
			observe(false)
			first := c.Check(alloc, state, false)
			require.Equal(t, []uint64{4}, commandsOf(first, pb.RemoveReplica))
			require.Equal(t, []uint64{5}, commandsOf(first, pb.StartReplica))
			var start *operator.Operator
			for _, op := range c.OperatorController.GetOperators(1) {
				for _, step := range op.OpSteps() {
					if s, ok := step.(operator.StartLogService); ok && s.ReplicaID == 5 {
						start = op
					}
				}
			}
			require.NotNil(t, start)
			require.Equal(t, operator.STARTED, start.Status())
			shard.Epoch++
			delete(shard.Replicas, 4)
			observe(false)
			second := c.Check(alloc, state, false)
			require.NotContains(t, commandsOf(second, pb.RemoveReplica), uint64(5))
			require.Equal(t, operator.STARTED, start.Status())
			if finish == "reported" {
				observe(true)
			} else {
				start.SetStatus(operator.EXPIRED)
			}
			third := c.Check(alloc, state, false)
			removed := commandsOf(third, pb.RemoveReplica)
			require.Len(t, removed, 1, "removal can resume after the Start finishes")
			if finish == "expired" {
				require.Equal(t, uint64(5), removed[0], "the unreported voter is the safe removal")
			}
		})
	}
}

func TestCoordinatorKeepsConflictingRemovalAtSameEpoch(t *testing.T) {
	for _, release := range []string{"epoch", "expired"} {
		t.Run(release, func(t *testing.T) {
			c := NewCoordinator("", hakeeper.Config{})
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7,
				Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"}}
			state := pb.CheckerState{Tick: 1,
				ClusterInfo: pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
				LogState:    pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{"d": {Tick: 1}}},
			}
			for id, uuid := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
				state.LogState.Stores[uuid] = pb.LogStoreInfo{Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
			}
			start := operator.CreateStartReplica("", "d", 1, 4)
			remove := operator.NewOperator("", 1, 7, operator.RemoveLogService{
				Target: "a", Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4, Epoch: 7},
			})
			c.OperatorController.Dispatch([]*operator.Operator{start, remove}, state.LogState, state.TNState, state.CNState, state.ProxyState)
			store := state.LogState.Stores["a"]
			store.Replicas = nil
			state.LogState.Stores["a"] = store
			commands := c.Check(util.NewTestIDAllocator(100), state, false)
			require.Equal(t, operator.STARTED, remove.Status(), "a previously submitted removal remains owned at the same epoch")
			require.Contains(t, c.OperatorController.GetOperators(1), remove)
			for _, cmd := range commands {
				if cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ShardID == 1 && cmd.ConfigChange.Replica.ReplicaID == 4 {
					require.NotEqual(t, pb.StartReplica, cmd.ConfigChange.ChangeType)
					require.NotEqual(t, pb.RemoveReplica, cmd.ConfigChange.ChangeType)
				}
			}
			if release == "epoch" {
				shard.Epoch++
				state.LogState.Shards[1] = shard
				c.Check(util.NewTestIDAllocator(100), state, false)
				require.Equal(t, operator.CANCELED, remove.Status(), "an advanced epoch fences the old ordered change")
			} else {
				remove.SetStatus(operator.EXPIRED)
				c.Check(util.NewTestIDAllocator(100), state, false)
				require.NotContains(t, c.OperatorController.GetOperators(1), remove)
				require.Contains(t, c.OperatorController.GetOperators(1), start)
			}
		})
	}
}

func TestCoordinatorRetiresStaleMembershipOperators(t *testing.T) {
	for _, staleBy := range []string{"epoch", "executor", "destination_missing", "destination_expired"} {
		t.Run(staleBy, func(t *testing.T) {
			c := NewCoordinator("", hakeeper.Config{})
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c"}}
			state := pb.CheckerState{
				Tick:        1,
				ClusterInfo: pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 4}}},
				LogState: pb.LogState{
					Shards: map[uint64]pb.LogShardInfo{1: shard},
					Stores: map[string]pb.LogStoreInfo{
						"log-a": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
						"log-b": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
						"log-c": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}},
						"log-d": {Tick: 1},
						"log-e": {Tick: 1},
					},
				},
			}
			stale := operator.NewOperator("stale", 1, 1, operator.AddLogService{
				Target: "log-a", Replica: operator.Replica{UUID: "log-d", ShardID: 1, ReplicaID: 4, Epoch: 1},
			})
			commands := c.OperatorController.Dispatch([]*operator.Operator{stale}, state.LogState, state.TNState, state.CNState, state.ProxyState)
			require.Len(t, commands, 1)

			switch staleBy {
			case "epoch":
				shard.Epoch = 2
				state.LogState.Shards[1] = shard
			case "executor":
				store := state.LogState.Stores["log-a"]
				store.Replicas = nil
				state.LogState.Stores["log-a"] = store
			case "destination_missing":
				delete(state.LogState.Stores, "log-d")
			case "destination_expired":
				state.Tick = expiredTick + 2
				for _, uuid := range []string{"log-a", "log-b", "log-c", "log-e"} {
					store := state.LogState.Stores[uuid]
					store.Tick = state.Tick
					state.LogState.Stores[uuid] = store
				}
			}

			commands = c.Check(util.NewTestIDAllocator(100), state, false)
			require.Equal(t, operator.CANCELED, stale.Status())
			if staleBy == "executor" {
				// Two reporting voters cannot safely grow three voters to four.
				// The missing configured replica must restart first.
				for _, command := range commands {
					if command.ConfigChange != nil {
						require.NotEqual(t, pb.AddReplica, command.ConfigChange.ChangeType)
					}
				}
				store := state.LogState.Stores["log-a"]
				store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}
				state.LogState.Stores["log-a"] = store
				commands = c.Check(util.NewTestIDAllocator(100), state, false)
			}
			var add *pb.ScheduleCommand
			for i := range commands {
				if commands[i].ServiceType == pb.LogService && commands[i].ConfigChange != nil &&
					commands[i].ConfigChange.ChangeType == pb.AddReplica {
					add = &commands[i]
					break
				}
			}
			require.NotNil(t, add, "fresh membership repair must proceed without waiting for operator TTL")
			require.Equal(t, shard.Epoch, add.ConfigChange.Replica.Epoch)
		})
	}
}

func TestCoordinatorGatesMembershipWithoutVoterMajority(t *testing.T) {
	c := NewCoordinator("", hakeeper.Config{})
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 3, Replicas: map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c"}}
	state := pb.CheckerState{
		Tick: 1,
		ClusterInfo: pb.ClusterInfo{
			LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 5}},
		},
		NonVotingReplicaNum: 1,
		LogState: pb.LogState{
			Shards: map[uint64]pb.LogShardInfo{1: shard},
			Stores: map[string]pb.LogStoreInfo{
				"log-a": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"log-b": {Tick: 1},
				"log-c": {Tick: 1},
				"log-d": {Tick: 1},
				"log-e": {Tick: 1},
			},
		},
	}
	commands := c.Check(util.NewTestIDAllocator(100), state, false)
	for _, command := range commands {
		if command.ServiceType == pb.LogService && command.ConfigChange != nil {
			switch command.ConfigChange.ChangeType {
			case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
				t.Fatalf("membership change without voting majority: %+v", command)
			}
		}
	}
}

func TestCoordinatorReplansUnconsumedMembershipAfterExpiry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recovered bool
	}{{"still required", false}, {"target recovered", true}} {
		t.Run(tc.name, func(t *testing.T) {
			c, state, alloc := tnStartScenario(t)
			state.Tick = 10000
			state.TNState = pb.TNState{}
			state.ClusterInfo = pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}}
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, LeaderID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			state.LogState = pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
				"a": {Tick: state.Tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"b": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
				"c": {Tick: state.Tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}},
			}}
			hasRemoval := func(cmds []pb.ScheduleCommand) bool {
				for _, cmd := range cmds {
					if cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.RemoveReplica && cmd.ConfigChange.Replica.ReplicaID == 2 {
						return true
					}
				}
				return false
			}
			require.True(t, hasRemoval(c.Check(alloc, state, false)))
			var removal *operator.Operator
			for _, op := range c.OperatorController.GetOperators(1) {
				for _, step := range op.OpSteps() {
					if r, ok := step.(operator.RemoveLogService); ok && r.ReplicaID == 2 {
						removal = op
					}
				}
			}
			require.NotNil(t, removal)
			require.False(t, hasRemoval(c.Check(alloc, state, false)), "a still-owned operator is not repeatedly delivered")
			if tc.recovered {
				store := state.LogState.Stores["b"]
				store.Tick = state.Tick
				state.LogState.Stores["b"] = store
			}
			removal.SetStatus(operator.EXPIRED)
			require.Equal(t, !tc.recovered, hasRemoval(c.Check(alloc, state, false)), "retry must follow the current desired state")
		})
	}
}

func TestCoordinatorRetainsIssuedRemovalWithoutStart(t *testing.T) {
	for _, nonVoting := range []bool{false, true} {
		for _, retryTN := range []bool{false, true} {
			for _, release := range []string{"committed", "epoch", "expired"} {
				name := fmt.Sprintf("nonVoting=%t/tnRetry=%t/%s", nonVoting, retryTN, release)
				t.Run(name, func(t *testing.T) {
					c, state, alloc := tnStartScenario(t)
					if !retryTN {
						state.ClusterInfo.TNShards = nil
						state.TNState = pb.TNState{}
					}
					shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
					change := pb.RemoveReplica
					if nonVoting {
						shard.NonVotingReplicas = map[uint64]string{4: "d"}
						change = pb.RemoveNonVotingReplica
					} else {
						shard.Replicas[4] = "d"
					}
					state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
					state.LogState = pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{"d": {Tick: 1, TaskServiceCreated: true}}}
					for id, uuid := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
						state.LogState.Stores[uuid] = pb.LogStoreInfo{Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
					}
					first := c.Check(alloc, state, false)
					var removal *pb.ScheduleCommand
					for i := range first {
						if first[i].ConfigChange != nil && first[i].ConfigChange.ChangeType == change {
							removal = &first[i]
							break
						}
					}
					require.NotNil(t, removal, "the first scheduling round must issue the removal")
					require.Equal(t, uint64(4), removal.ConfigChange.Replica.ReplicaID)
					var owner *operator.Operator
					for _, op := range c.OperatorController.GetOperators(1) {
						for _, step := range op.OpSteps() {
							switch step := step.(type) {
							case operator.RemoveLogService:
								if step.ReplicaID == 4 {
									owner = op
								}
							case operator.RemoveNonVotingLogService:
								if step.ReplicaID == 4 {
									owner = op
								}
							case operator.StartLogService:
								require.NotEqual(t, uint64(4), step.ReplicaID)
							case operator.StartNonVotingLogService:
								require.NotEqual(t, uint64(4), step.ReplicaID)
							}
						}
					}
					require.NotNil(t, owner)
					executor := state.LogState.Stores[removal.UUID]
					unreported := executor
					unreported.Replicas = nil
					state.LogState.Stores[removal.UUID] = unreported
					second := c.Check(alloc, state, false)
					require.Equal(t, operator.STARTED, owner.Status())
					require.Contains(t, c.OperatorController.GetOperators(1), owner)
					startedExecutor := false
					for _, cmd := range second {
						if cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.StartReplica && cmd.UUID == removal.UUID {
							startedExecutor = true
						}
						if cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ReplicaID == 4 {
							require.NotEqual(t, pb.StartReplica, cmd.ConfigChange.ChangeType)
							require.NotEqual(t, pb.StartNonVotingReplica, cmd.ConfigChange.ChangeType)
							require.NotEqual(t, change, cmd.ConfigChange.ChangeType, "an unreporting executor cannot retry")
						}
					}
					require.True(t, startedExecutor, "the owned removal must not stall an independent replica")
					state.LogState.Stores[removal.UUID] = executor
					restored := c.Check(alloc, state, false)
					if retryTN {
						require.Contains(t, restored, *removal, "TN retries may replay a still-authorized removal")
					}
					switch release {
					case "committed":
						delete(shard.Replicas, 4)
						delete(shard.NonVotingReplicas, 4)
						shard.Epoch++
						state.LogState.Shards[1] = shard
					case "epoch":
						shard.Epoch++
						state.LogState.Shards[1] = shard
					case "expired":
						owner.SetStatus(operator.EXPIRED)
					}
					c.Check(alloc, state, false)
					require.NotContains(t, c.OperatorController.GetOperators(1), owner)
					if release == "committed" {
						require.Equal(t, operator.SUCCESS, owner.Status())
					} else if release == "epoch" {
						require.Equal(t, operator.CANCELED, owner.Status())
					} else {
						require.Equal(t, operator.EXPIRED, owner.Status())
					}
				})
			}
		}
	}
}

func TestCoordinatorBootstrapOwnsReplicaStart(t *testing.T) {
	c := NewCoordinator("", hakeeper.Config{})
	alloc := util.NewTestIDAllocator(100)
	state := pb.CheckerState{Tick: 1,
		ClusterInfo: pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}},
		LogState: pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1, Replicas: map[uint64]string{}}}, Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 1, TaskServiceCreated: true}, "b": {Tick: 1, TaskServiceCreated: true}, "c": {Tick: 1, TaskServiceCreated: true},
		}},
	}
	first := c.Check(alloc, state, false)
	var bootstrap []pb.ScheduleCommand
	for _, cmd := range first {
		if cmd.BootstrapShard != nil {
			bootstrap = append(bootstrap, cmd)
		}
	}
	require.Len(t, bootstrap, 3, "the first real scheduling round must bootstrap all replicas")
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: bootstrap[0].BootstrapShard.InitialMembers}
	state.LogState.Shards[1] = shard
	var missing pb.ScheduleCommand
	for i, cmd := range bootstrap {
		if i == 0 {
			missing = cmd
			continue
		}
		store := state.LogState.Stores[cmd.UUID]
		store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: cmd.BootstrapShard.ReplicaID}}
		state.LogState.Stores[cmd.UUID] = store
	}
	second := c.Check(alloc, state, false)
	t.Logf("unreported bootstrap store=%s id=%d commands=%+v", missing.UUID, missing.BootstrapShard.ReplicaID, second)
	for _, cmd := range second {
		if cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ReplicaID == missing.BootstrapShard.ReplicaID {
			require.NotEqual(t, pb.StartReplica, cmd.ConfigChange.ChangeType, "an in-flight bootstrap already owns this local start")
		}
	}
	// Even when the missing member becomes surplus, its in-flight bootstrap
	// owns the local start until a report or expiry releases it.
	state.ClusterInfo.LogShards[0].NumberOfReplicas = 2
	for _, cmd := range c.Check(alloc, state, false) {
		if cmd.ConfigChange != nil && cmd.ConfigChange.Replica.ReplicaID == missing.BootstrapShard.ReplicaID {
			require.NotEqual(t, pb.RemoveReplica, cmd.ConfigChange.ChangeType)
		}
	}
	store := state.LogState.Stores[missing.UUID]
	store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: missing.BootstrapShard.ReplicaID}}
	state.LogState.Stores[missing.UUID] = store
	removed := false
	for _, cmd := range c.Check(alloc, state, false) {
		if cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.RemoveReplica {
			removed = true
		}
	}
	require.True(t, removed, "removal resumes after bootstrap is reported")
}
