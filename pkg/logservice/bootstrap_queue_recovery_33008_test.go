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

package logservice

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestDynamicBootstrapSurvivesLegacyScheduleReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	var configs [3]Config
	var services [3]*Service
	seeds := make([]string, 0, len(configs))
	for i := range configs {
		configs[i] = getServiceTestConfig()
		configs[i].RTTMillisecond = 100
		configs[i].DisableWorkers = true
		configs[i].DataDir = t.TempDir()
		configs[i].SnapshotExportDir = filepath.Join(configs[i].DataDir, "export")
		configs[i].FS = vfs.Default
		configs[i].LogDBDisablePrealloc = true
		configs[i].RPC.MaxMessageSize = DefaultConfig().RPC.MaxMessageSize
		seeds = append(seeds, getTestGossipAddress(configs[i].GossipPort))
	}
	for i := range configs {
		configs[i].GossipSeedAddresses = seeds
		service, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = service
		idx := i
		t.Cleanup(func() {
			if services[idx] != nil {
				require.NoError(t, services[idx].Close())
			}
		})
	}
	waitForRecoveryPeers(t, ctx, services[:])

	rsm := hakeeper.NewStateMachine(0, 1)
	index := uint64(0)
	apply := func(command []byte) sm.Result {
		t.Helper()
		index++
		result, err := rsm.Update(sm.Entry{Index: index, Cmd: command})
		require.NoError(t, err)
		return result
	}
	logHeartbeat := func(heartbeat pb.LogStoreHeartbeat) pb.CommandBatch {
		t.Helper()
		heartbeat.TaskServiceCreated = true
		data, err := heartbeat.Marshal()
		require.NoError(t, err)
		result := apply(hakeeper.GetLogStoreHeartbeatCmd(data))
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		return batch
	}
	state := func() pb.CheckerState {
		t.Helper()
		value, err := rsm.Lookup(&hakeeper.StateQuery{Scheduling: true})
		require.NoError(t, err)
		return *value.(*pb.CheckerState)
	}
	queue := func(storeID string) pb.CommandBatch {
		t.Helper()
		value, err := rsm.Lookup(&hakeeper.ScheduleCommandQuery{UUID: storeID})
		require.NoError(t, err)
		return *value.(*pb.CommandBatch)
	}

	apply(hakeeper.GetInitialClusterRequestCmd(1, 1, 3, 0, nil, nil))
	apply(hakeeper.GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
	apply(hakeeper.GetSetStateCmd(pb.HAKeeperRunning))
	config := hakeeper.Config{}
	config.Fill()
	apply(hakeeper.GetTickCmd(1, config.LogStoreTimeoutTicks()))
	apply(hakeeper.GetAddLogShardCmd(pb.AddLogShard{ShardID: 77}))
	for _, service := range services {
		logHeartbeat(service.store.getHeartbeatMessage())
	}

	coordinator := checkers.NewCoordinator("", config)
	allocator := util.NewTestIDAllocator(100)
	initial := coordinator.Check(allocator, state(), false)
	require.Len(t, initial, 3)
	var members map[uint64]string
	replicaIDs := make(map[string]uint64, len(initial))
	for _, command := range initial {
		require.NotNil(t, command.BootstrapShard)
		require.Equal(t, uint64(77), command.BootstrapShard.ShardID)
		require.False(t, command.BootstrapShard.Join)
		if members == nil {
			members = command.BootstrapShard.InitialMembers
		}
		require.Equal(t, members, command.BootstrapShard.InitialMembers)
		replicaIDs[command.UUID] = command.BootstrapShard.ReplicaID
	}
	require.Len(t, members, 3)
	apply(hakeeper.GetUpdateCommandsCmd(1, initial))
	for _, service := range services {
		queued := queue(service.ID())
		require.Len(t, queued.Commands, 1)
		require.NotNil(t, queued.Commands[0].BootstrapShard)
	}

	// Deliver only the first approved member and let it report the original map.
	first := services[0]
	delivered := logHeartbeat(first.store.getHeartbeatMessage())
	require.Len(t, delivered.Commands, 1)
	require.NotNil(t, delivered.Commands[0].BootstrapShard)
	first.handleCommands(ctx, delivered.Commands)
	recoveryHeartbeat(t, ctx, first, 77, replicaIDs[first.ID()], members)
	logHeartbeat(first.store.getHeartbeatMessage())
	require.NoError(t, first.Close())
	services[0] = nil

	for _, service := range services[1:] {
		require.False(t, service.store.nh.HasNodeInfo(77, replicaIDs[service.ID()]))
		queued := queue(service.ID())
		require.Len(t, queued.Commands, 1)
		require.NotNil(t, queued.Commands[0].BootstrapShard)
	}

	// Independent TN work produces a real TN-only coordinator batch. Legacy
	// ScheduleCommandUpdate must not erase the other stores' approved bootstrap.
	apply(hakeeper.GetTaskTableUserCmd(pb.TaskTableUser{Username: "test", Password: "test"}))
	tnHeartbeat, err := (&pb.TNStoreHeartbeat{UUID: "fresh-tn"}).Marshal()
	require.NoError(t, err)
	apply(hakeeper.GetTNStoreHeartbeatCmd(tnHeartbeat))
	independent := coordinator.Check(allocator, state(), false)
	require.NotEmpty(t, independent)
	hasTaskCreation := false
	for _, command := range independent {
		require.Equal(t, pb.TNService, command.ServiceType)
		hasTaskCreation = hasTaskCreation || command.CreateTaskService != nil
	}
	require.True(t, hasTaskCreation)
	apply(hakeeper.GetUpdateCommandsCmd(1, independent))
	for _, service := range services[1:] {
		queued := queue(service.ID())
		require.Len(t, queued.Commands, 1)
		require.NotNil(t, queued.Commands[0].BootstrapShard)
		require.Equal(t, members, queued.Commands[0].BootstrapShard.InitialMembers)
	}

	// Snapshot recovery and a fresh coordinator must not let a nil-membership
	// StartReplica replace the still-queued original bootstrap for either peer.
	var snapshot bytes.Buffer
	require.NoError(t, rsm.SaveSnapshot(&snapshot, nil, nil))
	restored := hakeeper.NewStateMachine(0, 44)
	require.NoError(t, restored.RecoverFromSnapshot(&snapshot, nil, nil))
	rsm = restored
	freshCoordinator := checkers.NewCoordinator("", config)
	retried := freshCoordinator.Check(allocator, state(), false)
	starts := make(map[string]bool)
	for _, command := range retried {
		if command.ServiceType == pb.LogService && command.ConfigChange != nil &&
			command.ConfigChange.Replica.ShardID == 77 {
			require.Equal(t, pb.StartReplica, command.ConfigChange.ChangeType)
			require.Empty(t, command.ConfigChange.InitialMembers)
			starts[command.UUID] = true
		}
	}
	for _, service := range services[1:] {
		require.True(t, starts[service.ID()])
	}
	apply(hakeeper.GetUpdateCommandsCmd(2, retried))
	for _, service := range services[1:] {
		queued := queue(service.ID())
		require.Len(t, queued.Commands, 1)
		require.NotNil(t, queued.Commands[0].BootstrapShard)
		require.Equal(t, members, queued.Commands[0].BootstrapShard.InitialMembers)
	}

	// Start the preserved original cohort on real disks and require an actual
	// majority write while the first initial member remains offline.
	for _, service := range services[1:] {
		batch := logHeartbeat(service.store.getHeartbeatMessage())
		require.Len(t, batch.Commands, 1)
		require.NotNil(t, batch.Commands[0].BootstrapShard)
		require.Equal(t, members, batch.Commands[0].BootstrapShard.InitialMembers)
		service.handleCommands(ctx, batch.Commands)
		require.True(t, service.store.nh.HasNodeInfo(77, replicaIDs[service.ID()]))
	}
	waitForRecovery(t, ctx, "write through the surviving initial bootstrap cohort", 12*time.Second, 400*time.Millisecond, func(attempt context.Context) bool {
		_, err := services[1].store.nh.SyncPropose(
			attempt,
			services[1].store.nh.GetNoOPSession(77),
			getTestUserEntry(),
		)
		return err == nil
	})
	for _, service := range services[1:] {
		recoveryHeartbeat(t, ctx, service, 77, replicaIDs[service.ID()], members)
	}
}
