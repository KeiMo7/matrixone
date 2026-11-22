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

	"github.com/gogo/protobuf/proto"
	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestLogHeartbeatAuthorizesStartsWithoutChangingDeliveryState(t *testing.T) {
	const store = "store-a"
	start := func(kind pb.ConfigChangeType, shard, replica uint64, target string) pb.ScheduleCommand {
		return pb.ScheduleCommand{UUID: store, ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: kind, Replica: pb.Replica{UUID: target, ShardID: shard, ReplicaID: replica}}}
	}
	valid := start(pb.StartReplica, 1, 11, store)
	removed := start(pb.StartReplica, 1, 12, store)
	wrongRole := start(pb.StartNonVotingReplica, 1, 11, store)
	wrongTarget := start(pb.StartReplica, 1, 11, "store-b")
	unknown := start(pb.StartReplica, 99, 99, store)
	nonvoting := start(pb.StartNonVotingReplica, 3, 31, store)
	bootstrap := start(pb.StartReplica, 4, 41, store)
	bootstrap.Bootstrapping = true
	bootstrap.ConfigChange.InitialMembers = map[uint64]string{41: store}
	dynamic := pb.ScheduleCommand{UUID: store, ServiceType: pb.LogService, BootstrapShard: &pb.BootstrapShard{ShardID: 5, ReplicaID: 51}}
	shutdown := pb.ScheduleCommand{UUID: store, ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: store}}
	commands := []pb.ScheduleCommand{valid, removed, wrongRole, wrongTarget, unknown, nonvoting, bootstrap, dynamic, shutdown}

	machine := NewStateMachine(0, 1).(*stateMachine)
	machine.state.LogState.Stores[store] = pb.LogStoreInfo{}
	machine.state.LogState.Shards[1] = pb.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{11: store}}
	machine.state.LogState.Shards[3] = pb.LogShardInfo{ShardID: 3, NonVotingReplicas: map[uint64]string{31: store}}
	machine.state.ScheduleCommands[store] = pb.CommandBatch{Commands: commands}
	before := proto.Clone(&machine.state).(*pb.HAKeeperRSMState)
	hb := pb.LogStoreHeartbeat{UUID: store}
	data, err := hb.Marshal()
	require.NoError(t, err)
	result, err := machine.Update(sm.Entry{Index: 1, Cmd: GetLogStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	var got pb.CommandBatch
	require.NoError(t, got.Unmarshal(result.Data))
	require.Equal(t, []pb.ScheduleCommand{valid, nonvoting, bootstrap, dynamic, shutdown}, got.Commands)

	// Filtering the response must not change legacy queue or bootstrap retry
	// state while historical heartbeat entries are replayed.
	reference := NewStateMachine(0, 1).(*stateMachine)
	reference.state = *before
	reference.state.Index = 1
	reference.state.LogState.Update(hb, reference.state.Tick)
	reference.getCommandBatch(store)
	require.True(t, proto.Equal(&reference.state, &machine.state))
	require.Equal(t, []pb.ScheduleCommand{bootstrap}, machine.state.ScheduleCommands[store].Commands)
}
