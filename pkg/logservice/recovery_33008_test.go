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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

// A real disk-backed three-store Raft group. The retired store never returns:
// a surviving store must reopen its existing replica from an actual command.
func TestRecoverExistingReplicaWithoutRetiredStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var configs [3]Config
	var services [3]*Service
	members := make(map[uint64]string)
	seeds := make([]string, 0, len(configs))
	for i := range configs {
		configs[i] = getServiceTestConfig()
		configs[i].DataDir = t.TempDir()
		configs[i].FS = vfs.Default
		configs[i].LogDBDisablePrealloc = true
		configs[i].RPC.MaxMessageSize = DefaultConfig().RPC.MaxMessageSize
		seeds = append(seeds, getTestGossipAddress(configs[i].GossipPort))
		members[uint64(i+1)] = configs[i].UUID
	}
	for i := range configs {
		configs[i].GossipSeedAddresses = seeds
		configs[i].HAKeeperClientConfig.ServiceAddresses = nil
		for j := range configs {
			configs[i].HAKeeperClientConfig.ServiceAddresses = append(configs[i].HAKeeperClientConfig.ServiceAddresses, getTestServiceAddress(configs[j].LogServicePort))
		}
	}
	open := func(i int) {
		t.Helper()
		s, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = s
	}
	for i := range configs {
		open(i)
		t.Cleanup(func() {
			if services[i] != nil {
				require.NoError(t, services[i].Close())
			}
		})
		require.NoError(t, services[i].store.startHAKeeperReplica(uint64(i+1), members, false))
		require.NoError(t, services[i].store.startReplica(1, uint64(i+1), members, false))
	}
	readMembership := func(s *Service, shard uint64) *dragonboat.Membership {
		t.Helper()
		var membership *dragonboat.Membership
		require.Eventually(t, func() bool {
			attempt, stop := context.WithTimeout(ctx, 300*time.Millisecond)
			defer stop()
			var err error
			membership, err = s.store.nh.SyncGetShardMembership(attempt, shard)
			return err == nil && len(membership.Nodes) == 3
		}, 12*time.Second, 30*time.Millisecond)
		return membership
	}
	a, b, c := services[0], services[1], services[2]
	readMembership(a, 0)
	readMembership(a, 1)
	localHAID := atomic.LoadUint64(&a.store.haKeeperReplicaID)
	require.Error(t, a.store.stopReplica(0, localHAID+100))
	require.Equal(t, localHAID, atomic.LoadUint64(&a.store.haKeeperReplicaID), "a failed stop must not clear the live HAKeeper ID")
	var client LogHAKeeperClient
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		var err error
		client, err = NewLogHAKeeperClient(attempt, c.ID(), configs[2].GetHAKeeperClientConfig())
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	for _, service := range services {
		_, err := client.SendLogHeartbeat(ctx, service.store.getHeartbeatMessage())
		require.NoError(t, err)
	}
	state, err := client.GetClusterState(ctx)
	require.NoError(t, err)
	require.Equal(t, members, state.LogState.Shards[1].Replicas)
	old, err := a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	require.NoError(t, err)
	require.NoError(t, client.Close())

	// Preserve C's Dragonboat files while modeling missing MO replica metadata.
	require.NoError(t, b.Close())
	services[1] = nil
	require.NoError(t, c.store.stopReplica(1, 3))
	c.store.removeMetadata(1, 3)
	require.NoError(t, c.Close())
	services[2] = nil
	open(2)
	c = services[2]
	require.True(t, c.store.nh.HasNodeInfo(1, 3))
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound)
	attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	stop()
	require.Error(t, err, "one surviving data voter must not commit")
	readMembership(c, 0)
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		var err error
		c.haClient, err = NewLogHAKeeperClient(attempt, c.ID(), configs[2].GetHAKeeperClientConfig())
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	_, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	wrong := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 2, UUID: c.ID()}}}
	c.handleCommands(context.Background(), []pb.ScheduleCommand{wrong})
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound, "another voter's ID must not be resurrected")
	start := wrong
	start.ConfigChange = &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 3, UUID: c.ID()}}
	shutdown := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: c.ID()}}
	c.handleCommands(context.Background(), []pb.ScheduleCommand{shutdown, start})
	readMembership(c, 1)
	var next uint64
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		defer stop()
		result, err := a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
		next = result.Value
		return err == nil && next > old.Value
	}, 10*time.Second, 30*time.Millisecond)
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, 300*time.Millisecond)
		defer stop()
		lsn, err := c.store.getLatestLsn(attempt, 1)
		return err == nil && lsn >= next
	}, 10*time.Second, 30*time.Millisecond)

	// Once HAKeeper has observed the removal, an old command must not start
	// the retired identity on a replacement disk. A later authorized identity
	// can still join the same real Raft group.
	open(1)
	b = services[1]
	readMembership(b, 1)
	require.NoError(t, c.Close())
	services[2] = nil
	beforeRemove := readMembership(a, 1)
	require.NoError(t, a.store.nh.SyncRequestDeleteReplica(ctx, 1, 3, beforeRemove.ConfigChangeID))
	configs[2].DataDir = t.TempDir()
	open(2)
	c = services[2]
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		var err error
		c.haClient, err = NewLogHAKeeperClient(attempt, c.ID(), configs[2].GetHAKeeperClientConfig())
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	waitForReport := func(id uint64, present bool) pb.LogShardInfo {
		t.Helper()
		var observed pb.LogShardInfo
		require.Eventually(t, func() bool {
			attempt, stop := context.WithTimeout(ctx, time.Second)
			defer stop()
			for _, service := range []*Service{a, b} {
				if _, err := c.haClient.SendLogHeartbeat(attempt, service.store.getHeartbeatMessage()); err != nil {
					return false
				}
			}
			state, err := c.haClient.GetLogScheduleState(attempt, 1, c.ID(), false)
			if err != nil {
				return false
			}
			observed = state.LogState.Shards[1]
			_, exists := observed.Replicas[id]
			return observed.Epoch > beforeRemove.ConfigChangeID && exists == present
		}, 10*time.Second, 30*time.Millisecond)
		return observed
	}
	afterRemove := waitForReport(3, false)
	c.handleCommands(ctx, []pb.ScheduleCommand{start})
	require.False(t, c.store.nh.HasNodeInfo(1, 3), "an observed retired identity must not create Raft state")
	require.NoError(t, a.store.nh.SyncRequestAddReplica(ctx, 1, 4, c.ID(), afterRemove.Epoch))
	waitForReport(4, true)
	replacement := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 4, UUID: c.ID()}}}
	c.handleCommands(ctx, []pb.ScheduleCommand{replacement})
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		membership, err := c.store.nh.SyncGetShardMembership(attempt, 1)
		return err == nil && membership.Nodes[4] == c.ID()
	}, 10*time.Second, 30*time.Millisecond, "the replacement must join the current group")

	// Dynamic bootstrap commands are one-shot. They must carry the same full
	// initial membership to all three nodes even when validation is unavailable.
	dynamicMembers := map[uint64]string{11: a.ID(), 12: b.ID(), 13: c.ID()}
	for i, service := range []*Service{a, b, c} {
		client := &scheduleStateClient{waitForDeadline: true}
		previous := service.haClient
		service.haClient = client
		id := uint64(i + 11)
		service.handleCommands(ctx, []pb.ScheduleCommand{{UUID: service.ID(), ServiceType: pb.LogService,
			BootstrapShard: &pb.BootstrapShard{ShardID: 2, ReplicaID: id, InitialMembers: dynamicMembers}}})
		service.haClient = previous
		require.Zero(t, client.calls)
	}
	readMembership(a, 2)
	_, err = a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(2), getTestUserEntry())
	require.NoError(t, err, "a one-shot dynamic bootstrap must form a writable three-node group")
}

func TestNonVotingStartChecksIdentityAndReopensDisk(t *testing.T) {
	cfg := getServiceTestConfig()
	cfg.DataDir = t.TempDir()
	cfg.FS = vfs.Default
	cfg.LogDBDisablePrealloc = true
	var service *Service
	open := func() {
		t.Helper()
		var err error
		service, err = NewService(cfg, newFS(), nil)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		if service != nil {
			require.NoError(t, service.Close())
		}
	})
	open()
	state := func(voters, nonvoters map[uint64]string) pb.CheckerState {
		return pb.CheckerState{LogState: pb.LogState{Shards: map[uint64]pb.LogShardInfo{
			1: {ShardID: 1, Epoch: 3, Replicas: voters, NonVotingReplicas: nonvoters},
		}}}
	}
	client := &scheduleStateClient{states: []pb.CheckerState{
		state(map[uint64]string{2: service.ID()}, nil),
		state(nil, map[uint64]string{2: "different-store"}),
		state(nil, map[uint64]string{2: service.ID()}),
	}}
	service.haClient = client
	start := pb.ScheduleCommand{UUID: service.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartNonVotingReplica,
			Replica: pb.Replica{UUID: service.ID(), ShardID: 1, ReplicaID: 2}}}
	service.handleCommands(context.Background(), []pb.ScheduleCommand{start})
	require.False(t, service.store.nh.HasNodeInfo(1, 2), "a voting identity must not authorize a non-voter")
	service.handleCommands(context.Background(), []pb.ScheduleCommand{start})
	require.False(t, service.store.nh.HasNodeInfo(1, 2), "another store's identity must not authorize this start")
	service.handleCommands(context.Background(), []pb.ScheduleCommand{start})
	require.True(t, service.store.nh.HasNodeInfo(1, 2))
	require.NoError(t, service.store.stopReplica(1, 2))
	service.store.removeMetadata(1, 2)
	require.NoError(t, service.Close())
	service = nil

	open()
	require.True(t, service.store.nh.HasNodeInfo(1, 2), "the original Raft bootstrap must remain on disk")
	service.haClient = &scheduleStateClient{states: []pb.CheckerState{state(nil, map[uint64]string{2: service.ID()})}}
	service.handleCommands(context.Background(), []pb.ScheduleCommand{start})
	var found bool
	for _, local := range service.store.nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true}).ShardInfoList {
		found = found || (local.ShardID == 1 && local.ReplicaID == 2)
	}
	require.True(t, found, "the authorized non-voter must reopen its original Raft state")
	shards := service.store.getShards()
	require.Len(t, shards, 1)
	require.Equal(t, uint64(1), shards[0].ShardID)
	require.Equal(t, uint64(2), shards[0].ReplicaID)
	require.True(t, shards[0].NonVoting)
}
