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
	"maps"
	"path/filepath"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

// A real disk-backed three-store Raft group. The retired store never returns:
// a surviving store must reopen its existing replica from an actual command.
func TestRecoverExistingReplicaWithoutRetiredStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	var configs [3]Config
	var services [3]*Service
	members := make(map[uint64]string)
	seeds := make([]string, 0, len(configs))
	for i := range configs {
		configs[i] = getServiceTestConfig()
		configs[i].DataDir = t.TempDir()
		configs[i].SnapshotExportDir = filepath.Join(configs[i].DataDir, "exported-snapshot")
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
	}
	waitForRecoveryPeers(t, ctx, services[:])
	for i, service := range services {
		require.NoError(t, service.store.startHAKeeperReplica(uint64(i+1), members, false))
	}
	readMembership := func(s *Service, shard uint64, expected map[uint64]string) *dragonboat.Membership {
		t.Helper()
		var membership *dragonboat.Membership
		require.Eventually(t, func() bool {
			attempt, stop := context.WithTimeout(ctx, 300*time.Millisecond)
			defer stop()
			var err error
			membership, err = s.store.nh.SyncGetShardMembership(attempt, shard)
			return err == nil && maps.Equal(membership.Nodes, expected)
		}, 12*time.Second, 30*time.Millisecond)
		return membership
	}
	a, b, c := services[0], services[1], services[2]
	readMembership(a, 0, members)
	// Initial members may be observed by HAKeeper one at a time. Each
	// bootstrap command must start without waiting for the full quorum.
	dataMembers := map[uint64]string{11: a.ID(), 12: b.ID(), 13: c.ID()}
	for i, service := range []*Service{a, b, c} {
		id := uint64(i + 11)
		service.handleCommands(ctx, []pb.ScheduleCommand{{UUID: service.ID(), ServiceType: pb.LogService, Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
				Replica: pb.Replica{UUID: service.ID(), ShardID: 1, ReplicaID: id}, InitialMembers: dataMembers}}})
	}
	readMembership(a, 1, dataMembers)
	old, err := a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	require.NoError(t, err, "initial bootstrap must form a writable three-node group")
	clientConfig := configs[2].GetHAKeeperClientConfig()
	clientConfig.ServiceAddresses = []string{configs[0].LogServiceServiceAddr()}
	var client LogHAKeeperClient
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		var err error
		client, err = NewLogHAKeeperClient(attempt, c.ID(), clientConfig)
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	for i, service := range services {
		hb := recoveryHeartbeat(t, ctx, service, 1, uint64(i+11), dataMembers)
		_, err := client.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
	}
	state, err := client.GetClusterState(ctx)
	require.NoError(t, err)
	require.Equal(t, dataMembers, state.LogState.Shards[1].Replicas)
	require.NoError(t, client.Close())

	// Preserve C's Dragonboat files while modeling missing MO replica metadata.
	require.NoError(t, b.Close())
	services[1] = nil
	require.NoError(t, c.store.stopReplica(1, 13))
	c.store.removeMetadata(1, 13)
	require.NoError(t, c.Close())
	services[2] = nil
	open(2)
	c = services[2]
	require.True(t, c.store.nh.HasNodeInfo(1, 13))
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound)
	attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	stop()
	require.Error(t, err, "one surviving data voter must not commit")
	readMembership(c, 0, members)
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		var err error
		c.haClient, err = NewLogHAKeeperClient(attempt, c.ID(), clientConfig)
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	_, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	wrong := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 12, UUID: c.ID()}}}
	var term uint64
	require.Eventually(t, func() bool {
		for _, service := range []*Service{a, c} {
			leader, current, err := service.store.isLeaderHAKeeper()
			if err == nil && leader {
				term = current
				return true
			}
		}
		return false
	}, 10*time.Second, 30*time.Millisecond)
	_, err = a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(0), hakeeper.GetUpdateCommandsCmd(term, []pb.ScheduleCommand{wrong}))
	require.NoError(t, err)
	delivered, err := c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	require.Empty(t, delivered.Commands, "a different store's identity must be rejected at delivery")
	c.handleCommands(context.Background(), delivered.Commands)
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound, "another voter's ID must not be resurrected")
	start := wrong
	start.ConfigChange = &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 13, UUID: c.ID()}}
	shutdown := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: c.ID()}}
	c.handleCommands(context.Background(), []pb.ScheduleCommand{shutdown, start})
	readMembership(c, 1, dataMembers)
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
}
