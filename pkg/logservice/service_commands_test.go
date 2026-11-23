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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func TestRequestHeartbeatCoalescesBeforeWorker(t *testing.T) {
	service := &Service{heartbeatC: make(chan struct{}, 1)}
	service.requestHeartbeat()
	service.requestHeartbeat()

	select {
	case <-service.heartbeatC:
	default:
		t.Fatal("heartbeat request was not retained")
	}
	select {
	case <-service.heartbeatC:
		t.Fatal("heartbeat requests were not coalesced")
	default:
	}
}

func TestBackgroundTickAndHeartbeat(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			cfg := DefaultConfig()
			cfg.UUID = uuid.New().String()
			cfg.FS = vfs.NewStrictMem()
			cfg.DeploymentID = 1
			cfg.RTTMillisecond = 5
			cfg.DataDir = "data-1"
			cfg.LogServicePort = 9002
			cfg.RaftPort = 9000
			cfg.GossipPort = 9001
			// below is an unreachable address intentionally set
			cfg.GossipSeedAddresses = []string{"127.0.0.1:9010"}
			cfg.HeartbeatInterval.Duration = 5 * time.Millisecond
			cfg.HAKeeperTickInterval.Duration = 5 * time.Millisecond
			cfg.HAKeeperClientConfig.ServiceAddresses = []string{"127.0.0.1:9002"}

			runtime.SetupServiceBasedRuntime(cfg.UUID, rt)

			service, err := NewService(
				cfg,
				newFS(),
				nil,
				WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
					return true
				}),
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, service.Close())
			}()
			peers := make(map[uint64]dragonboat.Target)
			peers[1] = service.ID()
			require.NoError(t, service.store.startHAKeeperReplica(1, peers, false))

			for i := 0; i < 500; i++ {
				done := true
				state, err := service.store.getCheckerState()
				require.NoError(t, err)
				if state.Tick < 10 {
					done = false
				}
				si, ok := state.LogState.Stores[service.ID()]
				if !ok {
					done = false
				} else {
					if si.Tick < 10 {
						done = false
					}
				}
				if done {
					return
				} else {
					time.Sleep(5 * time.Millisecond)
				}
			}
			t.Fatalf("failed to tick/heartbeat")
		},
	)
}

func TestHandleKillZombie(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)

		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.KillZombie,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		mustHaveReplica(t, s.store, 1, 1)
		s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
		assert.False(t, hasReplica(s.store, 1, 1))

		has, err = hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.False(t, has)
	}
	runServiceTest(t, false, true, fn)
}

func TestHandleStartReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
				InitialMembers: map[uint64]string{1: s.ID()},
			},
		}
		s.handleStartReplica(cmd)
		s.handleStartReplica(cmd) // Retry before the next report.
		mustHaveReplica(t, s.store, 1, 1)

		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)
	}
	runServiceTest(t, false, false, fn)
}

func TestHandleAddNonVotingReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.close())
		require.NoError(t, store2.close())
	}()

	service1 := Service{
		store:   store1,
		runtime: runtime.DefaultRuntime(),
	}
	cmd := pb.ScheduleCommand{
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.AddNonVotingReplica,
			Replica: pb.Replica{
				UUID:      uuid.New().String(),
				ShardID:   1,
				ReplicaID: 3,
				Epoch:     2,
			},
		},
	}
	service1.handleAddNonVotingReplica(cmd)
	service1.handleAddNonVotingReplica(cmd) // Ordered epoch rejects a repeated membership mutation.
	count, ok := checkReplicaCount(store1, 1)
	require.True(t, ok)
	assert.Equal(t, 2, count)
	count, ok = checkNonVotingReplicaCount(store1, 1)
	require.True(t, ok)
	assert.Equal(t, 1, count)
}

func TestHandleStartNonVotingReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		var cfg Config
		genCfg := func() Config {
			cfg = getStoreTestConfig()
			gPort := getTestGossipPort()
			cfg.GossipAddress = fmt.Sprintf("0.0.0.0:%d", gPort)
			cfg.GossipSeedAddresses = []string{getTestGossipAddress(gPort)}
			cfg.GossipPort = getTestGossipPort()
			cfg.GossipSeedAddresses = []string{
				s.store.cfg.GossipAddress,
				getTestGossipAddress(cfg.GossipPort),
			}
			cfg.RaftAddress = getTestRaftAddress()
			svcPort := getTestServicePort()
			cfg.ServiceListenAddress = fmt.Sprintf("0.0.0.0:%d", svcPort)
			cfg.ServiceAddress = getTestServiceAddress(svcPort)
			return cfg
		}
		newStore, err := getTestStore(genCfg, false, nil)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, newStore.close())
		}()
		newService := Service{
			store:   newStore,
			runtime: runtime.DefaultRuntime(),
		}

		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.AddNonVotingReplica,
				Replica: pb.Replica{
					UUID:      newService.ID(),
					ShardID:   1,
					ReplicaID: 100,
					Epoch:     1,
				},
			},
		}
		s.handleAddNonVotingReplica(cmd)
		count, ok := checkReplicaCount(s.store, 1)
		require.True(t, ok)
		assert.Equal(t, 1, count)
		count, ok = checkNonVotingReplicaCount(s.store, 1)
		require.True(t, ok)
		assert.Equal(t, 1, count)

		cmd = pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartNonVotingReplica,
				Replica: pb.Replica{
					UUID:      newService.ID(),
					ShardID:   1,
					ReplicaID: 100,
				},
			},
		}
		newService.handleStartNonVotingReplica(cmd)
		mustHaveNonVotingReplica(t, s.store, 1, 100)
		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 100, newStore.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)
	}
	runServiceTest(t, false, true, fn)
}

func TestHandleStopReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
				InitialMembers: map[uint64]string{1: s.ID()},
			},
		}
		s.handleStartReplica(cmd)
		mustHaveReplica(t, s.store, 1, 1)

		cmd = pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StopReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
		assert.False(t, hasReplica(s.store, 1, 1))

		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)
	}
	runServiceTest(t, false, false, fn)
}

func TestHandleAddReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.close())
		require.NoError(t, store2.close())
	}()

	service1 := Service{
		store:   store1,
		runtime: runtime.DefaultRuntime(),
	}
	cmd := pb.ScheduleCommand{
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.AddReplica,
			Replica: pb.Replica{
				UUID:      uuid.New().String(),
				ShardID:   1,
				ReplicaID: 3,
				Epoch:     2,
			},
		},
	}
	service1.handleAddReplica(cmd)
	service1.handleAddReplica(cmd) // Ordered epoch rejects a repeated membership mutation.
	count, ok := checkReplicaCount(store1, 1)
	require.True(t, ok)
	assert.Equal(t, 3, count)
}

func TestHandleRemoveReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.close())
		require.NoError(t, store2.close())
	}()

	service1 := Service{
		store:   store1,
		runtime: runtime.DefaultRuntime(),
	}
	cmd := pb.ScheduleCommand{
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.RemoveReplica,
			Replica: pb.Replica{
				ShardID:   1,
				ReplicaID: 2,
				Epoch:     2,
			},
		},
	}
	service1.handleRemoveReplica(cmd)
	service1.handleRemoveReplica(cmd) // Ordered epoch rejects a repeated membership mutation.
	count, ok := checkReplicaCount(store1, 1)
	require.True(t, ok)
	assert.Equal(t, 1, count)
}

func checkReplicaCount(s *store, shardID uint64) (int, bool) {
	hb := s.getHeartbeatMessage()
	for _, info := range hb.Replicas {
		if info.ShardID == shardID {
			return len(info.Replicas), true
		}
	}
	return 0, false
}

func checkNonVotingReplicaCount(s *store, shardID uint64) (int, bool) {
	hb := s.getHeartbeatMessage()
	for _, info := range hb.Replicas {
		if info.ShardID == shardID {
			return len(info.NonVotingReplicas), true
		}
	}
	return 0, false
}

func TestHandleLegacyShutdown(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		// A historical shutdown must not close the service or stop a following
		// valid command in the same delivery.
		cmd := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ShutdownStore: &pb.ShutdownStore{StoreID: s.ID()}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := s.store.nh.SyncPropose(ctx, s.store.nh.GetNoOPSession(firstLogShardID), getTestUserEntry())
		require.NoError(t, err)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceAddLogShard(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := pb.ScheduleCommand{
			AddLogShard: &pb.AddLogShard{
				ShardID: 10,
			},
		}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})

		req := pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp := s.handleGetCheckerState(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		assert.Equal(t, 1, len(resp.CheckerState.LogState.Shards))
		_, ok := resp.CheckerState.LogState.Shards[10]
		assert.True(t, ok)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceBootstrapShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cfg Config
	genCfg := func() Config {
		cfg = getServiceTestConfig()
		return cfg
	}
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewServiceWithRetry(genCfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service.Close())
	}()

	cmd := pb.ScheduleCommand{
		BootstrapShard: &pb.BootstrapShard{
			ShardID:   1,
			ReplicaID: 100,
			InitialMembers: map[uint64]string{
				100: service.ID(),
			},
		},
	}
	service.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
	service.handleCommands(context.Background(), []pb.ScheduleCommand{cmd}) // Same initial members and replica ID.

	testServiceAddress := cfg.LogServiceServiceAddr()
	done := false
	for i := 0; i < 1000; i++ {
		si, ok, err := GetShardInfo("", testServiceAddress, 1)
		if err != nil || !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		done = true
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint64(100), si.ReplicaID)
		addr, ok := si.Replicas[si.ReplicaID]
		assert.True(t, ok)
		assert.Equal(t, testServiceAddress, addr)
		break
	}
	if !done {
		t.Fatalf("failed to get shard info")
	}
}

func Test_heartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	srv := &Service{
		runtime: runtime.DefaultRuntime(),
	}
	srv.cfg.HAKeeperClientConfig.ServiceAddresses = []string{"wrong address"}
	srv.heartbeat(ctx)
}

func TestCheckReplicaHealth(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// no tn shard yet.
		s.checkReplicaHealth(ctx)

		peers := map[uint64]dragonboat.Target{100: s.ID()}
		require.NoError(t, s.store.startReplica(firstLogShardID, 100, peers, false))
		require.Eventually(t, func() bool {
			_, _, ok, err := s.store.nh.GetLeaderID(firstLogShardID)
			return err == nil && ok
		}, 5*time.Second, 10*time.Millisecond)

		req := pb.Request{
			Method: pb.TN_HEARTBEAT,
			TNHeartbeat: &pb.TNStoreHeartbeat{
				UUID: uuid.NewString(),
				Shards: []pb.TNShardInfo{
					{
						ShardID:   2,
						ReplicaID: 100,
					},
				},
			},
		}
		s.handleTNHeartbeat(ctx, req)
		v2.LogServiceReplicaHealthGauge.Set(1)
		s.checkReplicaHealth(ctx)
		require.Equal(t, float64(0), testutil.ToFloat64(v2.LogServiceReplicaHealthGauge))

		req = pb.Request{
			Method: pb.LOG_HEARTBEAT,
			LogHeartbeat: &pb.LogStoreHeartbeat{
				UUID: s.ID(),
				Replicas: []pb.LogReplicaInfo{
					{
						LogShardInfo: pb.LogShardInfo{
							ShardID: firstLogShardID,
							Replicas: map[uint64]string{
								100: s.ID(),
							},
						},
					},
				},
			},
		}
		s.handleLogHeartbeat(ctx, req)
		s.checkReplicaHealth(ctx)
		require.Equal(t, float64(1), testutil.ToFloat64(v2.LogServiceReplicaHealthGauge))
	}
	runServiceTest(t, true, true, fn)
}

func TestBootstrapStartChecksLocalIdentity(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		bootstrap := func(shard, replica uint64, target string) pb.ScheduleCommand {
			return pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, Bootstrapping: true,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{UUID: target, ShardID: shard, ReplicaID: replica},
					InitialMembers: map[uint64]string{replica: target}}}
		}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{bootstrap(1, 1, s.ID()), bootstrap(2, 2, "different-store")})
		require.True(t, s.store.nh.HasNodeInfo(1, 1), "initial membership must start without a quorum read")
		require.False(t, s.store.nh.HasNodeInfo(2, 2), "a different target must not start locally")
	})
}

func TestBootstrapCannotBypassCatalogAdmission(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		s.store.catalogExecutor.enabled = true
		cmd := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
				Replica: pb.Replica{UUID: s.ID(), ShardID: 0, ReplicaID: 1}, InitialMembers: map[uint64]string{1: s.ID()}}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
		require.False(t, s.store.nh.HasNodeInfo(0, 1), "catalog start still requires its durable permit")
	})
}

func TestScheduleDefersBootstrapAfterStoppingSameShard(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		require.NoError(t, s.store.startReplica(1, 1, map[uint64]dragonboat.Target{1: s.ID()}, false))
		bootstrap := func(shard, replica uint64) pb.ScheduleCommand {
			return pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, Bootstrapping: true,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{UUID: s.ID(), ShardID: shard, ReplicaID: replica},
					InitialMembers: map[uint64]string{replica: s.ID()}}}
		}
		kill := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.KillZombie, Replica: pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 1}}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{kill, bootstrap(1, 2), bootstrap(2, 5)})
		require.False(t, s.store.nh.HasNodeInfo(1, 2), "bootstrap must not immediately replace a stopped replica")
		require.True(t, s.store.nh.HasNodeInfo(2, 5), "an unrelated bootstrap must proceed")
		s.handleCommands(context.Background(), nil)
		require.False(t, s.store.nh.HasNodeInfo(1, 2), "an empty batch cannot replay a skipped start")
	})
}

func TestStartConflictingWithBatchStopNeedsRedelivery(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		require.NoError(t, s.store.startReplica(1, 1, map[uint64]dragonboat.Target{1: s.ID()}, false))
		start := func(shard, replica uint64) pb.ScheduleCommand {
			return pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{UUID: s.ID(), ShardID: shard, ReplicaID: replica}}}
		}
		kill := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.KillZombie, Replica: pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 1}}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{kill, start(1, 2), start(2, 5)})
		require.False(t, s.store.nh.HasNodeInfo(1, 2), "the stopped shard must not start a new instance in the same batch")
		require.True(t, s.store.nh.HasNodeInfo(2, 5), "an independent start must still run")
		s.handleCommands(context.Background(), nil)
		require.False(t, s.store.nh.HasNodeInfo(1, 2), "a later empty heartbeat must not replay the skipped start")
	})
}

func TestCanceledServiceDoesNotStartDeliveredReplicas(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 1}}}
		bootstrap := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			BootstrapShard: &pb.BootstrapShard{ShardID: 2, ReplicaID: 2, InitialMembers: map[uint64]string{2: s.ID()}}}
		s.handleCommands(ctx, []pb.ScheduleCommand{start, bootstrap})
		require.False(t, s.store.nh.HasNodeInfo(1, 1))
		require.False(t, s.store.nh.HasNodeInfo(2, 2))
	})
}

func TestMembershipBatchPreservesIndependentWork(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		s.haClient = nil
		commands := make([]pb.ScheduleCommand, 0, 2)
		for _, shardID := range []uint64{2, 3} {
			require.NoError(t, s.store.startReplica(shardID, 1, map[uint64]dragonboat.Target{1: s.ID()}, false))
			var membership *dragonboat.Membership
			require.Eventually(t, func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				defer cancel()
				var err error
				membership, err = s.store.nh.SyncGetShardMembership(ctx, shardID)
				return err == nil && membership.Nodes[1] == s.ID()
			}, 10*time.Second, 30*time.Millisecond)
			commands = append(commands, pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.AddNonVotingReplica,
					Replica: pb.Replica{UUID: uuid.NewString(), ShardID: shardID, ReplicaID: 2, Epoch: membership.ConfigChangeID}}})
		}
		readMembership := func(shardID uint64) *dragonboat.Membership {
			t.Helper()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			membership, err := s.store.nh.SyncGetShardMembership(ctx, shardID)
			require.NoError(t, err)
			return membership
		}

		wrongRecipient := commands[0]
		wrongRecipient.UUID = "another-store"
		s.handleCommands(context.Background(), []pb.ScheduleCommand{wrongRecipient})
		require.Empty(t, readMembership(2).NonVotings)

		ctx, cancel := context.WithCancel(context.Background())
		logger := zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, commands[0].LogString()) {
				cancel()
			}
			return nil
		}))
		s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), logger)
		s.handleCommands(ctx, commands[:1])
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.Empty(t, readMembership(2).NonVotings)

		stale := commands[0]
		change := *stale.ConfigChange
		change.Replica.Epoch++
		stale.ConfigChange = &change
		s.handleCommands(context.Background(), []pb.ScheduleCommand{stale})
		require.Empty(t, readMembership(2).NonVotings, "Raft must reject a different configuration index")

		sawStart := false
		logger = zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, commands[0].LogString()) {
				sawStart = s.store.nh.HasNodeInfo(4, 1)
			}
			return nil
		}))
		s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), logger)
		start := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
				Replica: pb.Replica{UUID: s.ID(), ShardID: 4, ReplicaID: 1}, InitialMembers: map[uint64]string{1: s.ID()}}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{commands[0], start, commands[1]})
		require.True(t, sawStart, "an independent start must precede membership work")
		for _, cmd := range commands {
			membership := readMembership(cmd.ConfigChange.Replica.ShardID)
			require.Equal(t, cmd.ConfigChange.Replica.UUID, membership.NonVotings[2])
			require.Greater(t, membership.ConfigChangeID, cmd.ConfigChange.Replica.Epoch)
		}
	})
}

func TestScheduleStartCanceledBeforeDispatch(t *testing.T) {
	cases := []struct {
		name          string
		change        pb.ConfigChangeType
		bootstrapping bool
	}{
		{name: "initial voter", change: pb.StartReplica, bootstrapping: true},
		{name: "configured voter", change: pb.StartReplica},
		{name: "configured non-voter", change: pb.StartNonVotingReplica},
	}
	for _, tc := range cases {
		for _, atDispatch := range []bool{false, true} {
			name := tc.name + "/already canceled"
			if atDispatch {
				name = tc.name + "/canceled before dispatch"
			}
			t.Run(name, func(t *testing.T) {
				runServiceTest(t, false, false, func(t *testing.T, s *Service) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					// Delivered starts do not need another HAKeeper read.
					s.haClient = nil
					if atDispatch {
						logger := zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
							if strings.Contains(entry.Message, " applying cmd: ") {
								cancel()
							}
							return nil
						}))
						s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), logger)
					} else {
						cancel()
					}
					cmd := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, Bootstrapping: tc.bootstrapping,
						ConfigChange: &pb.ConfigChange{ChangeType: tc.change,
							Replica: pb.Replica{UUID: s.ID(), ShardID: 91, ReplicaID: 1}}}
					if tc.bootstrapping {
						cmd.ConfigChange.InitialMembers = map[uint64]string{1: s.ID()}
					}
					s.handleCommands(ctx, []pb.ScheduleCommand{cmd})
					require.ErrorIs(t, ctx.Err(), context.Canceled)
					require.Empty(t, s.store.getShards(), "canceled admission must not create replica metadata")
					// A later delivery, not a second local queue, retries the work.
					s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
					shards := s.store.getShards()
					require.Len(t, shards, 1)
					require.Equal(t, uint64(91), shards[0].ShardID)
					require.Equal(t, uint64(1), shards[0].ReplicaID)
					require.Equal(t, tc.change == pb.StartNonVotingReplica, shards[0].NonVoting)
				})
			})
		}
	}
}

type cancelAfterHeartbeatClient struct {
	LogHAKeeperClient
	cancel   context.CancelFunc
	commands []pb.ScheduleCommand
	sent     bool
}

func (c *cancelAfterHeartbeatClient) Close() error { return nil }

func (c *cancelAfterHeartbeatClient) SendLogHeartbeat(context.Context, pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	c.sent = true
	c.cancel()
	return pb.CommandBatch{Commands: c.commands}, nil
}

func TestScheduleCancellationStillProcessesLaterStop(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		name := "start"
		if dynamic {
			name = "dynamic bootstrap"
		}
		t.Run(name, func(t *testing.T) {
			runServiceTest(t, false, true, func(t *testing.T, s *Service) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				logger := zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
					if strings.Contains(entry.Message, " applying cmd: ") {
						cancel()
					}
					return nil
				}))
				s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), logger)
				start := pb.ScheduleCommand{UUID: s.ID(), ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
					Replica: pb.Replica{UUID: s.ID(), ShardID: 2, ReplicaID: 2}}}
				if dynamic {
					start.ConfigChange = nil
					start.BootstrapShard = &pb.BootstrapShard{ShardID: 2, ReplicaID: 2, InitialMembers: map[uint64]string{2: s.ID()}}
				}
				stop := pb.ScheduleCommand{UUID: s.ID(), ConfigChange: &pb.ConfigChange{ChangeType: pb.StopReplica,
					Replica: pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 1}}}
				present := func() bool {
					for _, replica := range s.store.nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true}).ShardInfoList {
						if replica.ShardID == 1 && replica.ReplicaID == 1 {
							return true
						}
					}
					return false
				}
				require.True(t, present())
				if dynamic {
					client := &cancelAfterHeartbeatClient{cancel: cancel, commands: []pb.ScheduleCommand{start, stop}}
					s.haClient = client
					s.heartbeat(ctx)
					require.True(t, client.sent, "the late heartbeat returned the batch")
				} else {
					s.handleCommands(ctx, []pb.ScheduleCommand{start, stop})
				}
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.False(t, s.store.nh.HasNodeInfo(2, 2), "canceled admission must not create a replica")
				require.Eventually(t, func() bool { return !present() }, 5*time.Second, 20*time.Millisecond,
					"the already delivered independent Stop must still run")
			})
		})
	}
}
