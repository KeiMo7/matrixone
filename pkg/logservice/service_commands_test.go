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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

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
	for _, tc := range []struct {
		name       string
		dynamic    bool
		logMessage string
	}{
		{name: "start", logMessage: " applying cmd: "},
		{name: "start debug", logMessage: "applying schedule command:"},
		{name: "dynamic bootstrap", dynamic: true, logMessage: " applying cmd: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runServiceTest(t, false, true, func(t *testing.T, s *Service) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				logger := zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
					if strings.Contains(entry.Message, tc.logMessage) {
						cancel()
					}
					return nil
				}))
				s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), logger)
				start := pb.ScheduleCommand{UUID: s.ID(), ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
					Replica: pb.Replica{UUID: s.ID(), ShardID: 2, ReplicaID: 2}}}
				if tc.dynamic {
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
				if tc.dynamic {
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
	t.Skip() // pls re-open after https://github.com/matrixorigin/matrixone/pull/19025
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
		s.handleStopReplica(cmd)
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

func TestLegacyShutdownDoesNotPreventOtherCommands(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		shutdown := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: s.ID()}}
		stop := pb.ScheduleCommand{ConfigChange: &pb.ConfigChange{ChangeType: pb.StopReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 1}}}
		mustHaveReplica(t, s.store, 1, 1)
		s.handleCommands(context.Background(), []pb.ScheduleCommand{shutdown, stop})
		require.False(t, hasReplica(s.store, 1, 1))
	})
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
	service.haClient = &scheduleStateClient{states: []pb.CheckerState{{State: pb.HAKeeperRunning,
		LogState: pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: {ShardID: 1}}}}}}

	cmd := pb.ScheduleCommand{
		UUID: service.ID(),
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		// no tn shard yet.
		s.checkReplicaHealth(ctx)

		req := pb.Request{
			Method: pb.TN_HEARTBEAT,
			TNHeartbeat: &pb.TNStoreHeartbeat{
				UUID: uuid.NewString(),
				Shards: []pb.TNShardInfo{
					{
						ShardID:   1,
						ReplicaID: 100,
					},
				},
			},
		}
		s.handleTNHeartbeat(ctx, req)
		s.checkReplicaHealth(ctx)

		req = pb.Request{
			Method: pb.LOG_HEARTBEAT,
			LogHeartbeat: &pb.LogStoreHeartbeat{
				UUID: s.ID(),
			},
		}
		s.handleLogHeartbeat(ctx, req)
		s.checkReplicaHealth(ctx)
	}
	runServiceTest(t, true, true, fn)
}

type scheduleStateClient struct {
	LogHAKeeperClient
	states          []pb.CheckerState
	calls           int
	waitForDeadline bool
}

func (c *scheduleStateClient) Close() error { return nil }
func (c *scheduleStateClient) GetLogScheduleState(ctx context.Context, _ uint64, _ string) (pb.CheckerState, error) {
	c.calls++
	if c.waitForDeadline {
		<-ctx.Done()
		return pb.CheckerState{}, ctx.Err()
	}
	index := min(c.calls-1, len(c.states)-1)
	return c.states[index], nil
}

func TestDeliveredStartRunsBeforeMembershipValidation(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		client := &startBeforeReadClient{store: s.store}
		s.haClient = client
		change := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.AddNonVotingReplica,
				Replica: pb.Replica{UUID: "other", ShardID: 1, ReplicaID: 2}}}
		start := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
				Replica: pb.Replica{UUID: s.ID(), ShardID: 2, ReplicaID: 2}}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{change, start})
		require.True(t, client.sawStart, "the independent Start must precede the membership read")
		require.Equal(t, 1, client.calls)
		s.handleCommands(context.Background(), nil)
		require.Equal(t, 1, client.calls, "an unsuccessful read must not retain the command")
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

type startBeforeReadClient struct {
	LogHAKeeperClient
	store    *store
	sawStart bool
	calls    int
}

func (c *startBeforeReadClient) Close() error { return nil }
func (c *startBeforeReadClient) GetLogScheduleState(context.Context, uint64, string) (pb.CheckerState, error) {
	c.calls++
	c.sawStart = c.store.nh.HasNodeInfo(2, 2)
	return pb.CheckerState{}, context.DeadlineExceeded
}

func TestBootstrapCommandsDoNotDependOnScheduleValidation(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		client := &scheduleStateClient{waitForDeadline: true}
		s.haClient = client
		initial := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService, Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
				Replica:        pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 1},
				InitialMembers: map[uint64]string{1: s.ID()}}}
		stop := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StopReplica,
				Replica: pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 99}}}
		dynamic := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			BootstrapShard: &pb.BootstrapShard{ShardID: 2, ReplicaID: 2, InitialMembers: map[uint64]string{2: s.ID()}}}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{stop, initial, dynamic})
		require.Zero(t, client.calls, "bootstrap must not be lost to a validation read")
		require.True(t, s.store.nh.HasNodeInfo(1, 1))
		require.True(t, s.store.nh.HasNodeInfo(2, 2))
	})
}

type slowScheduleStateClient struct {
	LogHAKeeperClient
	calls   []uint64
	healthy pb.CheckerState
}

func (c *slowScheduleStateClient) Close() error { return nil }
func (c *slowScheduleStateClient) GetLogScheduleState(ctx context.Context, shard uint64, _ string) (pb.CheckerState, error) {
	c.calls = append(c.calls, shard)
	if shard == 1 {
		timer := time.NewTimer(1100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return pb.CheckerState{}, ctx.Err()
		case <-timer.C:
		}
	}
	return c.healthy, nil
}

func TestMembershipAdmissionRotatesWithoutRetainingCommands(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		require.NoError(t, s.store.startReplica(2, 1, map[uint64]dragonboat.Target{1: s.ID()}, false))
		target := uuid.NewString()
		var current *dragonboat.Membership
		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			var err error
			current, err = s.store.nh.SyncGetShardMembership(ctx, 2)
			return err == nil && len(current.Nodes) == 1
		}, 10*time.Second, 30*time.Millisecond)
		shard := pb.LogShardInfo{ShardID: 2, Epoch: current.ConfigChangeID, Replicas: map[uint64]string{1: s.ID()}}
		client := &slowScheduleStateClient{healthy: pb.CheckerState{Tick: 1, LogState: pb.LogState{
			Shards: map[uint64]pb.LogShardInfo{2: shard},
			Stores: map[string]pb.LogStoreInfo{
				s.ID(): {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				target: {Tick: 1},
			},
		}}}
		s.haClient = client
		change := func(shard uint64) pb.ScheduleCommand {
			return pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.AddNonVotingReplica,
					Replica: pb.Replica{UUID: target, ShardID: shard, ReplicaID: 9, Epoch: current.ConfigChangeID}}}
		}
		s.handleCommands(context.Background(), []pb.ScheduleCommand{change(1), change(2)})
		require.Equal(t, []uint64{1}, client.calls, "one slow read exhausts this batch's admission budget")
		s.handleCommands(context.Background(), nil)
		require.Equal(t, []uint64{1}, client.calls, "a later empty heartbeat must not replay commands")
		s.handleCommands(context.Background(), []pb.ScheduleCommand{change(2), change(1)})
		require.GreaterOrEqual(t, len(client.calls), 2)
		require.Equal(t, uint64(2), client.calls[1], "a redelivered healthy shard must not be starved by batch order")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		actual, err := s.store.nh.SyncGetShardMembership(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, target, actual.NonVotings[9], "the healthy shard must actually commit the membership change")
	})
}

type blockingScheduleStateClient struct {
	LogHAKeeperClient
	started chan struct{}
}

func (c *blockingScheduleStateClient) Close() error { return nil }
func (c *blockingScheduleStateClient) GetLogScheduleState(ctx context.Context, _ uint64, _ string) (pb.CheckerState, error) {
	close(c.started)
	<-ctx.Done()
	return pb.CheckerState{}, ctx.Err()
}

func TestScheduleValidationStopsOnServiceCancellation(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		client := &blockingScheduleStateClient{started: make(chan struct{})}
		s.haClient = client
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleCommands(ctx, []pb.ScheduleCommand{{UUID: s.ID(), ServiceType: pb.LogService,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.AddReplica, Replica: pb.Replica{UUID: s.ID(), ShardID: 1, ReplicaID: 1}}}})
		}()
		select {
		case <-client.started:
		case <-time.After(5 * time.Second):
			t.Fatal("validation did not start")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("validation did not stop after cancellation")
		}
	})
}

func TestMembershipCancellationAfterValidation(t *testing.T) {
	runServiceTest(t, false, false, func(t *testing.T, s *Service) {
		require.NoError(t, s.store.startReplica(2, 1, map[uint64]dragonboat.Target{1: s.ID()}, false))
		var current *dragonboat.Membership
		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			var err error
			current, err = s.store.nh.SyncGetShardMembership(ctx, 2)
			return err == nil && len(current.Nodes) == 1
		}, 10*time.Second, 30*time.Millisecond)
		target := uuid.NewString()
		shard := pb.LogShardInfo{ShardID: 2, Epoch: current.ConfigChangeID, Replicas: map[uint64]string{1: s.ID()}}
		client := &scheduleStateClient{states: []pb.CheckerState{{Tick: 1, LogState: pb.LogState{
			Shards: map[uint64]pb.LogShardInfo{2: shard},
			Stores: map[string]pb.LogStoreInfo{
				s.ID(): {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				target: {Tick: 1},
			},
		}}}}
		s.haClient = client
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger := zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, " applying cmd: ") {
				cancel()
			}
			return nil
		}))
		s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), logger)
		cmd := pb.ScheduleCommand{UUID: s.ID(), ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.AddNonVotingReplica,
				Replica: pb.Replica{UUID: target, ShardID: 2, ReplicaID: 9, Epoch: current.ConfigChangeID}}}
		readMembership := func() *dragonboat.Membership {
			t.Helper()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			membership, err := s.store.nh.SyncGetShardMembership(ctx, 2)
			require.NoError(t, err)
			return membership
		}
		s.handleCommands(ctx, []pb.ScheduleCommand{cmd})
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.Equal(t, 1, client.calls, "the command reached authorization before cancellation")
		actual := readMembership()
		require.Equal(t, current.ConfigChangeID, actual.ConfigChangeID)
		require.NotContains(t, actual.NonVotings, uint64(9))

		s.runtime = runtime.NewRuntime(metadata.ServiceType_LOG, s.ID(), zap.NewNop())
		s.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
		actual = readMembership()
		require.Equal(t, target, actual.NonVotings[9], "the same command must succeed while active")
		require.Greater(t, actual.ConfigChangeID, current.ConfigChangeID)
	})
}
