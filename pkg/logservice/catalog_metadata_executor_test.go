// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"errors"
	"maps"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestCatalogExecutorErrorClass(t *testing.T) {
	require.True(t, moerr.IsMoErrCode(catalogExecutionError(), moerr.ErrInvalidState))
}

func TestCatalogExecutorLatch(t *testing.T) {
	cfg := Config{UUID: "store", DataDir: "catalog-test", FS: vfs.NewMem()}
	t.Cleanup(func() { vfs.ReportLeakedFD(cfg.FS, t) })
	s := &store{cfg: cfg}
	require.NoError(t, s.loadCatalogExecutor())
	require.False(t, s.catalogExecutor.enabled)
	found, err := exist(cfg.FS.PathJoin(cfg.DataDir, catalogExecutorFilename), cfg.FS)
	require.NoError(t, err)
	require.False(t, found)
	cfg.CatalogMetadataMaintenance = true
	s = &store{cfg: cfg}
	require.NoError(t, s.loadCatalogExecutor())
	require.True(t, s.catalogExecutor.enabled)
	cfg.CatalogMetadataMaintenance = false
	restarted := &store{cfg: cfg}
	require.NoError(t, restarted.loadCatalogExecutor())
	require.True(t, restarted.catalogExecutor.enabled)
	cfg.UUID = "different-store"
	require.Error(t, (&store{cfg: cfg}).loadCatalogExecutor())
}

func TestCatalogExecutorLegacyGates(t *testing.T) {
	s := &store{}
	s.catalogExecutor.enabled = true
	// A nil NodeHost makes any accidental execution immediately visible.
	require.Error(t, s.startHAKeeperReplica(1, nil, true))
	require.Error(t, s.startHAKeeperNonVotingReplica(1, nil, true))
	require.Error(t, s.addReplica(hakeeper.DefaultHAKeeperShardID, 1, "target", 7))
	require.Error(t, s.addNonVotingReplica(hakeeper.DefaultHAKeeperShardID, 1, "target", 7))
	require.Error(t, s.removeReplica(hakeeper.DefaultHAKeeperShardID, 1, 7))
	require.Error(t, s.stopReplica(hakeeper.DefaultHAKeeperShardID, 1))
	service := &Service{store: s, runtime: runtime.DefaultRuntime()}
	for _, change := range []pb.ConfigChangeType{pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica, pb.StartReplica, pb.StartNonVotingReplica, pb.StopReplica, pb.StopNonVotingReplica, pb.KillZombie} {
		cmd := pb.ScheduleCommand{ConfigChange: &pb.ConfigChange{ChangeType: change, Replica: pb.Replica{ShardID: hakeeper.DefaultHAKeeperShardID, ReplicaID: 1}}}
		require.Error(t, s.executeCatalogCommand(context.Background(), cmd))
	}
	require.NoError(t, service.BootstrapHAKeeper(context.Background(), Config{}))
}

type catalogFailCreateFS struct{ vfs.FS }

func (fs catalogFailCreateFS) Create(string) (vfs.File, error) {
	return nil, errors.New("injected metadata write failure")
}

func TestCatalogExecutorFailedPersistence(t *testing.T) {
	cfg := Config{UUID: "store", DataDir: "catalog-test", FS: vfs.NewMem()}
	t.Cleanup(func() { vfs.ReportLeakedFD(cfg.FS, t) })
	cfg.CatalogMetadataMaintenance = true
	s := &store{cfg: cfg}
	require.NoError(t, s.loadCatalogExecutor())
	s.mu.metadata.Incarnation = "inc"
	s.cfg.FS = catalogFailCreateFS{cfg.FS}
	p := pb.CatalogMetadataStartPermit{Token: 3, ReplicaID: 1, UUID: cfg.UUID, StoreIncarnation: "inc"}
	require.ErrorContains(t, s.executeCatalogStart(context.Background(), p), "injected")
	require.True(t, s.catalogExecutor.failed)
	require.Zero(t, s.catalogExecutor.record.Permit.Token)
	require.Nil(t, s.catalogStartHeartbeat())
	// A later successful filesystem operation cannot clear uncertain persistence.
	s.cfg.FS = cfg.FS
	require.Error(t, s.executeCatalogStart(context.Background(), p))
}

func TestCatalogExecutorStalePermit(t *testing.T) {
	cfg := Config{UUID: "store", DataDir: "catalog-test", FS: vfs.NewMem()}
	t.Cleanup(func() { vfs.ReportLeakedFD(cfg.FS, t) })
	cfg.CatalogMetadataMaintenance = true
	s := &store{cfg: cfg}
	require.NoError(t, s.loadCatalogExecutor())
	s.mu.metadata.Incarnation = "inc"
	p := pb.CatalogMetadataStartPermit{Token: 3, ReplicaID: 1, UUID: cfg.UUID, StoreIncarnation: "inc"}
	r := s.catalogExecutor.record
	r.Permit, r.State = p, "REVOKED"
	require.NoError(t, s.saveCatalogExecutor(r))
	restarted := &store{cfg: cfg}
	require.NoError(t, restarted.loadCatalogExecutor())
	restarted.mu.metadata.Incarnation = "inc"
	require.Error(t, restarted.executeCatalogStart(context.Background(), p))
	p.Revoked = true
	require.NoError(t, restarted.executeCatalogStart(context.Background(), p))
	require.True(t, restarted.catalogStartHeartbeat().Revoked)
	for _, alter := range []func(*pb.CatalogMetadataStartPermit){
		func(p *pb.CatalogMetadataStartPermit) { p.Token-- },
		func(p *pb.CatalogMetadataStartPermit) { p.ReplicaID++ },
		func(p *pb.CatalogMetadataStartPermit) { p.UUID = "wrong" },
		func(p *pb.CatalogMetadataStartPermit) { p.StoreIncarnation = "new" },
		func(p *pb.CatalogMetadataStartPermit) { p.NonVoting = true },
		func(p *pb.CatalogMetadataStartPermit) { p.Completed = true },
	} {
		copy := p
		alter(&copy)
		require.Error(t, restarted.executeCatalogStart(context.Background(), copy))
	}
	restarted.mu.metadata.Incarnation = "new"
	require.Nil(t, restarted.catalogStartHeartbeat())
	require.Error(t, restarted.executeCatalogStart(context.Background(), p))
}

// One existing single-node fixture proves actual Dragonboat start/stop,
// heartbeat readiness, on-disk restart, and concurrent late delivery. ReadIndex
// is the readiness barrier; no sleep or timing oracle is used.
type catalogLoopContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *catalogLoopContext) Err() error {
	err := c.Context.Err()
	if err == nil {
		c.once.Do(func() { close(c.observed) })
	}
	return err
}

func TestCatalogCommandCanceledBeforeStartAdmission(t *testing.T) {
	cfg := getStoreTestConfig()
	cfg.DisableWorkers = true
	cfg.CatalogMetadataMaintenance = true
	t.Cleanup(func() { vfs.ReportLeakedFD(cfg.FS, t) })
	s, err := getTestStore(func() Config { return cfg }, false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.close()) })
	require.NoError(t, s.loadMetadata())
	permit := pb.CatalogMetadataStartPermit{Token: 1, ReplicaID: 3, UUID: s.id(), StoreIncarnation: s.getStoreIncarnation()}
	require.NotEmpty(t, permit.StoreIncarnation)
	cmd := pb.ScheduleCommand{UUID: s.id(), ServiceType: pb.LogService, CatalogMetadataStart: &permit,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
			Replica: pb.Replica{ShardID: hakeeper.DefaultHAKeeperShardID, ReplicaID: permit.ReplicaID, UUID: permit.UUID}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = s.executeCatalogCommand(ctx, cmd)
	require.Zero(t, catalogReadRecord(t, s).Permit.Token, "canceled admission must not persist STARTING")
	require.Empty(t, s.getShards())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, s.catalogStartHeartbeat())

	// Select a queued command while the supervisor is held, then cancel the
	// worker before the supervisor can accept the start.
	worker, stopWorker := context.WithCancel(context.Background())
	observed := &catalogLoopContext{Context: worker, observed: make(chan struct{})}
	service := &Service{store: s, runtime: runtime.DefaultRuntime()}
	s.catalogExecutor.Lock()
	var unlock sync.Once
	release := func() { unlock.Do(s.catalogExecutor.Unlock) }
	t.Cleanup(release)
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.handleCommands(observed, []pb.ScheduleCommand{cmd})
	}()
	select {
	case <-observed.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog command did not enter the worker loop")
	}
	stopWorker()
	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog command did not finish")
	}
	require.Zero(t, catalogReadRecord(t, s).Permit.Token)
	require.Empty(t, s.getShards())

	// HAKeeper reoffers the same durable permit after the lost delivery.
	service.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
	require.Equal(t, permit.Token, catalogReadRecord(t, s).Permit.Token)
	require.True(t, s.nh.HasNodeInfo(hakeeper.DefaultHAKeeperShardID, permit.ReplicaID))

	revoke := permit
	revoke.RevocationPending = true
	cmd.CatalogMetadataStart = &revoke
	cmd.ConfigChange.ChangeType = pb.StopReplica
	service.handleCommands(ctx, []pb.ScheduleCommand{cmd})
	require.Equal(t, "REVOKED", catalogReadRecord(t, s).State)
}

func TestCatalogStartCompletesAfterAdmissionCancellation(t *testing.T) {
	s, permit := catalogTestSupervisor(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := &catalogTestReplica{ready: true, startHook: func() error {
		cancel()
		return nil
	}}
	require.NoError(t, s.executeCatalogStartWith(ctx, permit, host))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, 1, host.starts)
	require.Equal(t, "STARTED", catalogReadRecord(t, s).State)
	invalid := permit
	invalid.ReplicaID++
	err := s.executeCatalogStartWith(ctx, invalid, host)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.Equal(t, 1, host.starts)
}

func TestCatalogExecutorLifecycle(t *testing.T) {
	cfg := getStoreTestConfig()
	cfg.DisableWorkers = true
	t.Cleanup(func() { vfs.ReportLeakedFD(cfg.FS, t) })
	s, err := getTestStore(func() Config { return cfg }, false, nil)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, s.close())
		}
	})
	require.NoError(t, s.loadMetadata())
	require.NoError(t, s.startHAKeeperReplica(1, map[uint64]string{1: s.id()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err = s.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.CatalogMetadataStateQuery{})
	require.NoError(t, err)
	require.Nil(t, s.getHeartbeatMessage().CatalogMetadataCapabilities)
	// Simulate the maintenance restart by closing the pre-protocol executor.
	require.NoError(t, s.close())
	closed = true
	cfg.CatalogMetadataMaintenance = true
	s, err = getTestStore(func() Config { return cfg }, false, nil)
	require.NoError(t, err)
	closed = false
	require.NoError(t, s.loadMetadata())
	require.NoError(t, s.startReplicas(ctx))
	_, err = s.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.CatalogMetadataStateQuery{})
	require.NoError(t, err)
	proposal := func(req pb.CatalogMetadataRequest) uint64 {
		t.Helper()
		req.Version = 1
		cmd, err := hakeeper.GetCatalogMetadataRequestCmd(req)
		require.NoError(t, err)
		result, err := s.propose(ctx, s.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID), cmd)
		require.NoError(t, err)
		require.Equal(t, hakeeper.CatalogMetadataApplied, result.Value)
		require.Len(t, result.Data, 8)
		return binaryEnc.Uint64(result.Data)
	}
	// Enter maintenance through real replicated state and obtain a real permit.
	fixtureHeartbeat := s.getHeartbeatMessage()
	_, err = s.addLogStoreHeartbeat(ctx, fixtureHeartbeat)
	require.NoError(t, err)
	proposal(pb.CatalogMetadataRequest{Action: pb.CATALOG_ACTION_ENABLE_MAINTENANCE})
	p := pb.CatalogMetadataStartPermit{ReplicaID: 1, UUID: s.id(), StoreIncarnation: s.getStoreIncarnation()}
	p.Token = proposal(pb.CatalogMetadataRequest{Action: pb.CATALOG_ACTION_GRANT_START, StartPermit: &p})
	cmd := pb.ScheduleCommand{ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: hakeeper.DefaultHAKeeperShardID, ReplicaID: 1, UUID: s.id()}}, CatalogMetadataStart: &p}
	service := &Service{store: s, runtime: runtime.DefaultRuntime()}
	service.handleCommands(context.Background(), []pb.ScheduleCommand{cmd})
	hb := s.getHeartbeatMessage()
	require.Equal(t, uint64(1), hb.CatalogMetadataCapabilities.HAKeeperBarrierProtocol)
	require.Zero(t, hb.CatalogMetadataCapabilities.ViewDependencyProtocol)
	require.NotNil(t, hb.CatalogMetadataStartResult)
	require.True(t, hb.CatalogMetadataStartResult.Completed)
	before, err := s.nh.SyncGetShardMembership(ctx, hakeeper.DefaultHAKeeperShardID)
	require.NoError(t, err)
	require.NoError(t, s.executeCatalogStart(context.Background(), p))
	after, err := s.nh.SyncGetShardMembership(ctx, hakeeper.DefaultHAKeeperShardID)
	require.NoError(t, err)
	require.Equal(t, before.ConfigChangeID, after.ConfigChangeID)
	_, err = s.addLogStoreHeartbeat(ctx, hb)
	require.NoError(t, err)
	barrier, err := s.catalogBarrierState(ctx)
	require.NoError(t, err)
	require.Len(t, barrier.Arbitration.StartPermits, 1)
	require.True(t, barrier.Arbitration.StartPermits[0].Completed)
	// A real add whose transport outcome is lost is completed only by Raft
	// readback; the same reservation then permits a real removal.
	target := "00000000-0000-0000-0000-000000000002"
	_, err = s.addLogStoreHeartbeat(ctx, pb.LogStoreHeartbeat{UUID: target, StoreIncarnation: "target-inc", CatalogMetadataCapabilities: &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: 1}})
	require.NoError(t, err)
	op := &pb.CatalogMetadataMembershipOperation{ConfigChangeIndex: after.ConfigChangeID, ReplicaID: 2, UUID: target, StoreIncarnation: "target-inc", ChangeType: pb.AddNonVotingReplica}
	proposal(pb.CatalogMetadataRequest{Action: pb.CATALOG_ACTION_RESERVE_MEMBERSHIP, Membership: op})
	barrier, err = s.catalogBarrierState(ctx)
	require.NoError(t, err)
	reserved := barrier.Arbitration.Reservation
	canceled, cancelOperation := context.WithCancel(ctx)
	cancelOperation()
	require.ErrorIs(t, s.reconcileCatalogMembership(canceled, reserved), context.Canceled)
	require.NoError(t, s.nh.SyncRequestAddNonVoting(ctx, hakeeper.DefaultHAKeeperShardID, 2, target, after.ConfigChangeID))
	require.NoError(t, s.reconcileCatalogMembership(ctx, reserved))
	barrier, err = s.catalogBarrierState(ctx)
	require.NoError(t, err)
	require.Nil(t, barrier.Arbitration.Reservation)
	_, err = s.addLogStoreHeartbeat(ctx, s.getHeartbeatMessage())
	require.NoError(t, err)
	current, err := s.catalogMembership(ctx)
	require.NoError(t, err)
	op.ChangeType, op.ConfigChangeIndex = pb.RemoveNonVotingReplica, current.ConfigChangeID
	proposal(pb.CatalogMetadataRequest{Action: pb.CATALOG_ACTION_RESERVE_MEMBERSHIP, Membership: op})
	barrier, err = s.catalogBarrierState(ctx)
	require.NoError(t, err)
	require.NoError(t, s.executeCatalogCommand(ctx, pb.ScheduleCommand{ConfigChange: &pb.ConfigChange{ChangeType: pb.RemoveNonVotingReplica, Replica: pb.Replica{ShardID: hakeeper.DefaultHAKeeperShardID, ReplicaID: 2, UUID: target, Epoch: current.ConfigChangeID}}, CatalogMetadataMembership: barrier.Arbitration.Reservation}))
	barrier, err = s.catalogBarrierState(ctx)
	require.NoError(t, err)
	require.Nil(t, barrier.Arbitration.Reservation)

	require.NoError(t, s.close())
	closed = true
	// Removing the config cannot unset the latch, and lost acks retain the token.
	cfg.CatalogMetadataMaintenance = false
	s, err = getTestStore(func() Config { return cfg }, false, nil)
	require.NoError(t, err)
	closed = false
	require.NoError(t, s.loadMetadata())
	require.NoError(t, s.startReplicas(ctx))
	_, err = s.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.CatalogMetadataStateQuery{})
	require.NoError(t, err)
	hb = s.getHeartbeatMessage()
	require.Equal(t, p.Token, hb.CatalogMetadataStartResult.Token)
	require.True(t, hb.CatalogMetadataStartResult.Completed)
	nid := nodeID{shardID: hakeeper.DefaultHAKeeperShardID, replicaID: p.ReplicaID}
	testPrepareSnapshot(t, s.snapshotMgr, nid, snapshotIndex(123))
	require.NoError(t, s.snapshotMgr.Init(nid.shardID, nid.replicaID))
	exportPath := s.snapshotMgr.exportPath(nid.shardID, nid.replicaID)
	revoke := p
	revoke.RevocationPending = true
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); <-start; _ = s.executeCatalogStart(context.Background(), p) }()
	go func() { defer wg.Done(); <-start; errs <- s.executeCatalogStart(context.Background(), revoke) }()
	close(start)
	wg.Wait()
	require.NoError(t, <-errs)
	require.False(t, s.catalogReplicaPresent(p, false))
	_, err = s.cfg.FS.Stat(exportPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Error(t, s.executeCatalogStart(context.Background(), p))
	require.True(t, s.getHeartbeatMessage().CatalogMetadataStartResult.Revoked)
	require.NoError(t, s.close())
	closed = true
	s, err = getTestStore(func() Config { return cfg }, false, nil)
	require.NoError(t, err)
	closed = false
	require.NoError(t, s.loadMetadata())
	require.NoError(t, s.startReplicas(ctx))
	require.False(t, s.catalogReplicaPresent(p, false))
	require.True(t, s.getHeartbeatMessage().CatalogMetadataStartResult.Revoked)
	// Ordinary data shards retain their old bootstrap behavior in maintenance.
	require.NoError(t, s.startReplica(10, 2, map[uint64]string{2: s.id()}, false))
}

func TestCatalogLeaderReconcilesWithReportingQuorum(t *testing.T) {
	runHAKeeperClusterTest(t, func(t *testing.T, services []*Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		const shard = hakeeper.DefaultHAKeeperShardID
		for _, service := range services {
			service.store.cfg.CatalogMetadataMaintenance = true
			require.NoError(t, service.store.loadCatalogExecutor())
		}

		var leader *store
		var reports [3]pb.LogStoreHeartbeat
		require.Eventually(t, func() bool {
			leader = nil
			for i, service := range services[:len(reports)] {
				s := service.store
				reports[i] = s.getHeartbeatMessage()
				if len(reports[i].Replicas) != 1 || len(reports[i].Replicas[0].Replicas) != len(reports) {
					return false
				}
				if isLeader, _, err := s.isLeaderHAKeeper(); err == nil && isLeader {
					leader = s
				}
			}
			return leader != nil
		}, 10*time.Second, 20*time.Millisecond)
		before, err := leader.catalogMembership(ctx)
		require.NoError(t, err)
		for _, hb := range reports {
			require.Equal(t, before.ConfigChangeID, hb.Replicas[0].Epoch)
			_, err = leader.addLogStoreHeartbeat(ctx, hb)
			require.NoError(t, err)
		}
		propose := func(req pb.CatalogMetadataRequest) uint64 {
			t.Helper()
			req.Version = 1
			cmd, err := hakeeper.GetCatalogMetadataRequestCmd(req)
			require.NoError(t, err)
			result, err := leader.propose(ctx, leader.nh.GetNoOPSession(shard), cmd)
			require.NoError(t, err)
			require.Equal(t, hakeeper.CatalogMetadataApplied, result.Value)
			return binaryEnc.Uint64(result.Data)
		}
		propose(pb.CatalogMetadataRequest{Action: pb.CATALOG_ACTION_ENABLE_MAINTENANCE})

		// The current leader's local replica report is delayed, while the
		// other two voters report and a spare is ready to join as a non-voter.
		hb := leader.getHeartbeatMessage()
		hb.Replicas = nil
		_, err = leader.addLogStoreHeartbeat(ctx, hb)
		require.NoError(t, err)
		spare := services[3].store.getHeartbeatMessage()
		_, err = leader.addLogStoreHeartbeat(ctx, spare)
		require.NoError(t, err)
		const replica = uint64(4)
		op := &pb.CatalogMetadataMembershipOperation{
			ConfigChangeIndex: before.ConfigChangeID, ReplicaID: replica, UUID: spare.UUID,
			StoreIncarnation: spare.StoreIncarnation, ChangeType: pb.AddNonVotingReplica,
		}
		token := propose(pb.CatalogMetadataRequest{Action: pb.CATALOG_ACTION_RESERVE_MEMBERSHIP, Membership: op})

		barrier, err := leader.catalogBarrierState(ctx)
		require.NoError(t, err)
		require.NotNil(t, barrier.Arbitration.Reservation)
		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{ChangeType: op.ChangeType,
				Replica: pb.Replica{ShardID: shard, ReplicaID: replica, UUID: spare.UUID, Epoch: before.ConfigChangeID}},
			CatalogMetadataMembership: barrier.Arbitration.Reservation,
		}
		canceled, cancelCommand := context.WithCancel(ctx)
		cancelCommand()
		require.ErrorIs(t, leader.executeCatalogCommand(canceled, cmd), context.Canceled)

		// The leader can recover a lost delivery using the reserved token.
		require.NoError(t, leader.reconcileCatalogMembership(ctx, nil))
		require.True(t, moerr.IsMoErrCode(leader.executeCatalogCommand(ctx, cmd), moerr.ErrInvalidState))
		after, err := leader.catalogMembership(ctx)
		require.NoError(t, err)
		require.Equal(t, before.Nodes, after.Nodes)
		require.Equal(t, map[uint64]string{replica: spare.UUID}, after.NonVotings)
		require.NotContains(t, after.Removed, uint64(0))
		barrier, err = leader.catalogBarrierState(ctx)
		require.NoError(t, err)
		require.Nil(t, barrier.Arbitration.Reservation)
		require.Equal(t, token, barrier.Arbitration.LastOperationID)
		require.Equal(t, after.ConfigChangeID, barrier.Arbitration.ConfirmedConfigChangeIndex)
	})
}

func TestCatalogMembershipIdentityAndResult(t *testing.T) {
	p := &pb.CatalogMetadataMembershipOperation{Token: 4, ConfigChangeIndex: 7, ReplicaID: 2, UUID: "target", StoreIncarnation: "inc", ChangeType: pb.AddReplica}
	require.True(t, sameCatalogMembership(p, p))
	for _, alter := range []func(*pb.CatalogMetadataMembershipOperation){
		func(p *pb.CatalogMetadataMembershipOperation) { p.Token++ },
		func(p *pb.CatalogMetadataMembershipOperation) { p.ConfigChangeIndex++ },
		func(p *pb.CatalogMetadataMembershipOperation) { p.ReplicaID++ },
		func(p *pb.CatalogMetadataMembershipOperation) { p.UUID = "other" },
		func(p *pb.CatalogMetadataMembershipOperation) { p.StoreIncarnation = "other" },
		func(p *pb.CatalogMetadataMembershipOperation) { p.ChangeType = pb.RemoveReplica },
	} {
		copy := *p
		alter(&copy)
		require.False(t, sameCatalogMembership(p, &copy))
	}
	for _, change := range []pb.ConfigChangeType{pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica} {
		p.ChangeType = change
		m := &dragonboat.Membership{Nodes: map[uint64]string{1: "retained"}, NonVotings: map[uint64]string{}}
		switch change {
		case pb.AddReplica:
			m.Nodes[2] = "target"
		case pb.AddNonVotingReplica:
			m.NonVotings[2] = "target"
		}
		p.ExpectedVoting, p.ExpectedNonVoting = maps.Clone(m.Nodes), maps.Clone(m.NonVotings)
		require.True(t, catalogMembershipMatches(p, m))
		m.Nodes[2], m.NonVotings[2] = "other", "other"
		require.False(t, catalogMembershipMatches(p, m))
	}
}

// All accesses to this controller are serialized by the production supervisor.
// Hooks assert durable ordering or block on explicit channels, never on sleeps.
type catalogTestReplica struct {
	present, ready      bool
	starts, stops       int
	startHook, stopHook func() error
}

func (h *catalogTestReplica) catalogReplicaPresent(_ pb.CatalogMetadataStartPermit, ready bool) bool {
	return h.present && (!ready || h.ready)
}
func (h *catalogTestReplica) catalogStartReplica(_ pb.CatalogMetadataStartPermit) error {
	h.starts++
	if h.startHook != nil {
		if err := h.startHook(); err != nil {
			return err
		}
	}
	h.present = true
	return nil
}
func (h *catalogTestReplica) catalogStopReplica(_ pb.CatalogMetadataStartPermit) error {
	h.stops++
	if h.stopHook != nil {
		if err := h.stopHook(); err != nil {
			return err
		}
	}
	h.present, h.ready = false, false
	return nil
}
func catalogTestSupervisor(t *testing.T) (*store, pb.CatalogMetadataStartPermit) {
	t.Helper()
	cfg := Config{UUID: "store", DataDir: "catalog-test", FS: vfs.NewMem(), CatalogMetadataMaintenance: true}
	t.Cleanup(func() { vfs.ReportLeakedFD(cfg.FS, t) })
	s := &store{cfg: cfg}
	require.NoError(t, s.loadCatalogExecutor())
	s.mu.metadata.Incarnation = "inc"
	return s, pb.CatalogMetadataStartPermit{Token: 1, ReplicaID: 3, UUID: cfg.UUID, StoreIncarnation: "inc"}
}
func catalogReadRecord(t *testing.T, s *store) catalogExecutorRecord {
	t.Helper()
	var r catalogExecutorRecord
	require.NoError(t, readMetadataFile(s.cfg.DataDir, catalogExecutorFilename, &r, s.cfg.FS))
	return r
}

func TestCatalogExecutorDurableTransitions(t *testing.T) {
	s, p := catalogTestSupervisor(t)
	h := &catalogTestReplica{}
	h.startHook = func() error {
		r := catalogReadRecord(t, s)
		require.Equal(t, "STARTING", r.State)
		require.Equal(t, p.Token, r.Permit.Token)
		return errors.New("start outcome unknown")
	}
	require.ErrorContains(t, s.executeCatalogStartWith(context.Background(), p, h), "unknown")
	require.Equal(t, "STARTING", catalogReadRecord(t, s).State)
	next := p
	next.Token++
	require.Error(t, s.executeCatalogStartWith(context.Background(), next, h))
	require.Equal(t, 1, h.starts)
	// A crashed pending execution can retry only the same durable token.
	restarted := &store{cfg: s.cfg}
	restarted.mu.metadata.Incarnation = "inc"
	require.NoError(t, restarted.loadCatalogExecutor())
	h.startHook = nil
	require.NoError(t, restarted.executeCatalogStartWith(context.Background(), p, h))
	require.Equal(t, "STARTING", catalogReadRecord(t, restarted).State)
	require.Equal(t, 2, h.starts)
	// Start returning does not prove readiness. Publish STARTED only after the
	// local replica is actually ready, without any membership-index condition.
	h.ready = true
	require.NoError(t, restarted.executeCatalogStartWith(context.Background(), p, h))
	require.Equal(t, "STARTED", catalogReadRecord(t, restarted).State)
	require.Equal(t, 2, h.starts)
	require.NoError(t, restarted.executeCatalogStartWith(context.Background(), p, h))
	require.Equal(t, 2, h.starts)
	revoke := p
	revoke.RevocationPending = true
	h.stopHook = func() error {
		require.Equal(t, "REVOKING", catalogReadRecord(t, restarted).State)
		return errors.New("stop outcome unknown")
	}
	require.ErrorContains(t, restarted.executeCatalogStartWith(context.Background(), revoke, h), "unknown")
	require.Error(t, restarted.executeCatalogStartWith(context.Background(), p, h))
	// Restart during cancellation must never execute a late start.
	final := &store{cfg: s.cfg}
	final.mu.metadata.Incarnation = "inc"
	require.NoError(t, final.loadCatalogExecutor())
	require.Error(t, final.executeCatalogStartWith(context.Background(), p, h))
	h.stopHook = nil
	require.NoError(t, final.executeCatalogStartWith(context.Background(), revoke, h))
	require.False(t, h.present)
	require.Equal(t, "REVOKED", catalogReadRecord(t, final).State)
	require.NoError(t, final.executeCatalogStartWith(context.Background(), revoke, h))
	require.Equal(t, 2, h.stops)
	require.Error(t, final.executeCatalogStartWith(context.Background(), p, h))
}

func TestCatalogExecutorSerialStartCancel(t *testing.T) {
	s, p := catalogTestSupervisor(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	h := &catalogTestReplica{ready: true, startHook: func() error { close(entered); <-release; return nil }}
	startDone := make(chan error, 1)
	go func() { startDone <- s.executeCatalogStartWith(context.Background(), p, h) }()
	<-entered
	revoke := p
	revoke.RevocationPending = true
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- s.executeCatalogStartWith(context.Background(), revoke, h) }()
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-startDone)
	require.NoError(t, <-cancelDone)
	require.False(t, h.present)
	require.Equal(t, 1, h.starts)
	require.Equal(t, 1, h.stops)
	require.Equal(t, "REVOKED", catalogReadRecord(t, s).State)
	require.Error(t, s.executeCatalogStartWith(context.Background(), p, h))
}

func TestCatalogExecutorCompletionWriteFailure(t *testing.T) {
	s, p := catalogTestSupervisor(t)
	fs := s.cfg.FS
	h := &catalogTestReplica{ready: true}
	h.startHook = func() error { s.cfg.FS = catalogFailCreateFS{fs}; return nil }
	require.ErrorContains(t, s.executeCatalogStartWith(context.Background(), p, h), "injected")
	require.True(t, h.present)
	require.True(t, s.catalogExecutor.failed)
	require.Nil(t, s.catalogStartHeartbeat())
	s.cfg.FS = fs
	require.Equal(t, "STARTING", catalogReadRecord(t, s).State)
	// Lost STARTED write/ack: authoritative local identity plus the same durable
	// token permits completion after restart, without executing Start twice.
	restarted := &store{cfg: s.cfg}
	restarted.mu.metadata.Incarnation = "inc"
	require.NoError(t, restarted.loadCatalogExecutor())
	require.NoError(t, restarted.executeCatalogStartWith(context.Background(), p, h))
	require.Equal(t, 1, h.starts)
	require.Equal(t, "STARTED", catalogReadRecord(t, restarted).State)
}

type catalogTestMembership struct {
	state                           pb.CatalogMetadataBarrierState
	membership                      dragonboat.Membership
	readErr, mutateErr, completeErr error
	completeCount                   int
	attempts                        []pb.CatalogMetadataMembershipOperation
	advance                         bool
	afterRead                       func()
}

func (c *catalogTestMembership) catalogBarrierState(context.Context) (*pb.CatalogMetadataBarrierState, error) {
	return &c.state, c.readErr
}
func (c *catalogTestMembership) catalogMembership(context.Context) (*dragonboat.Membership, error) {
	copy := c.membership
	if c.afterRead != nil {
		c.afterRead()
	}
	return &copy, c.readErr
}
func (c *catalogTestMembership) catalogMutateMembership(_ context.Context, p *pb.CatalogMetadataMembershipOperation) error {
	c.attempts = append(c.attempts, *p)
	if c.advance {
		c.membership.ConfigChangeID++
		c.membership.NonVotings = map[uint64]string{p.ReplicaID: p.UUID}
	}
	return c.mutateErr
}
func (c *catalogTestMembership) catalogCompleteMembership(_ context.Context, p *pb.CatalogMetadataMembershipOperation, m *dragonboat.Membership) error {
	c.completeCount++
	if c.completeErr != nil {
		return c.completeErr
	}
	c.state.Arbitration.Reservation = nil
	return nil
}

func TestCatalogMembershipReconciliation(t *testing.T) {
	p := pb.CatalogMetadataMembershipOperation{Token: 4, ConfigChangeIndex: 7, ReplicaID: 2, UUID: "target", StoreIncarnation: "inc", ChangeType: pb.AddNonVotingReplica, ExpectedVoting: map[uint64]string{1: "retained"}, ExpectedNonVoting: map[uint64]string{2: "target"}}
	newOwner := func() *catalogTestMembership {
		copy := p
		return &catalogTestMembership{state: pb.CatalogMetadataBarrierState{Arbitration: &pb.CatalogMetadataArbitration{MaintenanceEnabled: true, Reservation: &copy}}, membership: dragonboat.Membership{ConfigChangeID: 7, Nodes: map[uint64]string{1: "retained"}}}
	}
	ctx := context.Background()
	t.Run("unknown result retains token", func(t *testing.T) {
		c := newOwner()
		c.mutateErr = context.DeadlineExceeded
		require.ErrorIs(t, reconcileCatalogMembership(ctx, &p, c), context.DeadlineExceeded)
		require.Equal(t, &p, c.state.Arbitration.Reservation)
		require.Zero(t, c.completeCount)
		c.mutateErr = nil
		require.NoError(t, reconcileCatalogMembership(ctx, nil, c))
		require.Equal(t, []pb.CatalogMetadataMembershipOperation{p, p}, c.attempts)
		require.NotNil(t, c.state.Arbitration.Reservation)
		require.Zero(t, c.completeCount)
		c.advance = true
		require.NoError(t, reconcileCatalogMembership(ctx, nil, c))
		require.Nil(t, c.state.Arbitration.Reservation)
		require.Equal(t, 1, c.completeCount)
	})
	t.Run("late applied readback and lost completion", func(t *testing.T) {
		c := newOwner()
		c.advance, c.mutateErr = true, context.DeadlineExceeded
		require.Error(t, reconcileCatalogMembership(ctx, &p, c))
		require.NotNil(t, c.state.Arbitration.Reservation)
		c.completeErr = context.DeadlineExceeded
		require.Error(t, reconcileCatalogMembership(ctx, nil, c))
		require.Len(t, c.attempts, 1)
		c.completeErr = nil
		require.NoError(t, reconcileCatalogMembership(ctx, nil, c))
		require.Len(t, c.attempts, 1)
		require.Nil(t, c.state.Arbitration.Reservation)
		require.Error(t, reconcileCatalogMembership(ctx, &p, c))
	})
	t.Run("conflict stays closed", func(t *testing.T) {
		c := newOwner()
		c.membership.ConfigChangeID++
		c.membership.NonVotings = map[uint64]string{2: "conflicting"}
		require.Error(t, reconcileCatalogMembership(ctx, nil, c))
		require.Equal(t, &p, c.state.Arbitration.Reservation)
		require.Empty(t, c.attempts)
		require.Zero(t, c.completeCount)
	})
	t.Run("unrelated member conflict stays closed", func(t *testing.T) {
		c := newOwner()
		c.membership.ConfigChangeID++
		c.membership.NonVotings = map[uint64]string{2: "target"}
		c.membership.Nodes[1] = "unexpected-replacement"
		require.Error(t, reconcileCatalogMembership(ctx, nil, c))
		require.Equal(t, &p, c.state.Arbitration.Reservation)
		require.Empty(t, c.attempts)
		require.Zero(t, c.completeCount)
	})
	t.Run("inconsistent preimage cannot mutate", func(t *testing.T) {
		c := newOwner()
		c.membership.Nodes[1] = "unexpected-replacement"
		require.Error(t, reconcileCatalogMembership(ctx, nil, c))
		require.Empty(t, c.attempts)
		require.Zero(t, c.completeCount)
	})
	t.Run("identity read and cancellation reject", func(t *testing.T) {
		c := newOwner()
		wrong := p
		wrong.StoreIncarnation = "new"
		require.Error(t, reconcileCatalogMembership(ctx, &wrong, c))
		c.readErr = errors.New("leader unavailable")
		require.ErrorContains(t, reconcileCatalogMembership(ctx, nil, c), "leader unavailable")
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, reconcileCatalogMembership(canceled, nil, c), context.Canceled)
		require.Empty(t, c.attempts)
		require.Zero(t, c.completeCount)
		require.Equal(t, &p, c.state.Arbitration.Reservation)
	})
	t.Run("cancellation after membership read preserves reservation", func(t *testing.T) {
		c := newOwner()
		attempt, cancel := context.WithCancel(ctx)
		defer cancel()
		c.afterRead = cancel
		require.ErrorIs(t, reconcileCatalogMembership(attempt, &p, c), context.Canceled)
		require.Empty(t, c.attempts)
		require.Equal(t, &p, c.state.Arbitration.Reservation)
	})
}

func TestCatalogExecutorPermitReplacement(t *testing.T) {
	s, p := catalogTestSupervisor(t)
	h := &catalogTestReplica{ready: true}
	require.NoError(t, s.executeCatalogStartWith(context.Background(), p, h))
	replacement := p
	replacement.Token++
	replacement.ReplicaID++
	// Keep the old cancellation owner until it has stopped its replica.
	require.Error(t, s.executeCatalogStartWith(context.Background(), replacement, h))
	require.Equal(t, p.Token, catalogReadRecord(t, s).Permit.Token)
	revoke := p
	revoke.RevocationPending = true
	require.NoError(t, s.executeCatalogStartWith(context.Background(), revoke, h))
	require.NoError(t, s.executeCatalogStartWith(context.Background(), replacement, h))
	require.Equal(t, replacement.Token, catalogReadRecord(t, s).Permit.Token)
	require.Error(t, s.executeCatalogStartWith(context.Background(), p, h))
	require.Error(t, s.executeCatalogStartWith(context.Background(), revoke, h))
	require.Equal(t, 2, h.starts)
}

func TestCatalogExecutorCapabilityGate(t *testing.T) {
	s, _ := catalogTestSupervisor(t)
	// Only the durable maintenance executor advertises the barrier protocol.
	capability := s.catalogExecutorCapabilities()
	require.NotNil(t, capability)
	require.Equal(t, uint64(1), capability.HAKeeperBarrierProtocol)
	require.Zero(t, capability.ViewDependencyProtocol)
	require.Zero(t, capability.RecoveryProtocol)
	s.catalogExecutor.failed = true
	require.Nil(t, s.catalogExecutorCapabilities())
	require.False(t, DefaultConfig().CatalogMetadataMaintenance)
}
