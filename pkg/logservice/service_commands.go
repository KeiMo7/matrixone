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
	"errors"
	"fmt"
	"reflect"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

func (s *Service) handleCommands(ctx context.Context, cmds []pb.ScheduleCommand) {
	// A stop initiates asynchronous offload. Do not start a new incarnation of
	// the same shard in this batch; bootstrap and catalog owners retry theirs.
	var stopping map[uint64]bool
	for _, cmd := range cmds {
		if change := cmd.GetConfigChange(); isReplicaCleanup(change) {
			if stopping == nil {
				stopping = make(map[uint64]bool)
			}
			stopping[change.Replica.ShardID] = true
		}
	}
	for _, cmd := range cmds {
		change := cmd.GetConfigChange()
		if ctx.Err() != nil && !isReplicaCleanup(change) {
			continue
		}
		if change != nil && isMembershipChange(change) {
			continue
		}
		if change != nil && (change.ChangeType == pb.StartReplica || change.ChangeType == pb.StartNonVotingReplica) {
			if stopping[change.Replica.ShardID] {
				s.runtime.Logger().Warn("skipping replica start in the same batch as a stop", zap.String("command", cmd.LogString()))
				continue
			}
			if change.Replica.ShardID == hakeeper.DefaultHAKeeperShardID && s.store.catalogExecutor.enabled {
				if err := s.store.executeCatalogCommand(ctx, cmd); err != nil {
					s.runtime.Logger().Error("catalog metadata command rejected", zap.Error(err))
				}
				continue
			}
			if cmd.UUID != s.ID() || !s.canStartLocalReplica(change.Replica) ||
				(cmd.Bootstrapping && change.InitialMembers[change.Replica.ReplicaID] != s.ID()) {
				continue
			}
			s.runtime.Logger().Info(fmt.Sprintf("%s applying cmd: %s", s.ID(), cmd.LogString()))
			if ctx.Err() != nil {
				continue
			}
			if change.ChangeType == pb.StartReplica {
				s.handleStartReplica(cmd)
			} else {
				s.handleStartNonVotingReplica(cmd)
			}
			continue
		}
		s.runtime.Logger().Info(fmt.Sprintf("%s applying cmd: %s", s.ID(), cmd.LogString()))
		if ctx.Err() != nil && !isReplicaCleanup(change) {
			continue
		}
		if change != nil {
			if change.Replica.ShardID == hakeeper.DefaultHAKeeperShardID && s.store.catalogExecutor.enabled {
				if err := s.store.executeCatalogCommand(ctx, cmd); err != nil {
					s.runtime.Logger().Error("catalog metadata command rejected", zap.Error(err))
				}
				continue
			}
			switch change.ChangeType {
			case pb.StopReplica, pb.StopNonVotingReplica:
				s.handleStopReplica(cmd)
			case pb.KillZombie:
				s.handleKillZombie(cmd)
			default:
				panic(fmt.Sprintf("unknown config change cmd type %d", change.ChangeType))
			}
		} else if cmd.GetShutdownStore() != nil {
			// Historical cluster-wide shutdowns must not prevent quorum recovery.
			continue
		} else if cmd.GetCreateTaskService() != nil {
			s.createTaskService(cmd.CreateTaskService)
			s.createSQLLogger(cmd.CreateTaskService)
		} else if cmd.GetAddLogShard() != nil {
			s.handleAddLogShard(cmd)
		} else if cmd.GetBootstrapShard() != nil {
			s.handleBootstrapShard(cmd)
		} else {
			panic(fmt.Sprintf("unknown schedule command type: %+v", cmd))
		}
	}

	// Lifecycle work can restore quorum even when a membership operation blocks.
	// Preserve delivery order among the membership operations themselves.
	for _, cmd := range cmds {
		if !isMembershipChange(cmd.ConfigChange) {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if cmd.ConfigChange.Replica.ShardID == hakeeper.DefaultHAKeeperShardID && s.store.catalogExecutor.enabled {
			if err := s.store.executeCatalogCommand(ctx, cmd); err != nil {
				s.runtime.Logger().Error("catalog metadata command rejected", zap.Error(err))
			}
			continue
		}
		if cmd.UUID != s.ID() {
			continue
		}
		s.runtime.Logger().Info(fmt.Sprintf("%s applying cmd: %s", s.ID(), cmd.LogString()))
		if ctx.Err() != nil {
			return
		}
		switch cmd.ConfigChange.ChangeType {
		case pb.AddReplica:
			s.handleAddReplica(cmd)
		case pb.AddNonVotingReplica:
			s.handleAddNonVotingReplica(cmd)
		case pb.RemoveReplica, pb.RemoveNonVotingReplica:
			s.handleRemoveReplica(cmd)
		}
	}
}

func isReplicaCleanup(change *pb.ConfigChange) bool {
	if change == nil {
		return false
	}
	switch change.ChangeType {
	case pb.StopReplica, pb.StopNonVotingReplica, pb.KillZombie:
		return true
	default:
		return false
	}
}

func isMembershipChange(change *pb.ConfigChange) bool {
	if change == nil {
		return false
	}
	switch change.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
		return true
	default:
		return false
	}
}

func (s *Service) canStartLocalReplica(replica pb.Replica) bool {
	if replica.UUID != s.ID() || s.store.isSkippedZombie(replica.ShardID, replica.ReplicaID) {
		return false
	}
	for _, local := range s.store.getShards() {
		if local.ShardID == replica.ShardID && local.ReplicaID == replica.ReplicaID && local.Retiring {
			return false
		}
	}
	return true
}

func (s *Service) handleAddReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	target := cmd.ConfigChange.Replica.UUID
	if err := s.store.addReplica(shardID, replicaID, target, epoch); err != nil {
		s.runtime.Logger().Error("failed to add replica", zap.Error(err))
	}
}

func (s *Service) handleAddNonVotingReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	target := cmd.ConfigChange.Replica.UUID
	if err := s.store.addNonVotingReplica(shardID, replicaID, target, epoch); err != nil {
		s.runtime.Logger().Error("failed to add non-voting replica", zap.Error(err))
	}
}

func (s *Service) handleRemoveReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	epoch := cmd.ConfigChange.Replica.Epoch
	if err := s.store.removeReplica(shardID, replicaID, epoch); err != nil {
		s.runtime.Logger().Error("failed to remove replica", zap.Error(err))
	}
}

func (s *Service) handleStartReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	join := len(cmd.ConfigChange.InitialMembers) == 0 && !s.store.nh.HasNodeInfo(shardID, replicaID)
	if shardID == hakeeper.DefaultHAKeeperShardID {
		if err := s.store.startHAKeeperReplica(replicaID,
			cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start HAKeeper replica", zap.Error(err))
		}
	} else {
		if err := s.store.startReplica(shardID,
			replicaID, cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start log replica", zap.Error(err))
		}
	}
}

func (s *Service) handleStartNonVotingReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	join := len(cmd.ConfigChange.InitialMembers) == 0 && !s.store.nh.HasNodeInfo(shardID, replicaID)
	if shardID == hakeeper.DefaultHAKeeperShardID {
		if err := s.store.startHAKeeperNonVotingReplica(replicaID,
			cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start HAKeeper replica",
				zap.Uint64("shard ID", shardID),
				zap.Uint64("replica ID", replicaID),
				zap.Error(err),
			)
		}
	} else {
		if err := s.store.startNonVotingReplica(shardID,
			replicaID, cmd.ConfigChange.InitialMembers, join); err != nil {
			s.runtime.Logger().Error("failed to start log replica",
				zap.Uint64("shard ID", shardID),
				zap.Uint64("replica ID", replicaID),
				zap.Error(err),
			)
		}
	}
}

func (s *Service) handleStopReplica(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	if err := s.store.stopReplica(shardID, replicaID); err != nil {
		s.runtime.Logger().Error("failed to stop replica", zap.Error(err))
	}
}

func (s *Service) handleKillZombie(cmd pb.ScheduleCommand) {
	shardID := cmd.ConfigChange.Replica.ShardID
	replicaID := cmd.ConfigChange.Replica.ReplicaID
	s.store.markReplicaRetiring(shardID, replicaID)
	if err := s.store.stopReplica(shardID, replicaID); err != nil {
		if s.store.localReplicaPresent(shardID, replicaID) {
			s.runtime.Logger().Error("failed to stop zombie replica", zap.Error(err))
			return
		}
	}
	if err := s.store.snapshotMgr.RemoveReplica(shardID, replicaID); err != nil {
		if errors.Is(err, errSnapshotExportPending) {
			s.runtime.Logger().Warn("defer zombie exported snapshot cleanup until restart",
				zap.Uint64("shardID", shardID), zap.Uint64("replicaID", replicaID))
		} else {
			s.runtime.Logger().Error("failed to remove zombie exported snapshots", zap.Error(err))
		}
		return
	}
	s.store.removeMetadata(shardID, replicaID)
}

func (s *Service) heartbeatWorker(ctx context.Context) {
	// TODO: check tick interval
	if s.cfg.HeartbeatInterval.Duration == 0 {
		panic("invalid heartbeat interval")
	}
	defer func() {
		s.runtime.Logger().Info("heartbeat worker stopped")
	}()
	ticker := time.NewTicker(s.cfg.HeartbeatInterval.Duration)
	defer ticker.Stop()
	ctx, span := trace.Start(ctx, "heartbeatWorker")
	defer span.End()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.heartbeatC:
			s.heartbeat(ctx)
		case <-ticker.C:
			s.heartbeat(ctx)
			// I'd call this an ugly hack to just workaround select's
			// policy of randomly picking a ready channel from the case list.
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (s *Service) requestHeartbeat() {
	if s.heartbeatC == nil {
		return
	}
	select {
	case s.heartbeatC <- struct{}{}:
	default:
	}
}

func (s *Service) checkReplicaHealth(ctx context.Context) {
	details, err := s.store.getClusterDetails(ctx)
	if err != nil {
		s.runtime.Logger().Error("failed to get cluster details",
			zap.Error(err))
		return
	}
	var tnShardID uint64
	for _, tnStore := range details.TNStores {
		for _, shard := range tnStore.Shards {
			tnShardID = shard.ShardID
		}
	}
	if tnShardID == 0 {
		s.runtime.Logger().Error("failed to get tn shard ID",
			zap.Uint64("shardID", tnShardID))
		return
	}
	// A TN shard and its backing Log shard have distinct IDs. The only
	// supported TN shard is backed by the first Log shard.
	if err := s.store.checkHealth(firstLogShardID); err != nil {
		s.runtime.Logger().Error("failed to check health", zap.Error(err))
		v2.LogServiceReplicaHealthGauge.Set(0)
	} else {
		v2.LogServiceReplicaHealthGauge.Set(1)
	}
}

func (s *Service) heartbeat(ctx context.Context) {
	start := time.Now()
	defer func() {
		v2.LogHeartbeatHistogram.Observe(time.Since(start).Seconds())
	}()
	ctx2, cancel := context.WithTimeoutCause(ctx, 3*time.Second, moerr.CauseLogServiceHeartbeat)
	defer cancel()

	if s.haClient == nil {
		if reflect.DeepEqual(s.cfg.HAKeeperClientConfig, HAKeeperClientConfig{}) {
			panic("empty HAKeeper client config")
		}
		cc, err := NewLogHAKeeperClient(ctx2, s.cfg.UUID, s.cfg.GetHAKeeperClientConfig())
		if err != nil {
			err = moerr.AttachCause(ctx2, err)
			s.runtime.Logger().Error("failed to create HAKeeper client", zap.Error(err))
			return
		}
		s.haClient = cc
	}

	hb := s.store.getHeartbeatMessage()
	hb.TaskServiceCreated = s.taskServiceCreated()
	hb.ConfigData = s.config.GetData()
	s.addWALRecoveryStatus(&hb)

	cb, err := s.haClient.SendLogHeartbeat(ctx2, hb)
	if err != nil {
		err = moerr.AttachCause(ctx2, err)
		v2.LogHeartbeatFailureCounter.Inc()
		v2.LogServiceReplicaHealthGauge.Set(0)
		s.runtime.Logger().Error("failed to send log service heartbeat", zap.Error(err))
		return
	}

	// check the logService TN replica's health on this store.
	// NB: do the health checking after heartbeat
	s.checkReplicaHealth(ctx2)

	s.config.DecrCount()

	s.handleCommands(ctx, cb.Commands)
}
