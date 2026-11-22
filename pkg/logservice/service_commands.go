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
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

func (s *Service) handleCommands(ctx context.Context, cmds []pb.ScheduleCommand) {
	// StopReplica only initiates asynchronous Raft offload. Defer queued
	// non-bootstrap starts for the same shard in this heartbeat batch.
	stopping := make(map[uint64]struct{})
	for _, cmd := range cmds {
		if change := cmd.GetConfigChange(); change != nil {
			switch change.ChangeType {
			case pb.StopReplica, pb.StopNonVotingReplica, pb.KillZombie:
				stopping[change.Replica.ShardID] = struct{}{}
			}
		}
	}
	// Bootstrap commands carry an initial membership and follow the existing
	// bootstrap delivery protocol, including one-shot dynamic bootstrap.
	validation := scheduleValidation{remaining: time.Second}
	for _, cmd := range cmds {
		change := cmd.GetConfigChange()
		if cmd.BootstrapShard != nil {
			s.handleBootstrapShard(cmd)
			continue
		}
		if cmd.Bootstrapping && change != nil && change.ChangeType == pb.StartReplica {
			s.handleStartReplica(cmd)
			continue
		}
		if change != nil && (isMembershipChange(change) || change.ChangeType == pb.StartReplica || change.ChangeType == pb.StartNonVotingReplica) {
			if !s.pendingSchedule.add(cmd) {
				s.runtime.Logger().Warn("schedule command queue full; relying on HAKeeper retry", zap.String("command", cmd.LogString()))
			}
			continue
		}
		s.runtime.Logger().Info(fmt.Sprintf("%s applying cmd: %s", s.ID(), cmd.LogString()))
		if change != nil {
			s.runtime.Logger().Debug("applying schedule command:", zap.String("command", cmd.LogString()))
			switch cmd.ConfigChange.ChangeType {
			case pb.StopReplica:
				s.handleStopReplica(cmd)
			case pb.StopNonVotingReplica:
				s.handleStopReplica(cmd)
			case pb.KillZombie:
				s.handleKillZombie(cmd)
			default:
				panic(fmt.Sprintf("unknown config change cmd type %d", cmd.ConfigChange.ChangeType))
			}
		} else if cmd.GetShutdownStore() != nil {
			// Ignore historical global shutdowns without dropping other commands.
			continue
		} else if cmd.GetCreateTaskService() != nil {
			s.createTaskService(cmd.CreateTaskService)
			s.createSQLLogger(cmd.CreateTaskService)
		} else if cmd.GetAddLogShard() != nil {
			s.handleAddLogShard(cmd)
		} else {
			panic(fmt.Sprintf("unknown schedule command type: %+v", cmd))
		}
	}

	// Bound admission of work per heartbeat, while giving each read its own
	// deadline. A failed read rotates to avoid starving unrelated shards.
	for remaining := len(s.pendingSchedule.items); remaining > 0 && len(s.pendingSchedule.items) > 0 && !validation.exhausted() && ctx.Err() == nil; remaining-- {
		cmd := s.pendingSchedule.items[0].cmd
		if change := cmd.ConfigChange; change.ChangeType == pb.StartReplica || change.ChangeType == pb.StartNonVotingReplica {
			if _, stopped := stopping[change.Replica.ShardID]; stopped {
				s.pendingSchedule.rotate()
				continue
			}
		}
		allowed := s.authorizeScheduledChange(ctx, cmd.ConfigChange, &validation)
		if validation.failed {
			s.pendingSchedule.rotate()
			break
		}
		s.pendingSchedule.pop()
		if !allowed {
			continue
		}
		started := time.Now()
		s.runtime.Logger().Info(fmt.Sprintf("%s applying cmd: %s", s.ID(), cmd.LogString()))
		switch cmd.ConfigChange.ChangeType {
		case pb.AddReplica:
			s.handleAddReplica(cmd)
		case pb.AddNonVotingReplica:
			s.handleAddNonVotingReplica(cmd)
		case pb.RemoveReplica, pb.RemoveNonVotingReplica:
			s.handleRemoveReplica(cmd)
		case pb.StartReplica:
			s.handleStartReplica(cmd)
		case pb.StartNonVotingReplica:
			s.handleStartNonVotingReplica(cmd)
		}
		validation.charge(time.Since(started))
	}
}

func isMembershipChange(change *pb.ConfigChange) bool {
	switch change.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
		return true
	default:
		return false
	}
}

type scheduleValidation struct {
	remaining time.Duration
	failed    bool
}

func (v *scheduleValidation) exhausted() bool              { return v.failed || v.remaining <= 0 }
func (v *scheduleValidation) charge(elapsed time.Duration) { v.remaining -= elapsed }

// Authorize against the latest HAKeeper observation immediately before each
// operation. Ordered config changes still use the observed Raft index.
func (s *Service) authorizeScheduledChange(ctx context.Context, change *pb.ConfigChange, validation *scheduleValidation) bool {
	if s.haClient == nil {
		validation.failed = true
		return false
	}
	replica := change.Replica
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer func() {
		cancel()
		validation.charge(time.Since(started))
	}()
	state, err := s.haClient.GetLogScheduleState(ctx, replica.ShardID, replica.UUID, isMembershipChange(change))
	if err != nil {
		validation.failed = true
		if !errors.Is(ctx.Err(), context.Canceled) {
			s.runtime.Logger().Warn("cannot validate schedule command", zap.Error(err))
		}
		return false
	}
	if ctx.Err() != nil {
		validation.failed = true
		return false
	}
	shard, ok := state.LogState.Shards[replica.ShardID]
	if change.ChangeType == pb.StartReplica || change.ChangeType == pb.StartNonVotingReplica {
		if replica.UUID != s.ID() {
			return false
		}
		if !ok {
			return false
		}
		members := shard.Replicas
		if change.ChangeType == pb.StartNonVotingReplica {
			members = shard.NonVotingReplicas
		}
		return members[replica.ReplicaID] == s.ID()
	}
	if !ok {
		return false
	}
	cfg := s.cfg.GetHAKeeperConfig()
	cfg.Fill()
	return slices.Contains(hakeeper.LogMembershipTargets(cfg, state.LogState, state.Tick, change.ChangeType, replica), s.ID())
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
	s.handleStopReplica(cmd)
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
	if err := s.store.checkHealth(tnShardID); err != nil {
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
