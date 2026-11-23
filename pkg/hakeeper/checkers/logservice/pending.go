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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"slices"
)

// PendingOperators preserves suppressed Log work when a TN retry causes another
// legacy replacement batch. Reuse current checker candidates: unfinished alone
// does not mean that an old configuration or storage incarnation is still valid.
func PendingOperators(fields hakeeper.CheckerCommonFields, state pb.LogState,
	executing, executingNonVoting operator.ExecutingReplicas,
	nonVotingReplicaNum uint64, ops, newOps []*operator.Operator) []*operator.Operator {
	working, expired := parseLogStores(fields.Cfg, state, fields.CurrentTick)
	normal, nonVoting := parseLogShards(fields.Cluster, state, expired, nonVotingReplicaNum)
	starts := [2]map[operator.Replica]bool{}
	removes := [2]map[operator.Replica]bool{}
	additions := [2]map[uint64]uint32{normal.toAdd, nonVoting.toAdd}
	for role, candidates := range []*stats{normal, nonVoting} {
		starts[role] = make(map[operator.Replica]bool, len(candidates.toStart))
		removes[role] = make(map[operator.Replica]bool)
		for _, r := range candidates.toStart {
			starts[role][operator.Replica{UUID: r.uuid, ShardID: r.shardID, ReplicaID: r.replicaID}] = true
		}
		for _, replicas := range candidates.toRemove {
			for _, r := range replicas {
				removes[role][operator.Replica{UUID: r.uuid, ShardID: r.shardID, ReplicaID: r.replicaID}] = true
			}
		}
	}
	// Bootstrap demand is independent of add-slot consumption below.
	bootstrap := make(map[uint64]bool)
	for _, shard := range fields.Cluster.LogShards {
		bootstrap[shard.ShardID] = shard.NumberOfReplicas > 0 && uint64(normal.toAdd[shard.ShardID]) == shard.NumberOfReplicas
	}
	live := func(uuid string) bool { _, ok := working[uuid]; return ok }
	membershipScheduled := make(map[uint64]struct{})
	selectedRemovals := make(map[operator.Replica]bool)
	for _, op := range newOps {
		if op == nil {
			continue
		}
		for _, step := range op.OpSteps() {
			var replica operator.Replica
			switch step := step.(type) {
			case operator.RemoveLogService:
				replica = step.Replica
			case operator.RemoveNonVotingLogService:
				replica = step.Replica
			default:
				continue
			}
			selectedRemovals[operator.Replica{ShardID: replica.ShardID, ReplicaID: replica.ReplicaID}] = true
		}
	}
	authorized := func(r operator.Replica, target string, change pb.ConfigChangeType) bool {
		member := pb.Replica{UUID: r.UUID, ShardID: r.ShardID, ReplicaID: r.ReplicaID, Epoch: r.Epoch}
		return slices.Contains(hakeeper.LogMembershipTargets(fields.Cfg, state, fields.CurrentTick, change, member), target)
	}
	add := func(role int, r operator.Replica, target string) bool {
		change := pb.AddReplica
		if role == 1 {
			change = pb.AddNonVotingReplica
		}
		if _, scheduled := membershipScheduled[r.ShardID]; scheduled ||
			additions[role][r.ShardID] == 0 || !authorized(r, target, change) {
			return false
		}
		additions[role][r.ShardID]--
		membershipScheduled[r.ShardID] = struct{}{}
		return true
	}
	remove := func(role int, r operator.Replica, target string) bool {
		key := r
		key.Epoch = 0
		change := pb.RemoveReplica
		if role == 1 {
			change = pb.RemoveNonVotingReplica
		}
		if replicaIsStarting(executing, executingNonVoting, r.ShardID, r.ReplicaID) {
			// The previous removal may already be in flight. Do not resend
			// either side of the conflict while both operators are owned.
			selectedRemovals[operator.Replica{ShardID: r.ShardID, ReplicaID: r.ReplicaID}] = true
			return false
		}
		if _, scheduled := membershipScheduled[r.ShardID]; scheduled ||
			!removes[role][key] || !authorized(r, target, change) {
			return false
		}
		membershipScheduled[r.ShardID] = struct{}{}
		selectedRemovals[operator.Replica{ShardID: r.ShardID, ReplicaID: r.ReplicaID}] = true
		return true
	}
	var pending []*operator.Operator
	startsByOperator := make(map[*operator.Operator]operator.Replica)
	seen := make(map[*operator.Operator]bool, len(ops))
	for _, op := range ops {
		if op == nil || seen[op] {
			continue
		}
		seen[op] = true
		// Other service operations must not observe synthetic empty states.
		steps := op.OpSteps()
		if len(steps) == 0 {
			continue
		}
		switch steps[0].(type) {
		case operator.StartLogService, operator.StartNonVotingLogService,
			operator.AddLogService, operator.AddNonVotingLogService,
			operator.RemoveLogService, operator.RemoveNonVotingLogService,
			operator.BootstrapShard:
		default:
			continue
		}
		step := op.Check(state, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
		if step == nil || op.IsEnd() {
			continue
		}
		eligible := false
		switch step := step.(type) {
		case operator.StartLogService:
			eligible = live(step.UUID) && starts[0][step.Replica]
			startsByOperator[op] = step.Replica
		case operator.StartNonVotingLogService:
			eligible = live(step.UUID) && starts[1][step.Replica]
			startsByOperator[op] = step.Replica
		case operator.AddLogService:
			eligible = add(0, step.Replica, step.Target)
		case operator.AddNonVotingLogService:
			eligible = add(1, step.Replica, step.Target)
		case operator.RemoveLogService:
			eligible = remove(0, step.Replica, step.Target)
		case operator.RemoveNonVotingLogService:
			eligible = remove(1, step.Replica, step.Target)
		case operator.BootstrapShard:
			key := operator.Replica{UUID: step.UUID, ShardID: step.ShardID, ReplicaID: step.ReplicaID}
			eligible = live(step.UUID) && (bootstrap[step.ShardID] || starts[0][key])
			startsByOperator[op] = key
		}
		if eligible {
			pending = append(pending, op)
		}
	}
	return slices.DeleteFunc(pending, func(op *operator.Operator) bool {
		replica, isStart := startsByOperator[op]
		return isStart && (replicaIsRemoving(executing, executingNonVoting, replica.ShardID, replica.ReplicaID) ||
			selectedRemovals[operator.Replica{ShardID: replica.ShardID, ReplicaID: replica.ReplicaID}])
	})
}
