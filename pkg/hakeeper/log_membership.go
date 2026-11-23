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
	"slices"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// LogMembershipTargets returns reporting, retained voters that can submit the
// change with a reporting majority in both configurations.
func LogMembershipTargets(cfg Config, state pb.LogState, tick uint64, change pb.ConfigChangeType, replica pb.Replica) []string {
	targets, _ := logMembershipTargets(cfg.LogStoreTimeoutTicks(), true, state, tick, change, replica, "")
	return targets
}

// An empty target collects sorted executors; a non-empty target checks one
// executor without allocating a list.
func logMembershipTargets(timeoutTicks uint64, checkExpiry bool, state pb.LogState, tick uint64, change pb.ConfigChangeType, replica pb.Replica, target string) ([]string, bool) {
	shard, ok := state.Shards[replica.ShardID]
	n := len(shard.Replicas)
	if !ok || shard.Epoch != replica.Epoch || n == 0 {
		return nil, false
	}
	voting := change == pb.AddReplica || change == pb.RemoveReplica
	removing := change == pb.RemoveReplica || change == pb.RemoveNonVotingReplica
	switch change {
	case pb.RemoveReplica:
		if shard.Replicas[replica.ReplicaID] != replica.UUID {
			return nil, false
		}
	case pb.RemoveNonVotingReplica:
		if shard.NonVotingReplicas[replica.ReplicaID] != replica.UUID {
			return nil, false
		}
	case pb.AddReplica, pb.AddNonVotingReplica:
		store, exists := state.Stores[replica.UUID]
		if replica.UUID == "" || !exists || (checkExpiry && timeoutTicks+store.Tick < tick) {
			return nil, false
		}
		if _, exists := shard.Replicas[replica.ReplicaID]; exists {
			return nil, false
		}
		if _, exists := shard.NonVotingReplicas[replica.ReplicaID]; exists {
			return nil, false
		}
		for _, uuid := range shard.Replicas {
			if uuid == replica.UUID {
				return nil, false
			}
		}
		for _, uuid := range shard.NonVotingReplicas {
			if uuid == replica.UUID {
				return nil, false
			}
		}
	default:
		return nil, false
	}

	resulting := n
	if voting {
		if removing {
			resulting--
		} else {
			resulting++
		}
	}
	if resulting == 0 {
		return nil, false
	}

	var targets []string
	matched := false
	active := 0
	retained := 0
	for id, uuid := range shard.Replicas {
		store, exists := state.Stores[uuid]
		if !exists || (checkExpiry && timeoutTicks+store.Tick < tick) {
			continue
		}
		oldIncarnation := shard.ReplicaStoreIncarnations[id]
		if oldIncarnation != "" && store.StoreIncarnation != "" && oldIncarnation != store.StoreIncarnation {
			continue
		}
		reported := false
		for _, r := range store.Replicas {
			if r.ShardID == replica.ShardID && r.ReplicaID == id && !r.IsNonVoting {
				reported = true
				break
			}
		}
		if !reported {
			continue
		}
		active++
		if change != pb.RemoveReplica || id != replica.ReplicaID {
			retained++
			if target == "" {
				targets = append(targets, uuid)
			} else if uuid == target {
				matched = true
			}
		}
	}
	if active < n/2+1 || retained < resulting/2+1 {
		return nil, false
	}
	slices.Sort(targets)
	return targets, matched
}
