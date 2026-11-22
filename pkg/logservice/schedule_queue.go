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
	"github.com/gogo/protobuf/proto"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Legacy heartbeats deliver commands once. Retain work deferred by validation;
// the checker retries unfinished operators and bootstrap commands after restart.
const maxPendingScheduleCommands = 1024
const maxPendingScheduleBytes = 16 << 20

type scheduleKey struct {
	store, target             string
	shardID, replicaID, epoch uint64
	change                    pb.ConfigChangeType
}

type queuedSchedule struct {
	key  scheduleKey
	cmd  pb.ScheduleCommand
	size int
}

type scheduleQueue struct {
	items []*queuedSchedule
	byKey map[scheduleKey][]*queuedSchedule
	bytes int
}

func (q *scheduleQueue) add(cmd pb.ScheduleCommand) bool {
	r := cmd.ConfigChange.Replica
	key := scheduleKey{cmd.UUID, r.UUID, r.ShardID, r.ReplicaID, r.Epoch, cmd.ConfigChange.ChangeType}
	for _, item := range q.byKey[key] {
		if proto.Equal(&item.cmd, &cmd) {
			return true
		}
	}
	size := cmd.ProtoSize()
	if len(q.items) >= maxPendingScheduleCommands || size > maxPendingScheduleBytes-q.bytes {
		return false
	}
	if q.byKey == nil {
		q.byKey = make(map[scheduleKey][]*queuedSchedule)
	}
	item := &queuedSchedule{key: key, cmd: cmd, size: size}
	q.byKey[key] = append(q.byKey[key], item)
	q.items = append(q.items, item)
	q.bytes += size
	return true
}

func (q *scheduleQueue) pop() {
	item := q.items[0]
	for i, candidate := range q.byKey[item.key] {
		if candidate == item {
			q.byKey[item.key] = append(q.byKey[item.key][:i], q.byKey[item.key][i+1:]...)
			break
		}
	}
	if len(q.byKey[item.key]) == 0 {
		delete(q.byKey, item.key)
	}
	q.bytes -= item.size
	q.items[0] = nil
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = nil
	}
}

func (q *scheduleQueue) rotate() {
	first := q.items[0]
	copy(q.items, q.items[1:])
	q.items[len(q.items)-1] = first
}
