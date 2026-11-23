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
	"fmt"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/logreplicas"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestHAKeeperPolicyTickEncoding(t *testing.T) {
	hb := pb.LogStoreHeartbeat{UUID: "a"}
	data, err := hb.Marshal()
	require.NoError(t, err)
	heartbeat := GetLogStoreHeartbeatCmd(data)
	require.Equal(t, pb.GuardedLogHeartbeatUpdate, parseCmdTag(heartbeat))
	require.Equal(t, data, heartbeat[headerSize:])
	cmd := GetTickCmd(2, 19)
	require.Equal(t, pb.TickUpdate, parseCmdTag(cmd))
	require.Equal(t, uint64(2), binaryEnc.Uint64(cmd[headerSize:]))
	require.Equal(t, uint64(19), binaryEnc.Uint64(cmd[headerSize+8:]))
	for n := headerSize + 1; n < len(cmd); n++ {
		s := NewStateMachine(0, 1)
		_, err := s.Update(sm.Entry{Index: 1, Cmd: cmd[:n]})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "length %d: %v", n, err)
	}
	s := NewStateMachine(0, 1)
	_, err = s.Update(sm.Entry{Index: 1, Cmd: GetTickCmd(0, 19)})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}

func TestHAKeeperPolicyTickSurvivesRecoveryAndRejectsStaleTerms(t *testing.T) {
	s := NewStateMachine(0, 1).(*stateMachine)
	apply := func(cmd []byte) sm.Result {
		t.Helper()
		result, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: cmd})
		require.NoError(t, err)
		return result
	}
	// A released tick advances time without inventing an expiry policy.
	apply(getHeartbeatCmd(nil, pb.TickUpdate))
	require.Equal(t, uint64(1), s.state.Tick)
	require.Zero(t, s.state.LogStoreTimeoutPolicyTerm)
	result := apply(GetTickCmd(2, 20))
	require.Zero(t, result.Value)
	require.Equal(t, uint64(2), s.state.LogStoreTimeoutPolicyTerm)
	require.Equal(t, uint64(20), s.state.LogStoreTimeoutPolicyTicks)
	// Older and conflicting same-term proposals still tick, but cannot change
	// the policy already selected for the newer leader's term.
	apply(GetTickCmd(1, 50))
	apply(GetTickCmd(2, 50))
	require.Equal(t, uint64(4), s.state.Tick)
	require.Equal(t, uint64(20), s.state.LogStoreTimeoutPolicyTicks)
	s = runtimeRestore(t, s)
	state, err := s.Lookup(&StateQuery{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.(*pb.CheckerState).LogStoreTimeoutPolicyTerm)
	require.Equal(t, uint64(20), state.(*pb.CheckerState).LogStoreTimeoutPolicyTicks)
	apply(getHeartbeatCmd(nil, pb.TickUpdate))
	require.Equal(t, uint64(20), s.state.LogStoreTimeoutPolicyTicks)
	apply(GetTickCmd(3, 0))
	require.Equal(t, uint64(3), s.state.LogStoreTimeoutPolicyTerm)
	require.Zero(t, s.state.LogStoreTimeoutPolicyTicks)

	pending := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "pending"}}
	apply(GetUpdateCommandsCmd(3, []pb.ScheduleCommand{pending}))
	apply(GetUpdateCommandsCmd(2, []pb.ScheduleCommand{{UUID: "b", ServiceType: pb.LogService}}))
	require.Equal(t, []pb.ScheduleCommand{pending}, s.state.ScheduleCommands["a"].Commands)
	require.NotContains(t, s.state.ScheduleCommands, "b")
}

func TestGuardedHeartbeatRetainsCapabilityBlockedAdmission(t *testing.T) {
	s := NewStateMachine(0, 1).(*stateMachine)
	s.state.CommandDeliveryEnabled = true
	installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
	shard := pb.LogShardInfo{ShardID: DefaultHAKeeperShardID, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: shard.ShardID, NumberOfReplicas: 3}}
	s.state.NonVotingReplicaNum = 1
	reportA := pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 1}
	unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{reportA}, CommandDeliverySupported: true})
	unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "d"})
	add := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.AddNonVotingReplica, Replica: pb.Replica{UUID: "d", ShardID: DefaultHAKeeperShardID, ReplicaID: 4, Epoch: 7},
	}}
	other := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
	unhappyQueue(t, s, add, other)
	queued := s.state.ScheduleCommands["a"]
	require.False(t, s.logMembershipDeliverable("a", add))
	hb := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{reportA}, CommandDeliverySupported: true}
	got := membershipHeartbeat(t, s, hb, true)
	require.Equal(t, []pb.ScheduleCommand{other}, got.Commands)
	require.Equal(t, queued.CommandIDs[1:], got.CommandIDs)
	pending := s.state.ScheduleCommands["a"]
	require.Equal(t, []pb.ScheduleCommand{add}, pending.Commands)
	require.Equal(t, queued.CommandIDs[:1], pending.CommandIDs)

	// Once the target understands the protocol and a voting majority reports,
	// the retained command remains deliverable without a new scheduling round.
	unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "d", CommandDeliverySupported: true})
	unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "b", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}, CommandDeliverySupported: true})
	got = membershipHeartbeat(t, s, hb, true)
	require.Equal(t, []pb.ScheduleCommand{add}, got.Commands)
	require.Equal(t, queued.CommandIDs[:1], got.CommandIDs)
}

func TestGuardedHeartbeatBeforeCommittedPolicy(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		for _, withdrawn := range []bool{false, true} {
			t.Run(fmt.Sprintf("ack=%t/withdrawn=%t", acknowledged, withdrawn), func(t *testing.T) {
				s := NewStateMachine(0, 1).(*stateMachine)
				s.state.CommandDeliveryEnabled = acknowledged
				shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
				s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
				unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "b", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}, CommandDeliverySupported: true})
				for range 2 {
					_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: getHeartbeatCmd(nil, pb.TickUpdate)})
					require.NoError(t, err)
				}
				otherShard := pb.LogShardInfo{ShardID: 2, Epoch: 3, Replicas: map[uint64]string{11: "a"}}
				aReports := []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}, {LogShardInfo: otherShard, ReplicaID: 11}}
				unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "a", Replicas: aReports, CommandDeliverySupported: true})
				unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "c", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}, CommandDeliverySupported: true})
				remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
					ChangeType: pb.RemoveReplica, Replica: pb.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 7},
				}}
				start := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
					ChangeType: pb.StartReplica, Replica: pb.Replica{UUID: "a", ShardID: 2, ReplicaID: 11},
				}}
				unhappyQueue(t, s, remove, start)
				queued := s.state.ScheduleCommands["a"]
				if withdrawn {
					unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "b", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}, CommandDeliverySupported: true})
					unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "c", CommandDeliverySupported: true})
				}
				hb := pb.LogStoreHeartbeat{UUID: "a", Replicas: aReports, CommandDeliverySupported: true}
				got := membershipHeartbeat(t, s, hb, true)
				require.Equal(t, []pb.ScheduleCommand{start}, got.Commands)
				pending := s.state.ScheduleCommands["a"]
				require.Equal(t, []pb.ScheduleCommand{remove}, pending.Commands)
				if acknowledged {
					require.Equal(t, queued.CommandIDs[1:], got.CommandIDs)
					require.Equal(t, queued.CommandIDs[:1], pending.CommandIDs)
				}
				// The held command and its identity survive snapshot recovery and
				// are rechecked only after a replicated timeout is available.
				s = runtimeRestore(t, s)
				require.Empty(t, membershipHeartbeat(t, s, hb, true).Commands)
				installMembershipPolicy(t, s, 1)
				got = membershipHeartbeat(t, s, hb, true)
				if withdrawn {
					require.Empty(t, got.Commands)
				} else {
					require.Equal(t, []pb.ScheduleCommand{remove}, got.Commands)
					if acknowledged {
						require.Equal(t, queued.CommandIDs[:1], got.CommandIDs)
					}
				}
				require.NotContains(t, s.state.ScheduleCommands, "a")
			})
		}
	}
}

func TestLegacyPolicyActivationKeepsReplacementSemantics(t *testing.T) {
	s := NewStateMachine(0, 1).(*stateMachine)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}}
	unhappyHeartbeat(t, s, a)
	unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "c", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}})
	remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.RemoveReplica, Replica: pb.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 7},
	}}
	unhappyQueue(t, s, remove)
	require.Empty(t, membershipHeartbeat(t, s, a, true).Commands)
	installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
	// After policy activation the legacy producer can replace the queue with a
	// new checker batch, exactly as it did before the upgrade.
	other := pb.ScheduleCommand{UUID: "tn", ServiceType: pb.TNService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
	unhappyQueue(t, s, other)
	s = runtimeRestore(t, s)
	require.Empty(t, membershipHeartbeat(t, s, a, true).Commands)
	require.Equal(t, []pb.ScheduleCommand{other}, s.state.ScheduleCommands["tn"].Commands)
}

func TestGuardedHeartbeatBeforePolicyRejectsInvalidMembership(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*stateMachine, *pb.ScheduleCommand)
	}{
		{"stale epoch", func(_ *stateMachine, cmd *pb.ScheduleCommand) { cmd.ConfigChange.Replica.Epoch-- }},
		{"wrong voting role", func(s *stateMachine, _ *pb.ScheduleCommand) {
			c := s.state.LogState.Stores["c"]
			c.Replicas[0].IsNonVoting = true
			s.state.LogState.Stores["c"] = c
		}},
		{"different store incarnation", func(s *stateMachine, _ *pb.ScheduleCommand) {
			shard := s.state.LogState.Shards[1]
			shard.ReplicaStoreIncarnations[3] = "old"
			s.state.LogState.Shards[1] = shard
			c := s.state.LogState.Stores["c"]
			c.StoreIncarnation = "new"
			s.state.LogState.Stores["c"] = c
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStateMachine(0, 1).(*stateMachine)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}}
			unhappyHeartbeat(t, s, a)
			unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "c", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}})
			remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
				ChangeType: pb.RemoveReplica, Replica: pb.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 7},
			}}
			other := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
			tc.mutate(s, &remove)
			unhappyQueue(t, s, remove, other)
			require.Equal(t, []pb.ScheduleCommand{other}, membershipHeartbeat(t, s, a, true).Commands)
			require.Equal(t, []pb.ScheduleCommand{remove}, s.state.ScheduleCommands["a"].Commands)
			installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
			require.Empty(t, membershipHeartbeat(t, s, a, true).Commands)
			require.NotContains(t, s.state.ScheduleCommands, "a")
		})
	}
}

func TestLogHeartbeatRechecksMembershipBeforeDelivery(t *testing.T) {
	for _, tc := range []struct {
		change pb.ConfigChangeType
	}{
		{pb.RemoveReplica},
		{pb.AddReplica},
		{pb.RemoveNonVotingReplica},
		{pb.AddNonVotingReplica},
	} {
		for _, acknowledged := range []bool{false, true} {
			for _, rotate := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/ack=%t/rotated=%t", tc.change, acknowledged, rotate), func(t *testing.T) {
					s := NewStateMachine(0, 1).(*stateMachine)
					s.state.CommandDeliveryEnabled = acknowledged
					installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
					shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
					wanted := uint64(3)
					if tc.change == pb.RemoveReplica {
						wanted = 2
					} else if tc.change == pb.AddReplica {
						wanted = 4
					} else if tc.change == pb.AddNonVotingReplica {
						s.state.NonVotingReplicaNum = 1
					}
					s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: shard.ShardID, NumberOfReplicas: wanted}}
					if tc.change == pb.RemoveNonVotingReplica {
						shard.NonVotingReplicas = map[uint64]string{4: "d"}
					}
					other := pb.LogShardInfo{ShardID: 2, Epoch: 3, Replicas: map[uint64]string{11: "a"}}
					aReports := []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}, {LogShardInfo: other, ReplicaID: 11}}
					heartbeat := func(uuid string, replicas ...pb.LogReplicaInfo) {
						t.Helper()
						unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: uuid, Replicas: replicas, CommandDeliverySupported: true})
					}
					heartbeat("a", aReports...)
					if tc.change != pb.RemoveReplica {
						heartbeat("b", pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 2})
					}
					if tc.change == pb.RemoveReplica || tc.change == pb.AddReplica {
						heartbeat("c", pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 3})
					}
					if tc.change == pb.RemoveNonVotingReplica {
						heartbeat("d", pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 4, IsNonVoting: true})
					} else {
						heartbeat("d")
					}

					target := pb.Replica{UUID: "d", ShardID: 1, ReplicaID: 4, Epoch: 7}
					if tc.change == pb.RemoveReplica {
						target.UUID, target.ReplicaID = "b", 2
					}
					membership := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: tc.change, Replica: target}}
					start := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{UUID: "a", ShardID: 2, ReplicaID: 11}}}
					cfg := Config{}
					cfg.Fill()
					require.Contains(t, LogMembershipTargets(cfg, s.state.LogState, s.state.Tick, tc.change, target), "a")
					unhappyQueue(t, s, membership, start)
					queued := s.state.ScheduleCommands["a"]
					if rotate {
						switch tc.change {
						case pb.RemoveReplica:
							heartbeat("b", pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 2})
							heartbeat("c")
						case pb.AddReplica:
							heartbeat("c")
						default:
							heartbeat("b")
						}
						require.Empty(t, LogMembershipTargets(cfg, s.state.LogState, s.state.Tick, tc.change, target))
					}
					hb := pb.LogStoreHeartbeat{UUID: "a", Replicas: aReports, CommandDeliverySupported: true}
					data, err := hb.Marshal()
					require.NoError(t, err)
					result, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetLogStoreHeartbeatCmd(data)})
					require.NoError(t, err)
					var got pb.CommandBatch
					require.NoError(t, got.Unmarshal(result.Data))
					require.Equal(t, queued.BatchID, got.BatchID)
					if rotate {
						require.Equal(t, []pb.ScheduleCommand{start}, got.Commands)
						if acknowledged {
							require.Equal(t, queued.CommandIDs[1:], got.CommandIDs)
						}
						require.Zero(t, result.Value)
					} else {
						require.Equal(t, []pb.ScheduleCommand{membership, start}, got.Commands)
						require.Equal(t, queued.CommandIDs, got.CommandIDs)
					}
					require.NotContains(t, s.state.ScheduleCommands, "a")
				})
			}
		}
	}
}

func TestLogHeartbeatMembershipPolicyPreservesHistoricalReplay(t *testing.T) {
	for _, tc := range []struct {
		name       string
		guarded    bool
		policy     bool
		timeout    uint64
		tick       uint64
		wantRemove bool
	}{
		{"released heartbeat", false, false, 0, 3, true},
		{"before policy, age is unknown", true, false, 0, 3, false},
		{"zero timeout is guarded", true, true, 0, 1, false},
		{"expiry boundary", true, true, 2, 2, true},
		{"past expiry", true, true, 2, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStateMachine(0, 1).(*stateMachine)
			s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 2}}
			if tc.policy {
				installMembershipPolicy(t, s, tc.timeout)
			}
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			report := func(uuid string, id uint64) pb.LogStoreHeartbeat {
				return pb.LogStoreHeartbeat{UUID: uuid, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
			}
			unhappyHeartbeat(t, s, report("a", 1))
			unhappyHeartbeat(t, s, report("c", 3))
			remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: pb.RemoveReplica, Replica: pb.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 7}}}
			unhappyQueue(t, s, remove)
			s.state.Tick += tc.tick
			s = runtimeRestore(t, s)
			hb := report("a", 1)
			got := membershipHeartbeat(t, s, hb, tc.guarded)
			if tc.wantRemove {
				require.Equal(t, []pb.ScheduleCommand{remove}, got.Commands)
			} else {
				require.Empty(t, got.Commands)
			}
		})
	}
}

func TestLogHeartbeatUsesReplicatedTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout uint64
		want    bool
	}{
		{"leader timeout expires reporter", 20, false},
		{"leader timeout retains reporter", 50, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStateMachine(0, 1).(*stateMachine)
			s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 2}}
			installMembershipPolicy(t, s, tc.timeout)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}}
			unhappyHeartbeat(t, s, a)
			unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "c", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}})
			remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
				ChangeType: pb.RemoveReplica, Replica: pb.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 7},
			}}
			unhappyQueue(t, s, remove)
			s.state.Tick += 25
			// Replicas restored from the same committed state must make the same
			// decision without consulting the receiving server's local config.
			follower := runtimeRestore(t, s)
			leaderResult := membershipHeartbeat(t, s, a, true)
			followerResult := membershipHeartbeat(t, follower, a, true)
			require.Equal(t, leaderResult, followerResult)
			if tc.want {
				require.Equal(t, []pb.ScheduleCommand{remove}, leaderResult.Commands)
			} else {
				require.Empty(t, leaderResult.Commands)
			}
		})
	}
}

func TestLogHeartbeatMembershipGuardPrecedesCatalogReservation(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(fmt.Sprintf("guarded=%t", guarded), func(t *testing.T) {
			s := catalogRuntimeFixture(t)
			add := unhappyCommand(pb.AddReplica, 2, "new-log")
			unhappyQueue(t, s, add)
			s = runtimeRestore(t, s)
			hb := unhappyLogHeartbeat(s, "inc")
			if guarded {
				installMembershipPolicy(t, s, 5)
			}
			got := membershipHeartbeat(t, s, hb, guarded)
			reservation := s.state.CatalogMetadataBarrier.Arbitration.Reservation
			if guarded {
				require.Empty(t, got.Commands)
				require.Nil(t, reservation)
				return
			}
			require.Len(t, got.Commands, 1)
			require.NotNil(t, reservation)
			require.Equal(t, reservation.Token, got.Commands[0].CatalogMetadataMembership.Token)
			// A previously issued token remains owned, but an unsafe reoffer is
			// filtered. The periodic reconciler retains its existing retry contract.
			unhappyQueue(t, s, add)
			installMembershipPolicy(t, s, 5)
			got = membershipHeartbeat(t, s, hb, true)
			require.Empty(t, got.Commands)
			require.Equal(t, reservation.Token, s.state.CatalogMetadataBarrier.Arbitration.Reservation.Token)
		})
	}
}

func TestMissingPolicyDoesNotReserveCatalogMembership(t *testing.T) {
	s := catalogRuntimeFixture(t)
	add := unhappyCommand(pb.AddReplica, 2, "new-log")
	unhappyQueue(t, s, add)
	s = runtimeRestore(t, s)
	require.Empty(t, membershipHeartbeat(t, s, unhappyLogHeartbeat(s, "inc"), true).Commands)
	require.Equal(t, []pb.ScheduleCommand{add}, s.state.ScheduleCommands["log"].Commands)
	require.Nil(t, s.state.CatalogMetadataBarrier.Arbitration.Reservation)
}

func membershipHeartbeat(t *testing.T, s *stateMachine, hb pb.LogStoreHeartbeat, guarded bool) pb.CommandBatch {
	t.Helper()
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd := getHeartbeatCmd(data, pb.LogHeartbeatUpdate)
	if guarded {
		cmd = GetLogStoreHeartbeatCmd(data)
	}
	result, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: cmd})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	return batch
}

func installMembershipPolicy(t *testing.T, s *stateMachine, timeout uint64) {
	t.Helper()
	_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetTickCmd(s.state.Term+1, timeout)})
	require.NoError(t, err)
}

func TestQueuedNonVotingRemovalRechecksDesiredCount(t *testing.T) {
	for _, mode := range []string{"legacy", "preparing", "enabled"} {
		for _, desired := range []uint64{0, 1} {
			for _, interleave := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/desired=%d/interleave=%t", mode, desired, interleave), func(t *testing.T) {
					s := NewStateMachine(0, 1).(*stateMachine)
					s.state.CommandDeliveryPreparing = mode == "preparing"
					s.state.CommandDeliveryEnabled = mode == "enabled"
					s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 1}}
					shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a"}, NonVotingReplicas: map[uint64]string{2: "d"}}
					a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}, CommandDeliverySupported: true}
					unhappyHeartbeat(t, s, a)
					unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "d", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2, IsNonVoting: true}}, CommandDeliverySupported: true})
					remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: pb.RemoveNonVotingReplica, Replica: pb.Replica{ShardID: 1, Epoch: 7, UUID: "d", ReplicaID: 2}}}
					task := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
					unhappyQueue(t, s, remove, task)
					queued := s.state.ScheduleCommands["a"]
					before := membershipHeartbeat(t, s, a, true)
					require.Equal(t, []pb.ScheduleCommand{task}, before.Commands)
					pending := s.state.ScheduleCommands["a"]
					require.Equal(t, []pb.ScheduleCommand{remove}, pending.Commands)
					if mode != "legacy" {
						require.Equal(t, queued.CommandIDs[1:], before.CommandIDs)
						require.Equal(t, queued.CommandIDs[:1], pending.CommandIDs)
					}
					_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetUpdateNonVotingReplicaNumCmd(desired)})
					require.NoError(t, err)
					installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
					if interleave {
						unhappyQueue(t, s, pb.ScheduleCommand{UUID: "tn", ServiceType: pb.TNService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}})
					}
					s = runtimeRestore(t, s)
					got := membershipHeartbeat(t, s, a, true)
					if desired == 0 && !(interleave && mode == "legacy") {
						require.Equal(t, []pb.ScheduleCommand{remove}, got.Commands)
						if mode != "legacy" {
							require.Equal(t, queued.CommandIDs[:1], got.CommandIDs)
						}
					} else {
						require.Empty(t, got.Commands)
					}
				})
			}
		}
	}
}

func TestQueuedRemovalIsDiscardedWhenVotingReplicaRecovers(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(fmt.Sprintf("ack=%t", acknowledged), func(t *testing.T) {
			s := NewStateMachine(0, 1).(*stateMachine)
			s.state.CommandDeliveryEnabled = acknowledged
			s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
			b := pb.LogStoreHeartbeat{UUID: "b", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}, CommandDeliverySupported: true}
			unhappyHeartbeat(t, s, b)
			for range 2 {
				_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: getHeartbeatCmd(nil, pb.TickUpdate)})
				require.NoError(t, err)
			}
			for id, uuid := range map[uint64]string{1: "a", 3: "c"} {
				unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: uuid, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}, CommandDeliverySupported: true})
			}
			remove := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: pb.RemoveReplica, Replica: pb.Replica{ShardID: 1, Epoch: 7, UUID: "b", ReplicaID: 2}}}
			unhappyQueue(t, s, remove)
			a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}, CommandDeliverySupported: true}
			require.Empty(t, membershipHeartbeat(t, s, a, true).Commands)
			unhappyHeartbeat(t, s, b)
			installMembershipPolicy(t, s, 1)
			require.Empty(t, membershipHeartbeat(t, s, a, true).Commands)
			require.NotContains(t, s.state.ScheduleCommands, "a")
		})
	}
}

func TestQueuedNonVotingAddRechecksLocality(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			s := NewStateMachine(0, 1).(*stateMachine)
			s.state.CommandDeliveryEnabled = true
			s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 1}}
			s.state.NonVotingReplicaNum = 1
			installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a"}}
			a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}, CommandDeliverySupported: true}
			unhappyHeartbeat(t, s, a)
			unhappyHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "d", Locality: pb.Locality{Value: map[string]string{"zone": "east"}}, CommandDeliverySupported: true})
			_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetUpdateNonVotingLocality(pb.Locality{Value: map[string]string{"zone": "east"}})})
			require.NoError(t, err)
			add := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: pb.AddNonVotingReplica, Replica: pb.Replica{ShardID: 1, Epoch: 7, UUID: "d", ReplicaID: 2}}}
			unhappyQueue(t, s, add)
			if changed {
				_, err = s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetUpdateNonVotingLocality(pb.Locality{Value: map[string]string{"zone": "west"}})})
				require.NoError(t, err)
			}
			s = runtimeRestore(t, s)
			got := membershipHeartbeat(t, s, a, true)
			if changed {
				require.Empty(t, got.Commands)
			} else {
				require.Equal(t, []pb.ScheduleCommand{add}, got.Commands)
			}
		})
	}
}

func TestReleasedShardRegistrationAndPendingUpgrade(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("ack=%t", enabled), func(t *testing.T) {
			s := NewStateMachine(0, 1).(*stateMachine)
			s.state.CommandDeliveryEnabled = enabled
			_, err := s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetInitialClusterRequestCmd(1, 1, 3, 0, nil, nil)})
			require.NoError(t, err)
			shard := pb.LogShardInfo{ShardID: 77, Epoch: 7, Replicas: map[uint64]string{1: "a"}}
			a := pb.LogStoreHeartbeat{UUID: "a", Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}, CommandDeliverySupported: true}
			unhappyHeartbeat(t, s, a)
			// A released entry for an already observed shard did not create a
			// desired record. Replaying it must retain that historical behavior.
			_, err = s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetAddLogShardCmd(pb.AddLogShard{ShardID: 77})})
			require.NoError(t, err)
			_, known := logreplicas.RecordForShard(77, s.state.ClusterInfo.LogShards)
			require.False(t, known)
			add := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, AddLogShard: &pb.AddLogShard{ShardID: 77}}
			other := pb.ScheduleCommand{UUID: "a", ServiceType: pb.LogService, CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
			unhappyQueue(t, s, add, other)
			queued := s.state.ScheduleCommands["a"]
			got := membershipHeartbeat(t, s, a, true)
			require.Equal(t, []pb.ScheduleCommand{other}, got.Commands)
			require.Equal(t, []pb.ScheduleCommand{add}, s.state.ScheduleCommands["a"].Commands)
			if enabled {
				require.Equal(t, queued.CommandIDs[1:], got.CommandIDs)
				require.Equal(t, queued.CommandIDs[:1], s.state.ScheduleCommands["a"].CommandIDs)
			}
			s = runtimeRestore(t, s)
			installMembershipPolicy(t, s, testLogStoreTimeoutTicks)
			require.Equal(t, []pb.ScheduleCommand{add}, membershipHeartbeat(t, s, a, true).Commands)
			observed := s.state.LogState.Shards[77]
			_, err = s.Update(sm.Entry{Index: s.state.Index + 1, Cmd: GetAddLogShardCmd(*add.AddLogShard)})
			require.NoError(t, err)
			record, known := logreplicas.RecordForShard(77, s.state.ClusterInfo.LogShards)
			require.True(t, known)
			require.Equal(t, uint64(3), record.NumberOfReplicas)
			require.Equal(t, observed, s.state.LogState.Shards[77])
		})
	}
}
