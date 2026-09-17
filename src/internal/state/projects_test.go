package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func projectRequest(nodeID, projectID string) *pb.RegisterProjectRequest {
	return &pb.RegisterProjectRequest{NodeInstanceId: nodeID, ProjectId: projectID,
		CheckoutPath: `C:\repos\` + projectID, WorktreeRoot: `C:\worktrees\` + projectID}
}

func appliedProject(config *pb.ProjectConfig) *pb.ProjectAck {
	return &pb.ProjectAck{ProjectId: config.ProjectId, Generation: config.Generation, Status: "applied",
		CheckoutPath: config.CheckoutPath, WorktreeRoot: config.WorktreeRoot}
}

func TestProjectDesiredAckRestart(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	request := projectRequest("offline-node", "project")
	record, err := s.UpsertProject(ctx, request)
	requireOK(t, err)
	if record.Desired.Generation != 1 || record.Readiness != "pending" {
		t.Fatal("initial desired revision must be pending")
	}
	if rowCount(t, s, "SELECT count(*) FROM bindings") != 0 {
		t.Fatal("offline project registration created a node binding")
	}
	ack := appliedProject(record.Desired)
	record, err = s.AckProject(ctx, request.NodeInstanceId, ack)
	requireOK(t, err)
	if record.Readiness != "applied" || !proto.Equal(record.Applied, ack) {
		t.Fatal("matching acknowledgement was not applied")
	}
	record, err = s.UpsertProject(ctx, request)
	requireOK(t, err)
	if record.Desired.Generation != 1 || record.Readiness != "applied" {
		t.Fatal("identical upsert reset revision or readiness")
	}
	request.WorktreeRoot = ""
	record, err = s.UpsertProject(ctx, request)
	requireOK(t, err)
	if record.Desired.Generation != 2 || record.Readiness != "pending" || !proto.Equal(record.Applied, ack) {
		t.Fatal("changed revision lost historical acknowledgement or became ready")
	}
	stale, err := s.AckProject(ctx, request.NodeInstanceId, ack)
	requireOK(t, err)
	if !proto.Equal(stale, record) {
		t.Fatal("stale acknowledgement changed desired readiness")
	}
	future := appliedProject(record.Desired)
	future.Generation++
	if _, err := s.AckProject(ctx, request.NodeInstanceId, future); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("future acknowledgement accepted: %v", err)
	}
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	got, err := s.GetProject(ctx, request.NodeInstanceId, request.ProjectId)
	requireOK(t, err)
	if !proto.Equal(got, record) {
		t.Fatal("desired and historical applied state did not survive restart")
	}
	got.Desired.CheckoutPath = "caller changed this"
	got, err = s.GetProject(ctx, request.NodeInstanceId, request.ProjectId)
	requireOK(t, err)
	if !proto.Equal(got, record) {
		t.Fatal("caller mutation escaped into state")
	}
	for _, nodeID := range []string{"", request.NodeInstanceId, "unknown"} {
		list, err := s.ListProjects(ctx, nodeID)
		requireOK(t, err)
		want := 1
		if nodeID == "unknown" {
			want = 0
		}
		if len(list) != want {
			t.Fatalf("list %q: got %d", nodeID, len(list))
		}
	}
	if _, err := s.GetProject(ctx, "unknown", "project"); !errors.Is(err, ErrProjectNotFound) {
		t.Fatalf("missing mapping: %v", err)
	}
}

func TestProjectAckRevalidationAndConflicts(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	record, err := s.UpsertProject(ctx, projectRequest("node", "project"))
	requireOK(t, err)
	applied := appliedProject(record.Desired)
	// Node canonicalization is not required to preserve the desired path spelling.
	applied.CheckoutPath = "/canonical/project"
	invalid := &pb.ProjectAck{ProjectId: "project", Generation: 1, Status: "invalid", ErrorCode: "path_missing"}
	for _, ack := range []*pb.ProjectAck{invalid, invalid, applied, applied, invalid, applied} {
		got, err := s.AckProject(ctx, "node", ack)
		requireOK(t, err)
		if got.Readiness != ack.Status || !proto.Equal(got.Applied, ack) {
			t.Fatal("same-revision revalidation did not replace status")
		}
	}
	conflict := proto.Clone(applied).(*pb.ProjectAck)
	conflict.WorktreeRoot += "-changed"
	if _, err := s.AckProject(ctx, "node", conflict); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("conflicting duplicate accepted: %v", err)
	}
	_, err = s.AckProject(ctx, "node", invalid)
	requireOK(t, err)
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	conflict = proto.Clone(invalid).(*pb.ProjectAck)
	conflict.ErrorCode = "different_failure"
	updated, err := s.AckProject(ctx, "node", conflict)
	requireOK(t, err)
	if updated.Readiness != "invalid" || !proto.Equal(updated.Applied, conflict) {
		t.Fatal("fresh invalid revalidation was not persisted")
	}
	if _, err := s.AckProject(ctx, "other-node", applied); !errors.Is(err, ErrProjectNotFound) {
		t.Fatalf("cross-node acknowledgement accepted: %v", err)
	}
}

func TestProjectAdoptionOnceAndAtomic(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	existing := projectRequest("node", "existing")
	_, err := s.UpsertProject(ctx, existing)
	requireOK(t, err)
	offered := proto.Clone(existing).(*pb.RegisterProjectRequest)
	offered.CheckoutPath = "/local/different"
	fresh := projectRequest("node", "fresh")
	equal := projectRequest("node", "equal")
	_, err = s.UpsertProject(ctx, equal)
	requireOK(t, err)
	records, err := s.AdoptProjects(ctx, "node", []*pb.RegisterProjectRequest{offered, fresh, equal})
	requireOK(t, err)
	if records[0].AdoptionStatus != "conflict" || records[0].Desired.CheckoutPath != existing.CheckoutPath ||
		records[1].AdoptionStatus != "adopted" || records[2].AdoptionStatus != "adopted" {
		t.Fatal("adoption overwrote operator desired state or recorded the wrong decision")
	}
	for _, offer := range []*pb.RegisterProjectRequest{existing, fresh, equal} {
		offer.CheckoutPath = "/operator/new/" + offer.ProjectId
		record, err := s.UpsertProject(ctx, offer)
		requireOK(t, err)
		if record.Desired.Generation != 2 {
			t.Fatal("operator update did not advance adopted mapping")
		}
	}
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	before, err := s.ListProjects(ctx, "node")
	requireOK(t, err)
	offered.CheckoutPath = "/yet/another/local/path"
	fresh.CheckoutPath = "/stale/local"
	_, err = s.AdoptProjects(ctx, "node", []*pb.RegisterProjectRequest{offered, fresh, equal})
	requireOK(t, err)
	after, err := s.ListProjects(ctx, "node")
	requireOK(t, err)
	for i := range before {
		if !proto.Equal(before[i], after[i]) {
			t.Fatal("repeated adoption competed after operator update/restart")
		}
	}
	newOffer := projectRequest("node", "atomic")
	if _, err := s.AdoptProjects(ctx, "node", []*pb.RegisterProjectRequest{newOffer, newOffer}); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("duplicate adoption IDs: %v", err)
	}
	newOfferOther := projectRequest("other", "other")
	if _, err := s.AdoptProjects(ctx, "node", []*pb.RegisterProjectRequest{newOffer, newOfferOther}); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("cross-node adoption: %v", err)
	}
	if _, err := s.GetProject(ctx, "node", "atomic"); !errors.Is(err, ErrProjectNotFound) {
		t.Fatal("invalid batch partly committed")
	}
}

func TestProjectBoundsValidationAndGenerationOverflow(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, testPath(t))
	for name, mutate := range map[string]func(*pb.RegisterProjectRequest){
		"node":           func(r *pb.RegisterProjectRequest) { r.NodeInstanceId = "bad node" },
		"project":        func(r *pb.RegisterProjectRequest) { r.ProjectId = "../project" },
		"empty":          func(r *pb.RegisterProjectRequest) { r.CheckoutPath = "" },
		"checkout-bound": func(r *pb.RegisterProjectRequest) { r.CheckoutPath = strings.Repeat("x", 4097) },
		"root-bound":     func(r *pb.RegisterProjectRequest) { r.WorktreeRoot = strings.Repeat("x", 4097) },
		"nul":            func(r *pb.RegisterProjectRequest) { r.CheckoutPath = "a\x00b" },
		"newline":        func(r *pb.RegisterProjectRequest) { r.WorktreeRoot = "a\nb" },
		"unknown":        func(r *pb.RegisterProjectRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			request := projectRequest("node", "project")
			mutate(request)
			if _, err := s.UpsertProject(ctx, request); err == nil {
				t.Fatal("invalid project accepted")
			}
		})
	}
	if _, err := s.UpsertProject(ctx, nil); err == nil {
		t.Fatal("nil registration accepted")
	}
	request := projectRequest("node", "project")
	request.CheckoutPath, request.WorktreeRoot = strings.Repeat("x", 4096), strings.Repeat("y", 4096)
	record, err := s.UpsertProject(ctx, request)
	requireOK(t, err)
	_, err = s.AckProject(ctx, "node", appliedProject(record.Desired))
	requireOK(t, err)
	for _, invalid := range []*pb.ProjectAck{
		nil, {ProjectId: "project", Generation: 1, Status: "invalid"},
		{ProjectId: "project", Generation: 1, Status: "applied", CheckoutPath: "x", ErrorCode: "bad"},
		{ProjectId: "project", Generation: 1, Status: "invalid", ErrorCode: "raw error: private path"},
		{ProjectId: "project", Status: "applied", CheckoutPath: "x"},
	} {
		if _, err := s.AckProject(ctx, "node", invalid); err == nil {
			t.Fatal("invalid acknowledgement accepted")
		}
	}
	record.Applied = nil
	record.Readiness = "pending"
	record.Desired.Generation = math.MaxUint64
	requireOK(t, s.transaction(ctx, func(tx *sql.Tx) error { return writeProject(ctx, tx, record) }))
	got, err := s.UpsertProject(ctx, request)
	requireOK(t, err)
	if got.Desired.Generation != math.MaxUint64 {
		t.Fatal("uint64 generation was truncated")
	}
	request.CheckoutPath = "/changed"
	if _, err := s.UpsertProject(ctx, request); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("generation wrapped: %v", err)
	}
}

func TestProjectCapacityAndRollback(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, testPath(t))
	requireOK(t, s.transaction(ctx, func(tx *sql.Tx) error {
		for i := 0; i < maxProjectsPerNode-1; i++ {
			config, err := projectOffer(projectRequest("node", fmt.Sprintf("p%03d", i)))
			if err != nil {
				return err
			}
			if err := writeProject(ctx, tx, &pb.ProjectRecord{Desired: config, Readiness: "pending"}); err != nil {
				return err
			}
		}
		return nil
	}))
	_, err := s.AdoptProjects(ctx, "node", []*pb.RegisterProjectRequest{
		projectRequest("node", "new1"), projectRequest("node", "new2"),
	})
	if !errors.Is(err, ErrProjectCapacity) {
		t.Fatalf("oversized batch accepted: %v", err)
	}
	if _, err := s.GetProject(ctx, "node", "new1"); !errors.Is(err, ErrProjectNotFound) {
		t.Fatal("capacity failure committed half the adoption batch")
	}
	_, err = s.UpsertProject(ctx, projectRequest("node", "last"))
	requireOK(t, err)
	if _, err := s.UpsertProject(ctx, projectRequest("node", "overflow")); !errors.Is(err, ErrProjectCapacity) {
		t.Fatalf("node project capacity not enforced: %v", err)
	}
	update := projectRequest("node", "last")
	update.CheckoutPath = "/updated-at-capacity"
	_, err = s.UpsertProject(ctx, update)
	requireOK(t, err)
	// Seed a valid bounded fleet transactionally, avoiding thousands of fsyncs.
	requireOK(t, s.transaction(ctx, func(tx *sql.Tx) error {
		for n := 1; n < 128; n++ {
			for p := 0; p < maxProjectsPerNode; p++ {
				config, err := projectOffer(projectRequest(fmt.Sprintf("n%03d", n), fmt.Sprintf("p%03d", p)))
				if err != nil {
					return err
				}
				if err := writeProject(ctx, tx, &pb.ProjectRecord{Desired: config, Readiness: "pending"}); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	if _, err := s.UpsertProject(ctx, projectRequest("new-node", "overflow")); !errors.Is(err, ErrProjectCapacity) {
		t.Fatalf("fleet project capacity not enforced: %v", err)
	}
	_, err = s.UpsertProject(ctx, update)
	requireOK(t, err)
}

func TestProjectNodeCacheRestartAndOwnership(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	j := openTestNodeJournal(t, path)
	config, err := projectOffer(projectRequest("node", "project"))
	requireOK(t, err)
	if err := j.SaveProjectAck(ctx, appliedProject(config)); !errors.Is(err, ErrProjectNotFound) {
		t.Fatalf("ack saved without last-seen config: %v", err)
	}
	requireOK(t, j.SaveProjectConfig(ctx, config))
	requireOK(t, j.SaveProjectConfig(ctx, config))
	applied := appliedProject(config)
	requireOK(t, j.SaveProjectAck(ctx, applied))
	config.Generation = 3
	config.CheckoutPath = "/new-desired-that-fails-validation"
	requireOK(t, j.SaveProjectConfig(ctx, config))
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	configs, err := j.ProjectConfigs(ctx)
	requireOK(t, err)
	acks, err := j.ProjectAcks(ctx)
	requireOK(t, err)
	if len(configs) != 1 || !proto.Equal(configs[0], config) || len(acks) != 1 || !proto.Equal(acks[0], applied) {
		t.Fatal("restart lost last seen or historical applied state")
	}
	stale := proto.Clone(config).(*pb.ProjectConfig)
	stale.Generation--
	if err := j.SaveProjectConfig(ctx, stale); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("restart accepted rollback: %v", err)
	}
	conflict := proto.Clone(config).(*pb.ProjectConfig)
	conflict.WorktreeRoot = "/conflicting-root"
	if err := j.SaveProjectConfig(ctx, conflict); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("same-revision conflicting configuration accepted: %v", err)
	}
	conflict = proto.Clone(config).(*pb.ProjectConfig)
	conflict.NodeInstanceId = "other"
	if err := j.SaveProjectConfig(ctx, conflict); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("foreign node configuration accepted: %v", err)
	}
	if err := j.SaveProjectAck(ctx, applied); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("old ACK accepted: %v", err)
	}
	future := appliedProject(config)
	future.Generation++
	if err := j.SaveProjectAck(ctx, future); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("future ACK accepted: %v", err)
	}
	invalid := &pb.ProjectAck{ProjectId: "project", Generation: 3, Status: "invalid", ErrorCode: "path_missing"}
	requireOK(t, j.SaveProjectAck(ctx, invalid))
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	acks, err = j.ProjectAcks(ctx)
	requireOK(t, err)
	ownership, err := j.ProjectAppliedAcks(ctx)
	requireOK(t, err)
	if len(acks) != 1 || !proto.Equal(acks[0], invalid) || len(ownership) != 1 || !proto.Equal(ownership[0], applied) {
		t.Fatal("invalid latest ACK erased historical root ownership")
	}
	requireOK(t, j.SaveProjectAck(ctx, appliedProject(config)))
	changedApplied := appliedProject(config)
	changedApplied.WorktreeRoot += "-changed"
	if err := j.SaveProjectAck(ctx, changedApplied); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("changed applied identity accepted: %v", err)
	}
	requireOK(t, j.SaveProjectAck(ctx, invalid))
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	bad := proto.Clone(invalid).(*pb.ProjectAck)
	bad.ErrorCode = "other"
	requireOK(t, j.SaveProjectAck(ctx, bad))
	acks, err = j.ProjectAcks(ctx)
	requireOK(t, err)
	if len(acks) != 1 || !proto.Equal(acks[0], bad) {
		t.Fatal("fresh invalid revalidation was not persisted")
	}
	requireOK(t, j.store.transaction(ctx, func(tx *sql.Tx) error {
		for p := 1; p < maxProjectsPerNode; p++ {
			c, err := projectOffer(projectRequest("node", fmt.Sprintf("p%03d", p)))
			if err != nil {
				return err
			}
			payload, err := marshalProject(c)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO node_projects(project_id, config) VALUES (?, ?)", c.ProjectId, payload); err != nil {
				return err
			}
		}
		return nil
	}))
	c, err := projectOffer(projectRequest("node", "overflow"))
	requireOK(t, err)
	if err := j.SaveProjectConfig(ctx, c); !errors.Is(err, ErrProjectCapacity) {
		t.Fatalf("node cache capacity not enforced: %v", err)
	}
	requireOK(t, j.SaveProjectConfig(ctx, config))
}

func TestProjectMigrationPreservesAgentJournalBytes(t *testing.T) {
	ctx := context.Background()
	path, nodePath := testPath(t), testPath(t)
	s, j := commandStoreAtPath(t, path), openTestNodeJournal(t, nodePath)
	command := agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
	admitWorkspace(t, s, command, true)
	_, claimed, err := j.Claim(ctx, command)
	requireOK(t, err)
	if !claimed {
		t.Fatal("fixture claim failed")
	}
	requireOK(t, j.Complete(ctx, agentResult(command)))
	_, err = s.FinishCommand(ctx, "node", "stable", agentResult(command), commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	requireOK(t, j.Acknowledge(ctx, command.CommandId, statusSucceeded))
	requireOK(t, s.SaveNode(ctx, node("stable", "node")))
	var recordBytes, commandBytes, resultBytes, fleetBytes []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands").Scan(&recordBytes))
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&fleetBytes))
	requireOK(t, j.store.conn.QueryRowContext(ctx, "SELECT command, result FROM node_commands").Scan(&commandBytes, &resultBytes))
	removeLifecycleSchema(t, s, coordinatorKind)
	removeLifecycleSchema(t, j.store, nodeKind)
	_, err = s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 5")
	requireOK(t, err)
	_, err = j.store.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 4")
	requireOK(t, err)
	requireOK(t, s.Close())
	requireOK(t, j.Close())
	s, j = openTestStore(t, path), openTestNodeJournal(t, nodePath)
	if rowCount(t, s, "PRAGMA user_version") != 7 || rowCount(t, j.store, "PRAGMA user_version") != 6 {
		t.Fatal("project migration did not fence old binaries")
	}
	var gotRecord, gotCommand, gotResult, gotFleet []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands").Scan(&gotRecord))
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&gotFleet))
	requireOK(t, j.store.conn.QueryRowContext(ctx, "SELECT command, result FROM node_commands").Scan(&gotCommand, &gotResult))
	if !bytes.Equal(gotRecord, recordBytes) || !bytes.Equal(gotCommand, commandBytes) ||
		!bytes.Equal(gotResult, resultBytes) || !bytes.Equal(gotFleet, fleetBytes) ||
		rowCount(t, j.store, "SELECT delivered FROM node_commands") != 1 {
		t.Fatal("migration rewrote retained command, result, ACK or fleet bytes")
	}
	got, err := s.GetOperatorCommand(ctx, command.CommandId)
	requireOK(t, err)
	actorGot, err := s.GetCommand(ctx, "actor", command.CommandId)
	requireOK(t, err)
	if !proto.Equal(got, actorGot) {
		t.Fatal("operator lookup changed command record")
	}
	if _, err := s.GetCommand(ctx, "other-actor", command.CommandId); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("actor lookup crossed namespace: %v", err)
	}
	if _, err := s.GetOperatorCommand(ctx, "missing"); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("operator missing lookup: %v", err)
	}
}

func TestProjectCorruptionFailsClosed(t *testing.T) {
	for _, nodeJournal := range []bool{false, true} {
		for _, fault := range []string{"payload", "schema", "ownership"} {
			if !nodeJournal && fault == "ownership" {
				continue
			}
			t.Run(fmt.Sprintf("node=%v/%s", nodeJournal, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				var s *Store
				owner, kind, table, column := "server", coordinatorKind, "projects", "record"
				if nodeJournal {
					j := openTestNodeJournal(t, path)
					s, owner, kind, table, column = j.store, "node", nodeKind, "node_projects", "config"
					config, err := projectOffer(projectRequest("node", "project"))
					requireOK(t, err)
					requireOK(t, j.SaveProjectConfig(ctx, config))
					requireOK(t, j.SaveProjectAck(ctx, appliedProject(config)))
				} else {
					s = openTestStore(t, path)
					_, err := s.UpsertProject(ctx, projectRequest("node", "project"))
					requireOK(t, err)
				}
				statement := "UPDATE " + table + " SET " + column + " = x'ff'"
				if fault == "schema" {
					statement = "DROP TABLE " + table
				} else if fault == "ownership" {
					statement = "UPDATE node_projects SET applied_ack = NULL"
				}
				_, err := s.conn.ExecContext(ctx, statement)
				requireOK(t, err)
				requireOK(t, s.Close())
				before, err := os.ReadFile(path)
				requireOK(t, err)
				opened, err := openStore(ctx, path, owner, kind)
				if opened != nil {
					requireOK(t, opened.Close())
				}
				if err == nil || errors.Is(err, ErrProjectConflict) {
					t.Fatalf("corrupt project storage accepted/masked: %v", err)
				}
				after, err := os.ReadFile(path)
				requireOK(t, err)
				if !bytes.Equal(before, after) {
					t.Fatal("failed open rewrote corrupt database")
				}
			})
		}
	}
}
