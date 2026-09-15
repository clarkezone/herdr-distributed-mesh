package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "coordinator", "mesh.db")
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path, "server")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func node(stable, instance string) *agentflowv1.NodeView {
	return &agentflowv1.NodeView{
		InstanceId: instance, TailscaleStableId: stable,
		LastSeen: timestamppb.New(time.Unix(1700000000, 0)),
		Herdr:    &agentflowv1.HerdrState{Status: "ready", Sequence: 1},
	}
}

func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireConflict(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("want identity conflict, got %v", err)
	}
}

func rowCount(t *testing.T, s *Store, query string) int {
	t.Helper()
	var count int
	requireOK(t, s.conn.QueryRowContext(context.Background(), query).Scan(&count))
	return count
}

func TestDurableLatestViewAndBinding(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	requireOK(t, s.Bind(ctx, "stable", "instance"))
	view := node("stable", "instance")
	requireOK(t, s.SaveNode(ctx, view))
	view.Herdr.Sequence = 5
	view.Connected = true
	requireOK(t, s.SaveNode(ctx, view))
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	fleet, err := s.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != 1 || !proto.Equal(fleet[0], view) {
		t.Fatal("latest view did not survive close/reopen")
	}
	requireOK(t, s.Bind(ctx, "stable", "instance"))
	requireConflict(t, s.Bind(ctx, "stable", "other-instance"))
	requireConflict(t, s.Bind(ctx, "other-stable", "instance"))
	requireOK(t, s.DeleteNodes(ctx, []string{"instance", "missing"}))
	fleet, err = s.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != 0 {
		t.Fatal("snapshot was not deleted")
	}
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	requireConflict(t, s.Bind(ctx, "stable", "other-instance"))
	requireConflict(t, s.Bind(ctx, "other-stable", "instance"))
	requireOK(t, s.SaveNode(ctx, view))
}

func TestInitialHerdrStatesAndTransitions(t *testing.T) {
	for _, initial := range []string{"waiting", "disabled"} {
		t.Run(initial, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			s := openTestStore(t, path)
			requireOK(t, s.Bind(ctx, "stable", "instance"))
			view := node("stable", "instance")
			view.Herdr = &agentflowv1.HerdrState{Status: initial}
			view.Connected = true
			for _, status := range []string{initial, "ready", "unavailable"} {
				view.Herdr.Status = status
				if status != initial {
					view.Herdr.Sequence++
					view.Herdr.Version = "1.0.0"
					view.Herdr.Protocol = 1
					view.Herdr.ObservedAt = timestamppb.New(time.Unix(1700000001, 0))
					view.HerdrReceivedAt = timestamppb.New(time.Unix(1700000002, 0))
				}
				if status == "unavailable" {
					view.Herdr.ErrorCode = "offline"
				}
				requireOK(t, s.SaveNode(ctx, view))
				requireOK(t, s.Close())
				s = openTestStore(t, path)
				fleet, err := s.LoadFleet(ctx)
				requireOK(t, err)
				if len(fleet) != 1 || !proto.Equal(fleet[0], view) {
					t.Fatalf("%s snapshot did not survive close/reopen", status)
				}
			}
		})
	}
}

func TestImportAtomicIdempotent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, testPath(t))
	initial := []Binding{{"stable", "instance"}, {"stable", "instance"}}
	requireOK(t, s.ImportBindings(ctx, initial))
	requireOK(t, s.ImportBindings(ctx, initial))
	tests := []struct {
		name     string
		input    []Binding
		conflict bool
	}{
		{"existing stable", []Binding{{"new", "new"}, {"stable", "other"}}, true},
		{"existing instance", []Binding{{"new", "new"}, {"other", "instance"}}, true},
		{"input stable", []Binding{{"new", "new"}, {"new", "other"}}, true},
		{"input instance", []Binding{{"new", "new"}, {"other", "new"}}, true},
		{"empty stable", []Binding{{"new", "new"}, {"", "other"}}, false},
		{"empty instance", []Binding{{"new", "new"}, {"other", ""}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.ImportBindings(ctx, tt.input)
			if err == nil {
				t.Fatal("invalid import accepted")
			}
			if tt.conflict {
				requireConflict(t, err)
			}
			if got := rowCount(t, s, "SELECT count(*) FROM bindings"); got != 1 {
				t.Fatalf("partial import committed: %d bindings", got)
			}
		})
	}
}

func TestIdentityAndVersionMismatchDoNotChangeDatabase(t *testing.T) {
	ctx := context.Background()
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("version%d", version), func(t *testing.T) {
			path := testPath(t)
			s := openTestStore(t, path)
			requireOK(t, s.Bind(ctx, "stable", "instance"))
			if version == 3 {
				_, err := s.conn.ExecContext(ctx, "PRAGMA user_version = 3")
				requireOK(t, err)
			}
			requireOK(t, s.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			other, err := Open(ctx, path, "wrong-server")
			if err == nil {
				_ = other.Close()
				t.Fatal("incompatible database opened")
			}
			if version == 2 {
				requireConflict(t, err)
			}
			after, err := os.ReadFile(path)
			requireOK(t, err)
			if !bytes.Equal(before, after) {
				t.Fatal("failed startup modified database contents")
			}
			// A failed open must not retain ownership, even for a newer schema.
			retry, err := Open(ctx, path, "server")
			if errors.Is(err, ErrLocked) {
				t.Fatal("failed startup leaked lock")
			}
			if version == 2 {
				requireOK(t, err)
				requireOK(t, retry.Close())
			} else if err == nil {
				_ = retry.Close()
				t.Fatal("newer schema accepted")
			}
		})
	}
}

func TestCorruptDatabaseNotReset(t *testing.T) {
	path := testPath(t)
	requireOK(t, os.MkdirAll(filepath.Dir(path), 0700))
	bad := []byte("not a SQLite database; must remain intact")
	requireOK(t, os.WriteFile(path, bad, 0600))
	for i := 0; i < 2; i++ {
		s, err := Open(context.Background(), path, "server")
		if err == nil {
			_ = s.Close()
			t.Fatal("corrupt database opened")
		}
		if errors.Is(err, ErrLocked) {
			t.Fatal("failed startup leaked lock")
		}
		got, err := os.ReadFile(path)
		requireOK(t, err)
		if !bytes.Equal(got, bad) {
			t.Fatal("corrupt database was reset")
		}
	}
}

func TestExistingEmptyDatabaseNotInitialized(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(fmt.Sprintf("truncated=%t", truncated), func(t *testing.T) {
			path := testPath(t)
			if truncated {
				s := openTestStore(t, path)
				requireOK(t, s.Bind(context.Background(), "stable", "instance"))
				requireOK(t, s.SaveNode(context.Background(), node("stable", "instance")))
				requireOK(t, s.Close())
				requireOK(t, os.Truncate(path, 0))
			} else {
				requireOK(t, os.MkdirAll(filepath.Dir(path), 0700))
				requireOK(t, os.WriteFile(path, nil, 0600))
			}
			before, err := os.Stat(path)
			requireOK(t, err)
			for i := 0; i < 2; i++ {
				s, err := Open(context.Background(), path, "server")
				if s != nil {
					requireOK(t, s.Close())
				}
				if err == nil || !strings.Contains(err.Error(), "existing state database is empty") {
					t.Fatalf("expected explicit empty-database refusal, got %v", err)
				}
				if errors.Is(err, ErrLocked) {
					t.Fatal("failed open retained ownership")
				}
				after, err := os.Stat(path)
				requireOK(t, err)
				if after.Size() != 0 || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
					t.Fatal("existing empty database was modified or replaced")
				}
				for _, suffix := range []string{"-wal", "-shm", "-journal"} {
					if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("empty database opened SQLite sidecar %s: %v", suffix, err)
					}
				}
			}
		})
	}
}

func TestExistingUninitializedSchemaNotReset(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	for _, statement := range []string{
		"DROP TABLE latest_nodes", "DROP TABLE bindings", "DROP TABLE metadata", "PRAGMA user_version = 0",
	} {
		_, err := s.conn.ExecContext(ctx, statement)
		requireOK(t, err)
	}
	requireOK(t, s.Close())
	before, err := os.ReadFile(path)
	requireOK(t, err)
	if len(before) == 0 {
		t.Fatal("fixture must have a SQLite header")
	}
	reopened, err := Open(ctx, path, "server")
	if reopened != nil {
		requireOK(t, reopened.Close())
	}
	if err == nil || !strings.Contains(err.Error(), "existing state database has no initialized schema") {
		t.Fatalf("expected explicit uninitialized-schema refusal, got %v", err)
	}
	after, err := os.ReadFile(path)
	requireOK(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("existing uninitialized database was modified")
	}
}

func TestVersionOneMissingSchemaOrIdentityNotRepaired(t *testing.T) {
	for _, tt := range []struct {
		name      string
		statement string
		message   string
	}{
		{"metadata table", "DROP TABLE metadata", "required table metadata is missing"},
		{"bindings table", "DROP TABLE bindings", "required table bindings is missing"},
		{"latest nodes table", "DROP TABLE latest_nodes", "required table latest_nodes is missing"},
		{"server identity", "DELETE FROM metadata", "server identity is missing or invalid"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			s := openTestStore(t, path)
			requireOK(t, s.Bind(ctx, "stable", "instance"))
			requireOK(t, s.SaveNode(ctx, node("stable", "instance")))
			downgradeToVersionOne(t, s)
			_, err := s.conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF")
			requireOK(t, err)
			_, err = s.conn.ExecContext(ctx, tt.statement)
			requireOK(t, err)
			if version := rowCount(t, s, "PRAGMA user_version"); version != 1 {
				t.Fatalf("fixture must remain version 1, got %d", version)
			}
			requireOK(t, s.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			for _, serverID := range []string{"server", "different-server"} {
				reopened, err := Open(ctx, path, serverID)
				if reopened != nil {
					requireOK(t, reopened.Close())
				}
				if err == nil || !strings.Contains(err.Error(), tt.message) {
					t.Fatalf("expected corruption refusal %q, got %v", tt.message, err)
				}
				if errors.Is(err, ErrLocked) {
					t.Fatal("failed startup leaked ownership")
				}
				after, err := os.ReadFile(path)
				requireOK(t, err)
				if !bytes.Equal(before, after) {
					t.Fatal("version-1 database was modified or repaired")
				}
			}
		})
	}
}

func TestForeignBindingAndMalformedMetadata(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, testPath(t))
	requireOK(t, s.Bind(ctx, "stable", "instance"))
	requireConflict(t, s.SaveNode(ctx, node("other", "instance")))
	requireConflict(t, s.SaveNode(ctx, node("stable", "other")))
	requireConflict(t, s.SaveNode(ctx, node("unbound", "unbound")))
	tests := []*agentflowv1.NodeView{
		nil, {}, node("", "instance"), node("stable", ""),
		{InstanceId: "instance", TailscaleStableId: "stable"},
		{InstanceId: "instance", TailscaleStableId: "stable", LastSeen: &timestamppb.Timestamp{Nanos: -1}},
	}
	badReceipt := node("stable", "instance")
	badReceipt.HerdrReceivedAt = &timestamppb.Timestamp{Seconds: 253402300800}
	tests = append(tests, badReceipt)
	unknown := node("stable", "instance")
	unknown.Herdr.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1))
	tests = append(tests, unknown)
	for i, view := range tests {
		if err := s.SaveNode(ctx, view); err == nil {
			t.Fatalf("malformed node %d accepted", i)
		}
	}
	for _, binding := range []Binding{{"", "instance"}, {"stable", ""}, {" ", "instance"}} {
		if err := s.Bind(ctx, binding.StableID, binding.InstanceID); err == nil {
			t.Fatal("empty binding ID accepted")
		}
	}
	if got := rowCount(t, s, "SELECT count(*) FROM latest_nodes"); got != 0 {
		t.Fatal("invalid save persisted")
	}
	requireOK(t, s.SaveNode(ctx, node("stable", "instance")))
	if err := s.DeleteNodes(ctx, []string{"instance", ""}); err == nil {
		t.Fatal("empty delete ID accepted")
	}
	if got := rowCount(t, s, "SELECT count(*) FROM latest_nodes"); got != 1 {
		t.Fatal("invalid delete committed partially")
	}
}

func TestStoredProtobufAndBindingCorruption(t *testing.T) {
	for _, kind := range []string{"wire", "unknown", "nested unknown", "instance", "stable", "metadata", "oversized", "empty", "orphan"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			s := openTestStore(t, path)
			requireOK(t, s.Bind(ctx, "stable", "instance"))
			view := node("stable", "instance")
			var payload []byte
			switch kind {
			case "wire":
				payload = []byte{0xff}
			case "unknown":
				view.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1))
			case "nested unknown":
				view.Herdr.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1))
			case "instance":
				view.InstanceId = "other"
			case "stable":
				view.TailscaleStableId = "other"
			case "metadata":
				view.LastSeen = nil
			case "oversized":
				payload = bytes.Repeat([]byte{0}, maxNodeBytes+1)
			case "empty":
				payload = []byte{}
			}
			if payload == nil {
				var err error
				payload, err = proto.Marshal(view)
				requireOK(t, err)
			}
			_, err := s.conn.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON")
			requireOK(t, err)
			_, err = s.conn.ExecContext(ctx, "INSERT INTO latest_nodes(instance_id, payload) VALUES (?, ?)", "instance", payload)
			requireOK(t, err)
			if kind == "orphan" {
				_, err = s.conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF")
				requireOK(t, err)
				_, err = s.conn.ExecContext(ctx, "DELETE FROM bindings")
				requireOK(t, err)
			}
			fleet, err := s.LoadFleet(ctx)
			if err == nil || fleet != nil {
				t.Fatal("corrupt stored node produced a successful/partial fleet")
			}
			var after []byte
			requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&after))
			if !bytes.Equal(after, payload) {
				t.Fatal("corrupt payload was changed")
			}
			requireOK(t, s.Close())
			reopened, err := Open(ctx, path, "server")
			if err == nil {
				defer reopened.Close()
				fleet, err = reopened.LoadFleet(ctx)
				if err == nil || fleet != nil {
					t.Fatal("reopen accepted corrupt stored node")
				}
			} else if errors.Is(err, ErrLocked) {
				t.Fatal("ownership not released")
			}
		})
	}
}

func TestExactPayloadAndFleetBounds(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, testPath(t))
	view := node("stable", "instance")
	// The field's length varint changes as it grows, so solve for exact wire size.
	n := maxNodeBytes - proto.Size(view) - 4
	for {
		view.Herdr.ErrorCode = strings.Repeat("x", n)
		difference := maxNodeBytes - proto.Size(view)
		if difference == 0 {
			break
		}
		n += difference
		if n < 0 || n > maxNodeBytes {
			t.Fatal("cannot construct boundary payload")
		}
	}
	requireOK(t, s.Bind(ctx, "stable", "instance"))
	requireOK(t, s.SaveNode(ctx, view))
	fleet, err := s.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != 1 || proto.Size(fleet[0]) != maxNodeBytes {
		t.Fatal("exact 260 KiB boundary did not round-trip")
	}
	view.Herdr.ErrorCode += "x"
	if proto.Size(view) != maxNodeBytes+1 {
		t.Fatal("incorrect oversized test fixture")
	}
	if err := s.SaveNode(ctx, view); err == nil {
		t.Fatal("260 KiB plus one accepted")
	}
	for i := 1; i < maxNodes; i++ {
		id := fmt.Sprintf("node-%03d", i)
		requireOK(t, s.Bind(ctx, id, id))
		requireOK(t, s.SaveNode(ctx, node(id, id)))
	}
	fleet, err = s.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != maxNodes {
		t.Fatalf("want 128 nodes, got %d", len(fleet))
	}
	requireOK(t, s.SaveNode(ctx, node("stable", "instance")))
	requireOK(t, s.Bind(ctx, "overflow", "overflow"))
	if err := s.SaveNode(ctx, node("overflow", "overflow")); err == nil {
		t.Fatal("129th node accepted")
	}
	payload, err := proto.Marshal(node("overflow", "overflow"))
	requireOK(t, err)
	_, err = s.conn.ExecContext(ctx, "INSERT INTO latest_nodes(instance_id, payload) VALUES (?, ?)", "overflow", payload)
	requireOK(t, err)
	if fleet, err = s.LoadFleet(ctx); err == nil || fleet != nil {
		t.Fatal("oversized stored fleet accepted")
	}
	requireOK(t, s.DeleteNodes(ctx, []string{"overflow"}))
	_, err = s.LoadFleet(ctx)
	requireOK(t, err)
}

func TestContextsAndCloseSerialization(t *testing.T) {
	setup, cancelSetup := context.WithCancel(context.Background())
	path := testPath(t)
	s, err := Open(setup, path, "server")
	requireOK(t, err)
	t.Cleanup(func() { requireOK(t, s.Close()) })
	cancelSetup()
	ctx := context.Background()
	requireOK(t, s.Bind(ctx, "stable", "instance"))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	operations := []func(context.Context) error{
		func(ctx context.Context) error { return s.Bind(ctx, "a", "b") },
		func(ctx context.Context) error { return s.ImportBindings(ctx, nil) },
		func(ctx context.Context) error { return s.SaveNode(ctx, node("stable", "instance")) },
		func(ctx context.Context) error { return s.DeleteNodes(ctx, nil) },
		func(ctx context.Context) error { _, err := s.LoadFleet(ctx); return err },
	}
	for _, operation := range operations {
		if err := operation(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled operation returned %v", err)
		}
	}
	if opened, err := Open(canceled, testPath(t), "server"); !errors.Is(err, context.Canceled) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("canceled open returned %v", err)
	}
	requireOK(t, s.enter(ctx))
	waiting, cancelWaiting := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- s.Bind(waiting, "waiting", "waiting") }()
	cancelWaiting()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("waiting operation returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("waiting operation ignored cancellation")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		s.leave()
		t.Fatalf("Close did not wait for an active operation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	s.leave()
	select {
	case err := <-closed:
		requireOK(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	requireOK(t, s.Close())
	for _, operation := range operations {
		if err := operation(ctx); err == nil {
			t.Fatal("closed store accepted an operation")
		}
	}
	reopened := openTestStore(t, path)
	requireOK(t, reopened.Bind(ctx, "stable", "instance"))
}

func TestPathValidation(t *testing.T) {
	for _, path := range []string{"", " ", ":memory:", "file:mesh.db", "file::memory:?cache=shared", "https://example.invalid/state"} {
		s, err := Open(context.Background(), path, "server")
		if err == nil {
			_ = s.Close()
			t.Fatalf("invalid path %q accepted", path)
		}
	}
	if s, err := Open(context.Background(), testPath(t), ""); err == nil {
		_ = s.Close()
		t.Fatal("empty server identity accepted")
	}
}

func TestExclusiveOwnershipAndCrashRecovery(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	for _, identity := range []string{"server", "different-server"} {
		start := time.Now()
		second, err := Open(ctx, path, identity)
		if err == nil {
			_ = second.Close()
			t.Fatal("second live owner accepted")
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("second owner returned %v", err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("ownership refusal waited for busy timeout")
		}
	}
	runChild(t, path, "locked")
	requireOK(t, s.Close())
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Fatal("ownership file must remain after release")
	}
	runChild(t, path, "crash")
	s = openTestStore(t, path)
	fleet, err := s.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != 1 || !proto.Equal(fleet[0], node("child-stable", "child-instance")) {
		t.Fatal("committed WAL write was lost after child exited without Close")
	}
	requireOK(t, s.Bind(ctx, "child-stable", "child-instance"))
	requireConflict(t, s.Bind(ctx, "child-stable", "other"))
}

func runChild(t *testing.T, path, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStateOwnedChild$")
	command.Env = append(os.Environ(), "HERDR_STATE_TEST_CHILD="+mode, "HERDR_STATE_TEST_DB="+path)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("owned child %s: %v\n%s", mode, err, output)
	}
}

// This subprocess is always an owned copy of this test binary, never a mesh role.
func TestStateOwnedChild(t *testing.T) {
	mode := os.Getenv("HERDR_STATE_TEST_CHILD")
	if mode == "" {
		return
	}
	ctx := context.Background()
	s, err := Open(ctx, os.Getenv("HERDR_STATE_TEST_DB"), "server")
	if mode == "locked" {
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("child expected lock refusal, got %v", err)
		}
		return
	}
	if mode != "crash" {
		t.Fatal("unknown child mode")
	}
	requireOK(t, err)
	requireOK(t, s.Bind(ctx, "child-stable", "child-instance"))
	requireOK(t, s.SaveNode(ctx, node("child-stable", "child-instance")))
	// os.Exit skips Store.Close, Go defers and SQLite shutdown/checkpoint.
	os.Exit(0)
}
