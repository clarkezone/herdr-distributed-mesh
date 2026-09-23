package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

var (
	ErrProjectNotFound = errors.New("project not found")
	ErrProjectConflict = errors.New("project conflict")
	ErrProjectCapacity = errors.New("project capacity reached")
)

const (
	maxProjectsPerNode = 128
	maxProjects        = 16384
	maxProjectBytes    = 32768
)

const projectsSchema = `CREATE TABLE projects (
	node_id TEXT NOT NULL,
	project_id TEXT NOT NULL,
	record BLOB NOT NULL CHECK (length(record) BETWEEN 1 AND 32768),
	PRIMARY KEY (node_id, project_id)
) STRICT`

const nodeProjectsSchema = `CREATE TABLE node_projects (
	project_id TEXT PRIMARY KEY NOT NULL,
	config BLOB NOT NULL CHECK (length(config) BETWEEN 1 AND 32768),
	ack BLOB CHECK (length(ack) BETWEEN 1 AND 32768),
	applied_ack BLOB CHECK (length(applied_ack) BETWEEN 1 AND 32768)
) STRICT`

// Paths remain opaque to state: only the node can validate local filesystem
// semantics. Empty worktree roots select the node's default sibling directory.
func validProjectPath(path string, optional bool) bool {
	return (optional || strings.TrimSpace(path) != "") && len(path) <= 4096 &&
		utf8.ValidString(path) && !strings.ContainsFunc(path, unicode.IsControl)
}

func validateProjectConfig(config *pb.ProjectConfig) error {
	if config == nil || !protocol.ValidIdempotencyKey(config.NodeInstanceId) ||
		!protocol.ValidIdempotencyKey(config.ProjectId) || config.Generation == 0 ||
		!validProjectPath(config.CheckoutPath, false) || !validProjectPath(config.WorktreeRoot, true) ||
		unknownFields(config.ProtoReflect()) {
		return errors.New("invalid project configuration")
	}
	return nil
}

func projectOffer(request *pb.RegisterProjectRequest) (*pb.ProjectConfig, error) {
	if request == nil || unknownFields(request.ProtoReflect()) {
		return nil, errors.New("invalid project registration")
	}
	config := &pb.ProjectConfig{NodeInstanceId: request.NodeInstanceId, ProjectId: request.ProjectId,
		Generation: 1, CheckoutPath: request.CheckoutPath, WorktreeRoot: request.WorktreeRoot}
	return config, validateProjectConfig(config)
}

func validateProjectAck(ack *pb.ProjectAck) error {
	if ack == nil || !protocol.ValidIdempotencyKey(ack.ProjectId) || ack.Generation == 0 ||
		!validProjectPath(ack.CheckoutPath, ack.Status == "invalid") || !validProjectPath(ack.WorktreeRoot, true) ||
		unknownFields(ack.ProtoReflect()) {
		return errors.New("invalid project acknowledgement")
	}
	if (ack.Status == "applied" && ack.ErrorCode == "") ||
		(ack.Status == "invalid" && protocol.ValidIdempotencyKey(ack.ErrorCode)) {
		return nil
	}
	return errors.New("invalid project acknowledgement status or error category")
}

func marshalProject(message proto.Message) ([]byte, error) {
	if proto.Size(message) > maxProjectBytes || unknownFields(message.ProtoReflect()) {
		return nil, errors.New("project payload exceeds bounds or has unknown fields")
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func decodeProject(payload []byte, message proto.Message) error {
	if len(payload) == 0 || len(payload) > maxProjectBytes {
		return errors.New("stored project payload exceeds bounds")
	}
	if err := (proto.UnmarshalOptions{RecursionLimit: 16}).Unmarshal(payload, message); err != nil {
		return fmt.Errorf("decode stored project: %w", err)
	}
	if unknownFields(message.ProtoReflect()) {
		return errors.New("stored project has unknown fields")
	}
	return nil
}

func validateProjectRecord(record *pb.ProjectRecord) error {
	if err := validateProjectConfig(record.Desired); err != nil {
		return err
	}
	if record.AdoptionStatus != "" && record.AdoptionStatus != "adopted" && record.AdoptionStatus != "conflict" {
		return errors.New("stored project has invalid adoption marker")
	}
	readiness := "pending"
	if record.Applied != nil {
		if err := validateProjectAck(record.Applied); err != nil {
			return err
		}
		if record.Applied.ProjectId != record.Desired.ProjectId || record.Applied.Generation > record.Desired.Generation {
			return errors.New("stored project acknowledgement disagrees with desired revision")
		}
		if record.Applied.Generation == record.Desired.Generation {
			readiness = record.Applied.Status
		}
	}
	if record.Readiness != readiness {
		return errors.New("stored project readiness disagrees with acknowledgement")
	}
	return nil
}

func readProjects(ctx context.Context, tx *sql.Tx, nodeID, projectID string) (records []*pb.ProjectRecord, err error) {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM projects").Scan(&count); err != nil {
		return nil, err
	}
	if count > maxProjects {
		return nil, errors.New("stored fleet project count exceeds limit")
	}
	filter, args := "", []any{}
	if nodeID != "" {
		filter, args = " WHERE node_id = ?", []any{nodeID}
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM projects WHERE node_id = ?", nodeID).Scan(&count); err != nil {
			return nil, err
		}
		if count > maxProjectsPerNode {
			return nil, errors.New("stored node project count exceeds limit")
		}
	}
	if projectID != "" {
		filter += " AND project_id = ?"
		args = append(args, projectID)
	}
	rows, err := tx.QueryContext(ctx, `SELECT node_id, project_id,
		CASE WHEN typeof(record) = 'blob' AND length(record) BETWEEN 1 AND 32768 THEN record ELSE NULL END
		FROM projects`+filter+" ORDER BY node_id, project_id LIMIT 16385", args...)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	records = make([]*pb.ProjectRecord, 0)
	counts := make(map[string]int)
	for rows.Next() {
		var node, project string
		var payload []byte
		if err := rows.Scan(&node, &project, &payload); err != nil {
			return nil, err
		}
		record := new(pb.ProjectRecord)
		if err := decodeProject(payload, record); err != nil {
			return nil, err
		}
		if err := validateProjectRecord(record); err != nil {
			return nil, err
		}
		counts[node]++
		if node != record.Desired.NodeInstanceId || project != record.Desired.ProjectId || counts[node] > maxProjectsPerNode {
			return nil, errors.New("stored project identity or count is invalid")
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func readProject(ctx context.Context, tx *sql.Tx, nodeID, projectID string) (*pb.ProjectRecord, error) {
	records, err := readProjects(ctx, tx, nodeID, projectID)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, ErrProjectNotFound
	}
	return records[0], nil
}

func writeProject(ctx context.Context, tx *sql.Tx, record *pb.ProjectRecord) error {
	if err := validateProjectRecord(record); err != nil {
		return err
	}
	payload, err := marshalProject(record)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO projects(node_id, project_id, record) VALUES (?, ?, ?)
		ON CONFLICT(node_id, project_id) DO UPDATE SET record = excluded.record`,
		record.Desired.NodeInstanceId, record.Desired.ProjectId, payload)
	return err
}

func newProject(ctx context.Context, tx *sql.Tx, config *pb.ProjectConfig) (*pb.ProjectRecord, error) {
	var total, nodeCount int
	if err := tx.QueryRowContext(ctx, "SELECT count(*), count(CASE WHEN node_id = ? THEN 1 END) FROM projects",
		config.NodeInstanceId).Scan(&total, &nodeCount); err != nil {
		return nil, err
	}
	if total >= maxProjects || nodeCount >= maxProjectsPerNode {
		return nil, ErrProjectCapacity
	}
	return &pb.ProjectRecord{Desired: config, Readiness: "pending"}, nil
}

func sameProjectPaths(a, b *pb.ProjectConfig) bool {
	return a.CheckoutPath == b.CheckoutPath && a.WorktreeRoot == b.WorktreeRoot
}

// UpsertProject stores offline desired state without requiring a node binding.
// Historical acknowledgement remains available, but a changed revision is pending.
func (s *Store) UpsertProject(ctx context.Context, request *pb.RegisterProjectRequest) (*pb.ProjectRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	config, err := projectOffer(request)
	if err != nil {
		return nil, err
	}
	var record *pb.ProjectRecord
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		record, err = readProject(ctx, tx, config.NodeInstanceId, config.ProjectId)
		if errors.Is(err, ErrProjectNotFound) {
			record, err = newProject(ctx, tx, config)
		} else if err == nil {
			if sameProjectPaths(record.Desired, config) {
				return nil
			}
			if record.Desired.Generation == math.MaxUint64 {
				return ErrProjectConflict
			}
			config.Generation = record.Desired.Generation + 1
			record.Desired, record.Readiness = config, "pending"
		}
		if err != nil {
			return err
		}
		return writeProject(ctx, tx, record)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) GetProject(ctx context.Context, nodeID, projectID string) (*pb.ProjectRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if !protocol.ValidIdempotencyKey(nodeID) || !protocol.ValidIdempotencyKey(projectID) {
		return nil, errors.New("invalid project identity")
	}
	var record *pb.ProjectRecord
	err := s.transaction(ctx, func(tx *sql.Tx) (err error) {
		record, err = readProject(ctx, tx, nodeID, projectID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ListProjects lists a node's projects, or the fleet when nodeID is empty.
func (s *Store) ListProjects(ctx context.Context, nodeID string) ([]*pb.ProjectRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if nodeID != "" && !protocol.ValidIdempotencyKey(nodeID) {
		return nil, errors.New("invalid node identity")
	}
	var records []*pb.ProjectRecord
	err := s.transaction(ctx, func(tx *sql.Tx) (err error) {
		records, err = readProjects(ctx, tx, nodeID, "")
		return err
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// Invalid observations can change on fresh-stream filesystem revalidation.
// Successful identities at the same revision must not change. The caller owns
// current-stream duplicate checks, readiness, and stale-stream fencing.
func projectAckConflict(previous, next *pb.ProjectAck) bool {
	return previous != nil && previous.Generation == next.Generation &&
		previous.Status == "applied" && next.Status == "applied" && !proto.Equal(previous, next)
}

func (s *Store) AckProject(ctx context.Context, nodeID string, ack *pb.ProjectAck) (*pb.ProjectRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if !protocol.ValidIdempotencyKey(nodeID) {
		return nil, errors.New("invalid node identity")
	}
	if err := validateProjectAck(ack); err != nil {
		return nil, err
	}
	var record *pb.ProjectRecord
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		record, err = readProject(ctx, tx, nodeID, ack.ProjectId)
		if err != nil {
			return err
		}
		if ack.Generation > record.Desired.Generation {
			return ErrProjectConflict
		}
		if ack.Generation < record.Desired.Generation {
			return nil
		}
		if projectAckConflict(record.Applied, ack) {
			return ErrProjectConflict
		}
		record.Applied, record.Readiness = proto.Clone(ack).(*pb.ProjectAck), ack.Status
		return writeProject(ctx, tx, record)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

// AdoptProjects atomically consumes each mapping's first local offer. Conflict
// markers are returned as records, not errors, and survive later operator updates.
// Subsequent offers never change desired state or the persisted adoption decision.
func (s *Store) AdoptProjects(ctx context.Context, nodeID string, offers []*pb.RegisterProjectRequest) ([]*pb.ProjectRecord, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.leave()
	if !protocol.ValidIdempotencyKey(nodeID) {
		return nil, errors.New("invalid node identity")
	}
	if len(offers) > maxProjectsPerNode {
		return nil, ErrProjectCapacity
	}
	configs := make([]*pb.ProjectConfig, 0, len(offers))
	seen := make(map[string]bool)
	for _, offer := range offers {
		config, err := projectOffer(offer)
		if err != nil {
			return nil, err
		}
		if config.NodeInstanceId != nodeID || seen[config.ProjectId] {
			return nil, ErrProjectConflict
		}
		seen[config.ProjectId] = true
		configs = append(configs, config)
	}
	records := make([]*pb.ProjectRecord, 0, len(configs))
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		for _, config := range configs {
			record, err := readProject(ctx, tx, nodeID, config.ProjectId)
			if errors.Is(err, ErrProjectNotFound) {
				record, err = newProject(ctx, tx, config)
			}
			if err != nil {
				return err
			}
			if record.AdoptionStatus == "" {
				record.AdoptionStatus = "adopted"
				if !sameProjectPaths(record.Desired, config) {
					record.AdoptionStatus = "conflict"
				}
				if err := writeProject(ctx, tx, record); err != nil {
					return err
				}
			}
			records = append(records, record)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}
