package state

import (
	"context"
	"database/sql"
	"errors"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

type nodeProject struct {
	config  *pb.ProjectConfig
	ack     *pb.ProjectAck
	applied *pb.ProjectAck
}

func readNodeProjects(ctx context.Context, tx *sql.Tx, nodeID string) (entries []nodeProject, err error) {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM node_projects").Scan(&count); err != nil {
		return nil, err
	}
	if count > maxProjectsPerNode {
		return nil, errors.New("stored node project count exceeds limit")
	}
	rows, err := tx.QueryContext(ctx, `SELECT project_id, ack IS NULL, applied_ack IS NULL,
		CASE WHEN typeof(config) = 'blob' AND length(config) BETWEEN 1 AND 32768 THEN config ELSE NULL END,
		CASE WHEN typeof(ack) = 'blob' AND length(ack) BETWEEN 1 AND 32768 THEN ack ELSE NULL END,
		CASE WHEN typeof(applied_ack) = 'blob' AND length(applied_ack) BETWEEN 1 AND 32768 THEN applied_ack ELSE NULL END
		FROM node_projects ORDER BY project_id LIMIT 129`)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	entries = make([]nodeProject, 0)
	for rows.Next() {
		var id string
		var ackNull, appliedNull bool
		var configBytes, ackBytes, appliedBytes []byte
		if err := rows.Scan(&id, &ackNull, &appliedNull, &configBytes, &ackBytes, &appliedBytes); err != nil {
			return nil, err
		}
		entry := nodeProject{config: new(pb.ProjectConfig)}
		if err := decodeProject(configBytes, entry.config); err != nil {
			return nil, err
		}
		if err := validateProjectConfig(entry.config); err != nil {
			return nil, err
		}
		if id != entry.config.ProjectId || nodeID != entry.config.NodeInstanceId {
			return nil, errors.New("stored node project identity mismatch")
		}
		if !ackNull {
			entry.ack = new(pb.ProjectAck)
			if err := decodeProject(ackBytes, entry.ack); err != nil {
				return nil, err
			}
			if err := validateProjectAck(entry.ack); err != nil {
				return nil, err
			}
			if entry.ack.ProjectId != id || entry.ack.Generation > entry.config.Generation {
				return nil, errors.New("stored node project acknowledgement revision mismatch")
			}
		}
		if !appliedNull {
			entry.applied = new(pb.ProjectAck)
			if err := decodeProject(appliedBytes, entry.applied); err != nil {
				return nil, err
			}
			if err := validateProjectAck(entry.applied); err != nil {
				return nil, err
			}
			if entry.applied.Status != "applied" || entry.applied.ProjectId != id ||
				entry.ack == nil || entry.applied.Generation > entry.ack.Generation {
				return nil, errors.New("stored project ownership acknowledgement is invalid")
			}
		}
		if entry.ack != nil && entry.ack.Status == "applied" && !proto.Equal(entry.applied, entry.ack) {
			return nil, errors.New("stored project ownership acknowledgement disagrees with latest outcome")
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// SaveProjectConfig must commit BEFORE applying a configuration. The last-seen
// revision fences stale deliveries even when validation/application failed.
// Historical acknowledgements survive revision changes for root ownership
// recovery, not as evidence that the new desired revision is ready.
func (j *NodeJournal) SaveProjectConfig(ctx context.Context, config *pb.ProjectConfig) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	if err := validateProjectConfig(config); err != nil {
		return err
	}
	if config.NodeInstanceId != j.nodeID {
		return ErrProjectConflict
	}
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entries, err := readNodeProjects(ctx, tx, j.nodeID)
		if err != nil {
			return err
		}
		found := false
		for _, entry := range entries {
			if entry.config.ProjectId != config.ProjectId {
				continue
			}
			found = true
			if config.Generation < entry.config.Generation ||
				(config.Generation == entry.config.Generation && !proto.Equal(config, entry.config)) {
				return ErrProjectConflict
			}
			if config.Generation == entry.config.Generation {
				return nil
			}
		}
		if !found && len(entries) >= maxProjectsPerNode {
			return ErrProjectCapacity
		}
		payload, err := marshalProject(config)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO node_projects(project_id, config) VALUES (?, ?)
			ON CONFLICT(project_id) DO UPDATE SET config = excluded.config`, config.ProjectId, payload)
		return err
	})
}

// SaveProjectAck persists the latest local validation outcome after the config
// was saved. Older/future ACKs and changed same-revision applied identities are
// conflicts. Invalid observations may change when a fresh stream revalidates.
// Callers serialize apply/persistence and fence same-stream duplicates. Outcomes are
// retained separately for ownership recovery, even after a later invalid ACK.
func (j *NodeJournal) SaveProjectAck(ctx context.Context, ack *pb.ProjectAck) error {
	if err := j.store.enter(ctx); err != nil {
		return err
	}
	defer j.store.leave()
	if err := validateProjectAck(ack); err != nil {
		return err
	}
	return j.store.transaction(ctx, func(tx *sql.Tx) error {
		entries, err := readNodeProjects(ctx, tx, j.nodeID)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.config.ProjectId != ack.ProjectId {
				continue
			}
			if ack.Generation != entry.config.Generation || projectAckConflict(entry.ack, ack) {
				return ErrProjectConflict
			}
			payload, err := marshalProject(ack)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE node_projects SET ack = ?,
				applied_ack = CASE WHEN ? = 'applied' THEN ? ELSE applied_ack END WHERE project_id = ?`,
				payload, ack.Status, payload, ack.ProjectId)
			return err
		}
		return ErrProjectNotFound
	})
}

func (j *NodeJournal) projectEntries(ctx context.Context) ([]nodeProject, error) {
	if err := j.store.enter(ctx); err != nil {
		return nil, err
	}
	defer j.store.leave()
	var entries []nodeProject
	err := j.store.transaction(ctx, func(tx *sql.Tx) (err error) {
		entries, err = readNodeProjects(ctx, tx, j.nodeID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// ProjectConfigs returns last-seen desired revisions, not applied state.
func (j *NodeJournal) ProjectConfigs(ctx context.Context) ([]*pb.ProjectConfig, error) {
	entries, err := j.projectEntries(ctx)
	if err != nil {
		return nil, err
	}
	configs := make([]*pb.ProjectConfig, 0, len(entries))
	for _, entry := range entries {
		configs = append(configs, entry.config)
	}
	return configs, nil
}

// ProjectAcks returns retained local outcomes, which may precede cached desired
// revisions. They do not confer readiness on a restarted node or stream.
func (j *NodeJournal) ProjectAcks(ctx context.Context) ([]*pb.ProjectAck, error) {
	entries, err := j.projectEntries(ctx)
	if err != nil {
		return nil, err
	}
	acks := make([]*pb.ProjectAck, 0, len(entries))
	for _, entry := range entries {
		if entry.ack != nil {
			acks = append(acks, entry.ack)
		}
	}
	return acks, nil
}

// ProjectAppliedAcks returns the last successful outcome per project for managed
// root ownership recovery. A later invalid ACK does not destroy this evidence.
// These historical outcomes never establish current readiness.
func (j *NodeJournal) ProjectAppliedAcks(ctx context.Context) ([]*pb.ProjectAck, error) {
	entries, err := j.projectEntries(ctx)
	if err != nil {
		return nil, err
	}
	acks := make([]*pb.ProjectAck, 0, len(entries))
	for _, entry := range entries {
		if entry.applied != nil {
			acks = append(acks, entry.applied)
		}
	}
	return acks, nil
}
