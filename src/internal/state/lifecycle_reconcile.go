package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func finishLifecycleRecord(ctx context.Context, tx *sql.Tx, record *pb.CommandRecord, result *pb.CommandResult, now time.Time) error {
	if proto.Equal(recordResult(record), result) {
		return nil
	}
	if record.Status != statusRunning && record.Status != statusIndeterminate {
		return ErrCommandConflict
	}
	if result.AgentLifecycle != nil {
		if err := protocol.ValidateLifecycleAdvance(record.AgentLifecycle, result.AgentLifecycle, record.Command); err != nil {
			return fmt.Errorf("%w: %v", ErrCommandConflict, err)
		}
		record.AgentLifecycle = proto.Clone(result.AgentLifecycle).(*pb.AgentLifecycleReceipt)
	} else if record.AgentLifecycle != nil {
		return ErrCommandConflict
	}
	if record.Status != result.Status || record.Detail != result.Detail {
		return transitionCommand(ctx, tx, record, result.Status, result.Detail, now)
	}
	if err := validateCommandRecord(record); err != nil {
		return err
	}
	payload, err := marshalCommandMessage(record)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE commands SET record = ? WHERE command_id = ?", payload, result.CommandId)
	return err
}
