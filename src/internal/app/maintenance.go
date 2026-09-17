package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

// RunMaintenance is the local-only router; args excludes "maintenance".
// It never initializes identity or tsnet.
func RunMaintenance(ctx context.Context, args []string, streams IO) error {
	if len(args) == 0 {
		return errors.New("maintenance requires inspect, backup, or verify-backup")
	}
	operation := args[0]
	if operation != "inspect" && operation != "backup" && operation != "verify-backup" {
		return errors.New("maintenance supports only inspect, backup, or verify-backup; no pruning or restore")
	}
	flags := flag.NewFlagSet("maintenance "+operation, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	role := flags.String("role", "", "local role: server or node")
	stateDir := flags.String("state-dir", "", "existing absolute role state directory (backup directory for verify-backup)")
	destination := flags.String("destination", "", "new absolute backup directory; backup only")
	asJSON := flags.Bool("json", false, "print bounded aggregate JSON")
	timeout := flags.Duration("timeout", 30*time.Second, "operation timeout, greater than zero and at most 2m")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*role != "server" && *role != "node") || *stateDir == "" ||
		*timeout <= 0 || *timeout > 2*time.Minute {
		return errors.New("maintenance requires -role server|node, -state-dir, no positional arguments, and a timeout in (0,2m]")
	}
	if (operation == "backup") != (*destination != "") {
		return errors.New("-destination is required only for backup and must not be supplied to other operations")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var result any
	var err error
	switch operation {
	case "inspect":
		result, err = state.InspectMaintenance(ctx, *stateDir, *role)
	case "backup":
		result, err = state.BackupMaintenance(ctx, *stateDir, *role, *destination)
	case "verify-backup":
		result, err = state.VerifyMaintenanceBackup(ctx, *stateDir, *role)
	}
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(streams.Out).Encode(result)
	}
	switch report := result.(type) {
	case *state.MaintenanceReport:
		_, err = fmt.Fprintf(streams.Out, "role=%s integrity=%s schema=%d commands=%d/%d remaining=%d full=%t accepted=%d running=%d indeterminate=%d pending_receipts=%d\nNo pruning or automatic recovery is supported; preserve all retry identities.\n",
			report.Role, report.Integrity, report.SchemaVersion, report.Commands.Used, report.Commands.Limit,
			report.Commands.Remaining, report.Commands.Full, report.Accepted, report.Running, report.Indeterminate, report.PendingReceipts)
	case *state.MaintenanceBackupReport:
		_, err = fmt.Fprintf(streams.Out, "role=%s backup_verified=%t files=%d bytes=%d destination=%q\nBackup integrity does not authorize restoring stale deduplication state; operator review is required.\n",
			report.Source.Role, report.Verified, report.Files, report.Bytes, report.Destination)
	default:
		return errors.New("maintenance returned an unsupported report")
	}
	return err
}
