package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maintenanceMaxManifest = 32 << 20

type maintenanceManifest struct {
	Version int                `json:"version"`
	Role    string             `json:"role"`
	Report  MaintenanceReport  `json:"source_report"`
	Entries []maintenanceEntry `json:"entries"`
}

type MaintenanceBackupReport struct {
	Destination string            `json:"destination"`
	Verified    bool              `json:"verified"`
	Files       int               `json:"files"`
	Bytes       int64             `json:"bytes"`
	Source      MaintenanceReport `json:"source_report"`
	Recovery    string            `json:"recovery"`
}

// BackupMaintenance copies the complete stopped role, never a standalone DB.
// A runtime-created role-state locking marker is mandatory. No existing output
// is overwritten, and failed copies remain incomplete rather than being erased.
func BackupMaintenance(ctx context.Context, stateDir, role, destination string) (_ *MaintenanceBackupReport, err error) {
	ctx, cancel := context.WithTimeout(ctx, maintenanceTimeout)
	defer cancel()
	g, err := maintenanceOwn(ctx, stateDir, role, true)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, g.close()) }()
	report, err := g.inspect(ctx)
	if err != nil {
		return nil, err
	}
	// tsnet identity is part of the restore unit; a journal-only directory is
	// not silently promoted into a full-role backup.
	if _, err := maintenanceDirectory(filepath.Join(g.root, "tsnet")); err != nil {
		return nil, err
	}
	identity, err := maintenanceOpenRegular(filepath.Join(g.root, "tsnet", "tailscaled.state"), false)
	if err != nil {
		return nil, err
	}
	identityInfo, statErr := identity.Stat()
	identityCloseErr := identity.Close()
	if statErr != nil || identityCloseErr != nil || identityInfo.Size() == 0 {
		return nil, ErrMaintenancePath
	}
	entries, total, err := maintenanceTree(ctx, g.root)
	if err != nil {
		return nil, err
	}
	destination, err = maintenanceDestination(g.root, destination)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if os.Mkdir(destination, 0700) != nil {
		return nil, ErrMaintenancePath
	}
	if protectPath(destination, true) != nil {
		return nil, ErrMaintenancePath
	}
	out, err := os.OpenRoot(destination)
	if err != nil {
		return nil, ErrMaintenancePath
	}
	defer out.Close()
	if out.Mkdir("state", 0700) != nil {
		return nil, ErrMaintenancePath
	}
	files := 0
	for i := range entries {
		entry := &entries[i]
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target := filepath.Join("state", entry.Path)
		if entry.Directory {
			if out.Mkdir(target, 0700) != nil {
				return nil, ErrMaintenancePath
			}
			continue
		}
		file, err := out.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, ErrMaintenancePath
		}
		entry.SHA256, err = maintenanceReadEntry(ctx, g.root, *entry, g.locks, file)
		syncErr, closeErr := file.Sync(), file.Close()
		if err != nil {
			return nil, err
		}
		if syncErr != nil || closeErr != nil {
			return nil, ErrMaintenancePath
		}
		files++
	}
	// Re-read source metadata and bytes while both locks remain held. This
	// also catches an unsupported writer that bypasses the role-lock protocol.
	current, currentTotal, err := maintenanceTree(ctx, g.root)
	if err != nil {
		return nil, err
	}
	if currentTotal != total || len(current) != len(entries) {
		return nil, ErrMaintenanceChanged
	}
	for i, entry := range current {
		expected := entries[i]
		if entry.Path != expected.Path || entry.Directory != expected.Directory || entry.Size != expected.Size ||
			!os.SameFile(entry.info, expected.info) {
			return nil, ErrMaintenanceChanged
		}
		if !entry.Directory {
			hash, err := maintenanceReadEntry(ctx, g.root, entry, g.locks, nil)
			if err != nil {
				return nil, err
			}
			if hash != expected.SHA256 {
				return nil, ErrMaintenanceChanged
			}
		}
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Directory {
			if maintenanceSyncDirectory(filepath.Join(destination, "state", entries[i].Path)) != nil {
				return nil, ErrMaintenancePath
			}
		}
	}
	if maintenanceSyncDirectory(filepath.Join(destination, "state")) != nil {
		return nil, ErrMaintenancePath
	}
	manifest := maintenanceManifest{Version: 1, Role: role, Report: *report, Entries: entries}
	data, err := json.Marshal(manifest)
	if err != nil || len(data) > maintenanceMaxManifest {
		return nil, ErrMaintenanceBounds
	}
	if err := maintenanceWrite(out, "manifest.json", data); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if err := maintenanceWrite(out, "COMPLETE", []byte(hex.EncodeToString(digest[:])+"\n")); err != nil {
		return nil, err
	}
	if maintenanceSyncDirectory(destination) != nil || maintenanceSyncDirectory(filepath.Dir(destination)) != nil {
		return nil, ErrMaintenancePath
	}
	verified, err := VerifyMaintenanceBackup(ctx, destination, role)
	if err != nil {
		return nil, err
	}
	if verified.Files != files || verified.Bytes != total {
		return nil, ErrMaintenanceBackup
	}
	return verified, nil
}

func maintenanceWrite(root *os.Root, path string, data []byte) error {
	file, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ErrMaintenancePath
	}
	n, err := file.Write(data)
	syncErr, closeErr := file.Sync(), file.Close()
	if err != nil || n != len(data) || syncErr != nil || closeErr != nil {
		return ErrMaintenancePath
	}
	return nil
}

func maintenanceReadBounded(path string, limit int64) ([]byte, error) {
	file, err := maintenanceOpenRegular(path, false)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil || int64(len(data)) > limit {
		return nil, ErrMaintenanceBackup
	}
	return data, nil
}

// VerifyMaintenanceBackup verifies the complete artifact without opening its
// SQLite database, writing sidecars, or changing any receipt. It cannot certify
// that restoring an old backup is safe after subsequent external effects.
func VerifyMaintenanceBackup(ctx context.Context, backupDir, role string) (_ *MaintenanceBackupReport, err error) {
	ctx, cancel := context.WithTimeout(ctx, maintenanceTimeout)
	defer cancel()
	if _, _, err := maintenanceRole(role); err != nil {
		return nil, err
	}
	root, err := maintenanceDirectory(backupDir)
	if err != nil {
		return nil, err
	}
	data, err := maintenanceReadBounded(filepath.Join(root, "manifest.json"), maintenanceMaxManifest)
	if err != nil {
		return nil, err
	}
	complete, err := maintenanceReadBounded(filepath.Join(root, "COMPLETE"), 65)
	if err != nil {
		return nil, ErrMaintenanceBackup
	}
	digest := sha256.Sum256(data)
	if string(complete) != hex.EncodeToString(digest[:])+"\n" {
		return nil, ErrMaintenanceBackup
	}
	var manifest maintenanceManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil || decoder.Decode(new(any)) != io.EOF ||
		manifest.Version != 1 || manifest.Role != role || manifest.Report.Role != role || manifest.Report.Integrity != "ok" ||
		len(manifest.Entries) > maintenanceMaxFiles {
		return nil, ErrMaintenanceBackup
	}
	g, err := maintenanceOwn(ctx, filepath.Join(root, "state"), role, true)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, g.close()) }()
	entries, total, err := maintenanceTree(ctx, g.root)
	if err != nil {
		return nil, err
	}
	if len(entries) != len(manifest.Entries) {
		return nil, ErrMaintenanceBackup
	}
	files := 0
	for i, entry := range entries {
		expected := manifest.Entries[i]
		if !filepath.IsLocal(expected.Path) || strings.Contains(expected.Path, ":") ||
			entry.Path != expected.Path || entry.Directory != expected.Directory || entry.Size != expected.Size {
			return nil, ErrMaintenanceBackup
		}
		if entry.Directory {
			if expected.SHA256 != "" {
				return nil, ErrMaintenanceBackup
			}
			continue
		}
		hash, err := maintenanceReadEntry(ctx, g.root, entry, g.locks, nil)
		if err != nil {
			return nil, err
		}
		if hash != expected.SHA256 {
			return nil, ErrMaintenanceBackup
		}
		files++
	}
	return &MaintenanceBackupReport{Destination: root, Verified: true, Files: files, Bytes: total,
		Source: manifest.Report, Recovery: "operator_review_required_no_automatic_restore"}, nil
}
