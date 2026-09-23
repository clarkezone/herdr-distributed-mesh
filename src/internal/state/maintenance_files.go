package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

func maintenanceLocalPath(path string) bool {
	return filepath.IsAbs(path) && maintenancePathSyntaxSafe(path)
}

func maintenancePathSyntaxSafe(path string) bool {
	if len(path) > 4096 || !utf8.ValidString(path) ||
		strings.ContainsFunc(path, unicode.IsControl) || strings.HasPrefix(path, `\\`) ||
		strings.HasPrefix(path, "//") || strings.Contains(strings.TrimPrefix(path, filepath.VolumeName(path)), ":") {
		return false
	}
	for _, part := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return false
		}
	}
	return true
}

func maintenanceDirectory(path string) (string, error) {
	if !maintenanceLocalPath(path) {
		return "", ErrMaintenancePath
	}
	path = filepath.Clean(path)
	if filepath.Dir(path) == path {
		return "", ErrMaintenancePath
	}
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || !maintenanceOrdinary(current, info) {
			return "", ErrMaintenancePath
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	return path, nil
}

func maintenanceOpenRegular(path string, write bool) (*os.File, error) {
	if _, err := maintenanceDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return runtimeOpenRegular(path, write)
}

func runtimeOpenRegular(path string, write bool) (*os.File, error) {
	if _, err := runtimeDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || !maintenanceOrdinary(path, info) {
		return nil, ErrMaintenancePath
	}
	flags := os.O_RDONLY
	if write {
		flags = os.O_RDWR
	}
	file, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, ErrMaintenancePath
	}
	current, err := file.Stat()
	if err != nil || !os.SameFile(info, current) || !maintenanceSingleLink(file, current) {
		_ = file.Close()
		return nil, ErrMaintenancePath
	}
	return file, nil
}

type maintenanceEntry struct {
	Path      string `json:"path"`
	Directory bool   `json:"directory"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256,omitempty"`
	info      os.FileInfo
}

func maintenanceTree(ctx context.Context, root string) ([]maintenanceEntry, int64, error) {
	var entries []maintenanceEntry
	var total int64
	var directories []os.FileInfo
	var walk func(string, int) error
	walk = func(relative string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth > maintenanceMaxDepth {
			return ErrMaintenanceBounds
		}
		path := filepath.Join(root, relative)
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || !maintenanceOrdinary(path, info) {
			return ErrMaintenancePath
		}
		for _, previous := range directories {
			if os.SameFile(previous, info) {
				return ErrMaintenancePath
			}
		}
		directories = append(directories, info)
		dir, err := os.Open(path)
		if err != nil {
			return ErrMaintenancePath
		}
		defer dir.Close()
		opened, err := dir.Stat()
		if err != nil || !os.SameFile(info, opened) {
			return ErrMaintenanceChanged
		}
		for {
			children, readErr := dir.ReadDir(64)
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return ErrMaintenancePath
			}
			for _, child := range children {
				if len(entries) >= maintenanceMaxFiles {
					return ErrMaintenanceBounds
				}
				rel := filepath.Join(relative, child.Name())
				full := filepath.Join(root, rel)
				if len(full) > 4096 {
					return ErrMaintenanceBounds
				}
				if !maintenanceLocalPath(full) {
					return ErrMaintenancePath
				}
				stat, err := os.Lstat(full)
				if err != nil || !maintenanceOrdinary(full, stat) ||
					(!stat.IsDir() && !stat.Mode().IsRegular()) {
					return ErrMaintenancePath
				}
				entry := maintenanceEntry{Path: rel, Directory: stat.IsDir(), info: stat}
				if stat.IsDir() {
					entries = append(entries, entry)
					if err := walk(rel, depth+1); err != nil {
						return err
					}
				} else {
					if stat.Size() < 0 || stat.Size() > maintenanceMaxBytes-total {
						return ErrMaintenanceBounds
					}
					total += stat.Size()
					entry.Size = stat.Size()
					entries = append(entries, entry)
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
		return nil
	}
	if err := walk("", 0); err != nil {
		return nil, 0, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, total, nil
}

func maintenanceReadEntry(ctx context.Context, root string, entry maintenanceEntry, held map[string]*os.File, output io.Writer) (string, error) {
	path := filepath.Join(root, entry.Path)
	file := held[path]
	if file == nil {
		var err error
		file, err = maintenanceOpenRegular(path, false)
		if err != nil {
			return "", err
		}
		defer file.Close()
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !maintenanceSingleLink(file, info) ||
		info.Size() != entry.Size || !os.SameFile(entry.info, info) || !entry.info.ModTime().Equal(info.ModTime()) {
		return "", ErrMaintenanceChanged
	}
	hash := sha256.New()
	writer := io.Writer(hash)
	if output != nil {
		writer = io.MultiWriter(output, hash)
	}
	reader := io.NewSectionReader(file, 0, entry.Size)
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := reader.Read(buffer)
		if n > 0 {
			if written, err := writer.Write(buffer[:n]); err != nil || written != n {
				return "", ErrMaintenancePath
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", ErrMaintenancePath
		}
	}
	after, err := os.Lstat(path)
	if err != nil || !maintenanceOrdinary(path, after) || !os.SameFile(info, after) ||
		info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
		return "", ErrMaintenanceChanged
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func maintenanceDestination(source, destination string) (string, error) {
	if !maintenanceLocalPath(destination) {
		return "", ErrMaintenancePath
	}
	destination = filepath.Clean(destination)
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		return "", ErrMaintenancePath
	}
	parent, err := maintenanceDirectory(filepath.Dir(destination))
	if err != nil {
		return "", err
	}
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return "", ErrMaintenancePath
	}
	for current := parent; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err != nil || os.SameFile(sourceInfo, info) {
			return "", ErrMaintenancePath
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	return destination, nil
}
