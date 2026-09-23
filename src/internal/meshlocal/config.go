// Package meshlocal owns a single embedded mesh identity and private local
// Fleet gateway. Dial never starts or enrolls a network.
package meshlocal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
)

type Config struct {
	Version         int    `json:"schemaVersion"`
	Name            string `json:"name"`
	Tailnet         string `json:"tailnet"`
	Server          string `json:"server"`
	HerdrExecutable string `json:"herdrExecutable"`
	Coordinator     bool   `json:"coordinator"`
}

type Status struct {
	State   string
	DNSName string
	Server  string
	AuthURL string
	Error   string
}

func DefaultDir() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate executable for portable mesh state: %w", err)
	}
	return filepath.Join(filepath.Dir(executable), "herdr-mesh-state"), nil
}

func (c Config) validate() error {
	if c.Version != 1 || !transport.ValidNodeName(c.Name) {
		return errors.New("managed configuration requires version 1 and a lowercase computer name of at most 40 letters, digits or hyphens")
	}
	if (c.Coordinator && len(c.Tailnet) == 0) || len(c.Tailnet) > 253 || strings.ContainsAny(c.Tailnet, "\r\n\t /\\") {
		return errors.New("managed configuration requires a valid tailnet name (optional for workers)")
	}
	if len(c.HerdrExecutable) > 4096 || strings.ContainsAny(c.HerdrExecutable, "\x00\r\n") {
		return errors.New("invalid managed Herdr executable")
	}
	if c.Server != "" {
		host, port, err := net.SplitHostPort(c.Server)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host == "" || len(host) > 253 || strings.ContainsAny(host, " /\\\r\n\t") || numberErr != nil || number < 1 || number > 65535 {
			return errors.New("managed server must be a hostname or IP with an explicit valid port")
		}
	} else if !c.Coordinator {
		return errors.New("managed worker requires a coordinator address with port")
	}
	return nil
}

func privateDir(dir string, create bool) (string, error) {
	if strings.TrimSpace(dir) == "" || len(dir) > 4096 || !utf8.ValidString(dir) ||
		strings.ContainsFunc(dir, unicode.IsControl) || strings.HasPrefix(dir, `\\`) ||
		strings.HasPrefix(dir, "//") || strings.Contains(strings.TrimPrefix(dir, filepath.VolumeName(dir)), ":") {
		return "", errors.New("managed state requires a safe local directory")
	}
	for _, part := range strings.FieldsFunc(dir, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return "", errors.New("managed state directory cannot contain parent traversal")
		}
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if filepath.Dir(root) == root {
		return "", errors.New("managed state cannot be a filesystem root")
	}
	if create {
		if err := os.MkdirAll(root, 0700); err != nil {
			return "", err
		}
	}
	if err := state.ProtectPrivatePath(root, true); err != nil {
		return "", err
	}
	return root, nil
}

func Load(dir string) (Config, error) {
	var config Config
	root, err := privateDir(dir, false)
	if err == nil {
		err = readJSON(filepath.Join(root, "config.json"), &config)
	}
	if err == nil {
		err = config.validate()
	}
	return config, err
}

// Save is create-only: init/join cannot silently replace an enrolled identity.
func Save(dir string, config Config) (result error) {
	if err := config.validate(); err != nil {
		return err
	}
	root, err := privateDir(dir, true)
	if err != nil {
		return err
	}
	path := filepath.Join(root, "config.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create managed configuration (existing configuration is never replaced): %w", err)
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if err := state.ProtectPrivatePath(path, false); err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(config); err != nil {
		return err
	}
	return file.Sync()
}

func readJSON(path string, value any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("managed state must be an ordinary private file")
	}
	file, err := openPrivateRead(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 16384 {
		return errors.New("managed state exceeds file size limit")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 16385))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("managed state has trailing or excessive data")
	}
	return nil
}

func ReadStatus(dir string) (Status, error) {
	var status Status
	root, err := privateDir(dir, false)
	if err == nil {
		err = readJSON(filepath.Join(root, "status.json"), &status)
	}
	if err == nil && (len(status.State) > 32 || len(status.Error) > 512 || len(status.AuthURL) > 2048 || len(status.DNSName) > 253 || len(status.Server) > 260) {
		err = errors.New("managed status exceeds diagnostic bounds")
	}
	return status, err
}

func writeStatus(dir string, status Status) (result error) {
	file, err := os.CreateTemp(dir, ".status-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}()
	if err := state.ProtectPrivatePath(path, false); err != nil {
		return errors.Join(err, file.Close())
	}
	err = json.NewEncoder(file).Encode(status)
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	return replaceStatus(path, filepath.Join(dir, "status.json"))
}
