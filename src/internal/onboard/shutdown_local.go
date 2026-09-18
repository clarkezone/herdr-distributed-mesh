package onboard

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func stopManaged(ctx context.Context, dir string, output io.Writer) error {
	op, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := meshlocal.Shutdown(op, dir)
	if !errors.Is(err, meshlocal.ErrLegacyShutdown) {
		if err != nil {
			return err
		}
		return meshlocal.RecordStopped(op, dir)
	}
	discovery, endDiscovery := context.WithTimeout(ctx, 5*time.Second)
	identity, identityErr := meshlocal.ResolveManagedIdentity(discovery, dir)
	endDiscovery()
	if identityErr == nil {
		if err := meshlocal.RetainIdentity(dir, identity); err != nil {
			return fmt.Errorf("preserve legacy cleanup identity before shutdown: %w", err)
		}
	} else {
		fmt.Fprintln(output, "Warning: legacy device identity could not be captured; later remote cleanup may require the original daemon to be resumed. No identity was guessed.")
	}
	fmt.Fprintln(output, "Legacy daemon: stopping only the verified managed process. Journals are preserved; inspect any in-flight command outcomes before retrying work.")
	if err := StopLegacyDaemon(op, dir); err != nil {
		return err
	}
	running, err := meshlocal.IsRunning(dir)
	if err != nil {
		return err
	}
	if running {
		return errors.New("managed ownership is still held after shutdown; no state was deleted")
	}
	return meshlocal.RecordStopped(op, dir)
}

func confirmDestroy(ctx context.Context, input io.Reader, expected string) (bool, error) {
	if input == nil {
		return false, errors.New("destruction requires interactive confirmation or explicit --yes")
	}
	type response struct {
		value string
		err   error
	}
	ready := make(chan response, 1)
	go func() {
		line, err := bufio.NewReader(io.LimitReader(input, 128)).ReadString('\n')
		ready <- response{line, err}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case result := <-ready:
		if result.err != nil && !errors.Is(result.err, io.EOF) {
			return false, result.err
		}
		return strings.TrimSpace(result.value) == expected, nil
	}
}

var tokenEnvironmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

func shutdownToken(ctx context.Context, name string, input io.Reader, output io.Writer) ([]byte, error) {
	var token []byte
	if name == "" {
		value, err := ReadToken(ctx, input, output)
		if err != nil {
			return nil, err
		}
		token = value
	} else {
		if !tokenEnvironmentName.MatchString(name) {
			return nil, errors.New("--api-token-env must name an environment variable, never contain a credential")
		}
		value, exists := os.LookupEnv(name)
		if !exists || value == "" {
			return nil, errors.New("the selected API-token environment variable is unset or empty")
		}
		token = []byte(value)
	}
	if !strings.HasPrefix(string(token), "tskey-api-") || len(token) > 4096 ||
		strings.ContainsAny(string(token), " \t\r\n\x00") {
		clear(token)
		return nil, errors.New("cleanup requires a Tailscale API access token (tskey-api-), not a device enrollment key")
	}
	return token, nil
}
