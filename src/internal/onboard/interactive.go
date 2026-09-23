package onboard

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/setup"
	"golang.org/x/term"
	"google.golang.org/protobuf/types/known/emptypb"
)

func DefaultDependencies(input io.Reader, output io.Writer) Dependencies {
	return Dependencies{
		Dir: meshlocal.DefaultDir, Load: meshlocal.Load, Save: meshlocal.Save,
		Status: meshlocal.ReadStatus, Running: meshlocal.IsRunning,
		Verify: func(ctx context.Context, dir string) error {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			conn, err := meshlocal.Dial(ctx, dir)
			if err != nil {
				return err
			}
			defer conn.Close()
			info, err := pb.NewFleetClient(conn).GetServerInfo(ctx, &emptypb.Empty{})
			if err != nil {
				return err
			}
			_, err = protocol.Negotiate(info.Protocol)
			return err
		},
		Prerequisites: func(herdr string) error {
			if err := supportedPlatform(); err != nil {
				return err
			}
			for _, name := range []string{herdr, "git"} {
				if _, err := exec.LookPath(name); err != nil {
					return fmt.Errorf("required executable %q is not on PATH; install Herdr and Git yourself before onboarding: %w", name, err)
				}
			}
			fmt.Fprintln(output, "Use an already-installed, authenticated provider CLI with Herdr; onboarding does not install or authenticate provider tools.")
			return nil
		},
		Executable: os.Executable, Start: startDaemon, Browser: openBrowser,
		Token:   func(ctx context.Context) ([]byte, error) { return ReadToken(ctx, input, output) },
		Confirm: func(ctx context.Context) (bool, error) { return confirm(ctx, input) },
		Policy:  setup.RunWithToken, Wait: wait, Environment: os.Environ,
	}
}

func ReadToken(ctx context.Context, input io.Reader, output io.Writer) (token []byte, resultErr error) {
	file, ok := input.(*os.File)
	if !ok || !term.IsTerminal(int(file.Fd())) {
		return nil, errors.New("a real interactive terminal is required for the hidden Tailscale API token prompt; redirected input is refused (no insecure echo fallback)")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	original, err := term.GetState(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, term.Restore(int(file.Fd()), original)) }()
	if _, err := fmt.Fprint(output, "Tailscale API token (hidden): "); err != nil {
		return nil, err
	}
	type result struct {
		token []byte
		err   error
	}
	ready := make(chan result)
	go func() {
		value, err := term.ReadPassword(int(file.Fd()))
		select {
		case ready <- result{value, err}:
		case <-ctx.Done():
			clear(value)
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ready:
		fmt.Fprintln(output)
		return r.token, r.err
	}
}

func confirm(ctx context.Context, input io.Reader) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	type result struct {
		text string
		err  error
	}
	ready := make(chan result, 1)
	go func() {
		line, err := bufio.NewReader(io.LimitReader(input, 64)).ReadString('\n')
		ready <- result{line, err}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case r := <-ready:
		if r.err != nil && !errors.Is(r.err, io.EOF) {
			return false, r.err
		}
		value := strings.ToLower(strings.TrimSpace(r.text))
		return value == "y" || value == "yes", nil
	}
}

func validAuthURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.User == nil &&
		u.Port() == "" && (u.Hostname() == "login.tailscale.com" || u.Hostname() == "controlplane.tailscale.com") &&
		strings.HasPrefix(u.Path, "/a/") && !strings.ContainsAny(raw, "\x00\r\n")
}
