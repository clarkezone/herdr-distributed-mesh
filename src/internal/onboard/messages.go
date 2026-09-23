package onboard

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func JoinCommand(status meshlocal.Status) (string, error) {
	host := strings.TrimSuffix(status.DNSName, ".")
	if !ValidDNS(host) {
		return "", errors.New("the controller has not reported a valid Tailscale address; run herdr-mesh status to check setup progress")
	}
	endpoint := host
	if status.Server != "" {
		serverHost, port, err := net.SplitHostPort(status.Server)
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || number < 1 || number > 65535 ||
			!strings.EqualFold(strings.TrimSuffix(serverHost, "."), host) {
			return "", errors.New("the saved controller address does not match its Tailscale name; run herdr-mesh doctor before joining another computer")
		}
		if number != 50052 {
			endpoint = net.JoinHostPort(host, strconv.Itoa(number))
		}
	}
	return "herdr-mesh join --server " + endpoint, nil
}

func PrintJoinInstructions(output io.Writer, status meshlocal.Status) error {
	command, err := JoinCommand(status)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "\nOn another computer, run:\n  %s\n", command)
	return err
}

func StatusLabel(state string) string {
	switch state {
	case "ready":
		return "Ready"
	case "starting":
		return "Starting"
	case "login_required":
		return "Waiting for Tailscale sign-in"
	case "stopped":
		return "Stopped"
	case "failed", "error":
		return "Failed"
	case "":
		return "No status reported yet"
	default:
		return "Unknown state (" + state + ")"
	}
}
