package onboard

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/deinitnet"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

type cleanupTransport func(*http.Request) (*http.Response, error)

func (f cleanupTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestShutdownRemoteIntegrationUsesExactIdentityAndOwnedArtifacts(t *testing.T) {
	root, d, f := shutdownFixture(t)
	apply := filepath.Join(root, "policy-preview-test", "apply")
	if err := os.MkdirAll(apply, 0700); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string]string{
		filepath.Join(root, "policy-complete"):          "completed",
		filepath.Join(apply, "policy-before-test.json"): `{"grants":[]}`,
		filepath.Join(apply, "policy-proposed.json"):    `{"grants":[]}`,
	} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := state.ProtectPrivatePath(path, false); err != nil {
			t.Fatal(err)
		}
	}
	deleted, policyRead := false, false
	d.Remote = func(ctx context.Context, dir string, cfg meshlocal.Config, identity meshlocal.ManagedIdentity, removePolicy bool, token []byte) (RemoteCleanup, error) {
		client, err := deinitnet.New(token, deinitnet.Options{Transport: cleanupTransport(func(request *http.Request) (*http.Response, error) {
			code, body := 200, `{"id":"123","nodeId":"nPinned"}`
			switch request.Method + " " + request.URL.Path {
			case "GET /api/v2/device/nPinned":
				if deleted {
					code, body = 404, `{}`
				}
			case "DELETE /api/v2/device/nPinned":
				if len(f.calls) != 1 || f.calls[0] != "stop" {
					t.Fatal("remote deletion preceded local quiescence", f.calls)
				}
				deleted, body = true, `{}`
			case "GET /api/v2/tailnet/example.com/acl":
				policyRead, body = true, `{"grants":[]}`
			default:
				t.Fatalf("unexpected remote target or policy mutation: %s %s", request.Method, request.URL.Path)
			}
			return &http.Response{StatusCode: code, Header: http.Header{"Etag": []string{`"version-1"`}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		})})
		if err != nil {
			return nil, err
		}
		remote, err := prepareCleanupWithClient(ctx, dir, cfg, identity, removePolicy, client)
		if err != nil {
			client.Close()
		}
		return remote, err
	}
	if err := Shutdown(context.Background(), ShutdownOptions{Destroy: true, RemovePolicy: true, Yes: true}, io.Discard, d); err != nil {
		t.Fatal(err)
	}
	if !deleted || !policyRead {
		t.Fatal("remote cleanup was not exercised")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("verified remote cleanup did not permit local purge", err)
	}
}

func TestShutdownConfirmationAndCredentialInput(t *testing.T) {
	for _, text := range []string{"yes\n", "laptop\n", ""} {
		ok, err := confirmDestroy(context.Background(), strings.NewReader(text), "desktop")
		if err != nil || ok {
			t.Fatal("wrong node label confirmed destruction")
		}
	}
	if ok, err := confirmDestroy(context.Background(), strings.NewReader("desktop\n"), "desktop"); err != nil || !ok {
		t.Fatal(err)
	}
	t.Setenv("HERDR_CLEANUP_TEST_TOKEN", "tskey-auth-wrong-kind")
	if token, err := shutdownToken(context.Background(), "HERDR_CLEANUP_TEST_TOKEN", nil, io.Discard); err == nil || token != nil {
		t.Fatal("accepted an enrollment key")
	}
	t.Setenv("HERDR_CLEANUP_TEST_TOKEN", "tskey-api-private-test")
	token, err := shutdownToken(context.Background(), "HERDR_CLEANUP_TEST_TOKEN", nil, io.Discard)
	if err != nil || len(token) == 0 {
		t.Fatal(err)
	}
	clear(token)
	if _, err := shutdownToken(context.Background(), "", strings.NewReader("tskey-api-private-test"), io.Discard); err == nil {
		t.Fatal("redirected credential input bypassed hidden terminal prompt")
	}
}
