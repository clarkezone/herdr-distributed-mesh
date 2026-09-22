package herdrsession

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestVerifiedNativeDiscoveryContracts(t *testing.T) {
	for _, protocol := range []int{18, 20, 22} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			f := &listRunner{fakeRunner: fakeRunner{running: true, protocol: protocol},
				list: []byte(`{"sessions":[{"name":"default","default":true,"running":true,"socket_path":"local-marker","session_dir":"local"},
					{"name":"main","default":false,"running":true,"socket_path":"named-marker","session_dir":"named"}]}`)}
			m, err := newManager(Config{}, f, func(string) (string, error) { return "incarnation", nil })
			if err != nil {
				t.Fatal(err)
			}
			sessions, err := m.List(context.Background())
			if err != nil || len(sessions) != 2 {
				t.Fatalf("sessions=%+v err=%v", sessions, err)
			}
			for _, session := range sessions {
				if session.Status != "ready" || session.Protocol != protocol || session.Incarnation != "incarnation" {
					t.Fatalf("native session not ready: %+v", session)
				}
			}
			for _, compatible := range []bool{false, true} {
				f.output = []byte(fmt.Sprintf(`{"status":"running","running":true,"version":"0.8.2","protocol":%d,
					"capabilities":{"live_handoff":false,"detached_server_daemon":false},
					"compatible":%t,"socket":"local-marker","session":null,"restart_needed":false,
					"endpoint_compatible":false,"server_binary_stale":true}`, protocol, compatible))
				session, err := m.Status(context.Background(), "default")
				if compatible {
					if err != nil || session.Status != "ready" || !session.Default || session.Protocol != protocol {
						t.Fatalf("default null identity rejected: %+v %v", session, err)
					}
				} else if !errors.Is(err, ErrUnsupported) || session.Status != "unsupported" {
					t.Fatalf("native CLI incompatibility ignored: %+v %v", session, err)
				}
			}
			f.output = nil
			f.running = false
			sessions, err = m.List(context.Background())
			if err != nil || len(sessions) != 2 || sessions[0].Status != "stopped" || sessions[1].Status != "stopped" {
				t.Fatalf("status must override stale list running flags: %+v %v", sessions, err)
			}
		})
	}
}
