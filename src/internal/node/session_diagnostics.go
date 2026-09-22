package node

import "github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"

type sessionDiagnostic struct {
	status, errorCode string
	protocol          int
}

type sessionDiagnostics struct {
	logf        func(string, ...any)
	initialized bool
	errorCode   string
	count       int
	sessions    map[string]sessionDiagnostic
}

func (d *sessionDiagnostics) update(sessions []herdrsession.Session, err error) {
	code := herdrsession.ErrorCode(err)
	if !d.initialized || code != d.errorCode || len(sessions) != d.count {
		d.logf("Herdr session discovery sessions=%d error=%q", len(sessions), code)
	}
	next := make(map[string]sessionDiagnostic, len(sessions))
	for _, session := range sessions {
		value := sessionDiagnostic{status: session.Status, errorCode: session.ErrorCode, protocol: session.Protocol}
		if previous, exists := d.sessions[session.Name]; !exists || previous != value {
			d.logf("Herdr session name=%q status=%q native_protocol=%d error=%q",
				session.Name, value.status, value.protocol, value.errorCode)
		}
		next[session.Name] = value
	}
	d.initialized, d.errorCode, d.count, d.sessions = true, code, len(sessions), next
}
