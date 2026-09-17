package protocol

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

const ProjectConfigCapability = "projects.config.v1"
const MaxProjectPageSize = 64

func ProjectRevision(generation uint64) string { return fmt.Sprintf("managed:%d", generation) }

func ValidProjectPath(path string, optional bool) bool {
	return (optional || strings.TrimSpace(path) != "") && len(path) <= 4096 &&
		utf8.ValidString(path) && !strings.ContainsFunc(path, unicode.IsControl)
}

func ValidateProjectRegistration(r *pb.RegisterProjectRequest) error {
	if r == nil || !commandToken.MatchString(r.NodeInstanceId) || !commandToken.MatchString(r.ProjectId) ||
		!ValidProjectPath(r.CheckoutPath, false) || !ValidProjectPath(r.WorktreeRoot, true) ||
		len(r.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("project registration requires bounded node/project IDs and paths")
	}
	return nil
}

func ValidateProjectConfig(c *pb.ProjectConfig) error {
	if c == nil || c.Generation == 0 || len(c.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid project generation")
	}
	return ValidateProjectRegistration(&pb.RegisterProjectRequest{NodeInstanceId: c.NodeInstanceId,
		ProjectId: c.ProjectId, CheckoutPath: c.CheckoutPath, WorktreeRoot: c.WorktreeRoot})
}

func ValidateProjectAck(a *pb.ProjectAck) error {
	if a == nil || !commandToken.MatchString(a.ProjectId) || a.Generation == 0 || len(a.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid project acknowledgement")
	}
	if a.Status == "applied" && a.ErrorCode == "" && ValidProjectPath(a.CheckoutPath, false) && ValidProjectPath(a.WorktreeRoot, false) {
		return nil
	}
	if a.Status == "invalid" && a.CheckoutPath == "" && a.WorktreeRoot == "" &&
		(a.ErrorCode == "invalid_path" || a.ErrorCode == "configuration_conflict") {
		return nil
	}
	return errors.New("invalid project acknowledgement outcome")
}

// New records retain the original normalized request separately from the
// resolved binding. Old records keep their original byte-level retry scope.
func ValidateSubmittedRequest(c *pb.Command) error {
	if c == nil || c.SubmittedRequest == nil {
		return errors.New("original request is required")
	}
	r, err := NormalizeCommandRequest(c.SubmittedRequest)
	if err != nil || !proto.Equal(r, c.SubmittedRequest) || r.NodeInstanceId != c.TargetId ||
		r.CommandType != c.CommandType || r.IdempotencyKey != c.IdempotencyKey {
		return errors.New("invalid original managed request")
	}
	switch c.CommandType {
	case AgentStartCommandType, AgentStopCommandType:
		return validateLifecycleSubmitted(c, r)
	case SessionEnsureCommandType:
		if !proto.Equal(r.SessionEnsure, c.SessionEnsure) {
			return errors.New("original session identity mismatch")
		}
		return nil
	case AgentControlCommandType:
		if !proto.Equal(r.AgentControl, c.AgentControl) {
			return errors.New("original agent identity mismatch")
		}
		return nil
	}
	project, revision := CommandProject(c)
	name, incarnation := CommandSession(c)
	if project == "" || (!strings.HasPrefix(revision, "managed:") && name == "" && incarnation == "") {
		return errors.New("managed request requires a resolved configuration")
	}
	if w := r.WorkspaceEnsure; w != nil {
		if c.WorkspaceEnsure == nil || w.ProjectId != project || (w.BindingRevision != "" && w.BindingRevision != revision) ||
			(w.BindingRevision == "" && !strings.HasPrefix(revision, "managed:")) ||
			!matchesSession(name, incarnation, w.SessionName, w.SessionIncarnation) {
			return errors.New("managed workspace identity mismatch")
		}
	} else if w := r.WorktreeCreate; w != nil {
		got := c.WorktreeCreate
		branch := w.Branch
		if branch == "" {
			branch = w.Name
		}
		if got == nil || w.ProjectId != project || (w.BindingRevision != "" && w.BindingRevision != revision) ||
			(w.BindingRevision == "" && !strings.HasPrefix(revision, "managed:")) ||
			!matchesSession(name, incarnation, w.SessionName, w.SessionIncarnation) ||
			w.Name != got.Name || branch != got.Branch || w.BaseCommit != got.BaseCommit {
			return errors.New("managed worktree identity mismatch")
		}
	} else {
		return errors.New("managed request has no project")
	}
	return nil
}
