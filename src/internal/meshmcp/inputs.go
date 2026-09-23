package meshmcp

// These are local application-boundary types, not protobuf or provider API types.

type PageInput struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type NodeInput struct {
	NodeInstanceID string `json:"node_instance_id"`
}

type ProjectInput struct {
	NodeInput
	ProjectID string `json:"project_id"`
}

type ListProjectsInput struct {
	NodeInput
	PageInput
}

type ListSessionsInput struct {
	NodeInput
	PageInput
}

type RegisterProjectInput struct {
	ProjectInput
	CheckoutPath string `json:"checkout_path"`
	WorktreeRoot string `json:"worktree_root"`
}

type SessionInput struct {
	NodeInput
	// Both selectors are required together; their absence retains the configured default.
	HerdrSessionID          string `json:"herdr_session_id,omitempty"`
	HerdrSessionIncarnation string `json:"herdr_session_incarnation,omitempty"`
}

type EnsureHerdrSessionInput struct {
	NodeInput
	HerdrSessionID string `json:"herdr_session_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type WorkspaceInput struct {
	SessionInput
	ProjectID string `json:"project_id,omitempty"`
}

type ListWorkspacesInput struct {
	WorkspaceInput
	PageInput
}

type ListAgentsInput struct {
	WorkspaceInput
	PageInput
	WorkspaceID string `json:"workspace_id,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Readiness   string `json:"readiness,omitempty"`
}

type AgentTarget struct {
	AgentSessionID string `json:"agent_session_id,omitempty"`
	PaneID         string `json:"pane_id"`
	TerminalID     string `json:"terminal_id"`
}

type AgentInput struct {
	WorkspaceInput
	Target AgentTarget `json:"target"`
}

type ReadAgentInput struct {
	AgentInput
	Lines    int `json:"lines"`
	MaxBytes int `json:"max_bytes"`
}

type WaitAgentInput struct {
	AgentInput
	Until     []string `json:"until"`
	TimeoutMS int      `json:"timeout_ms"`
}

type EnsureWorkspaceInput struct {
	WorkspaceInput
	BindingRevision string `json:"binding_revision"`
	IdempotencyKey  string `json:"idempotency_key"`
}

type CreateWorktreeInput struct {
	EnsureWorkspaceInput
	Name       string `json:"name"`
	Branch     string `json:"branch"`
	BaseCommit string `json:"base_commit"`
}

type StartAgentInput struct {
	EnsureWorkspaceInput
	WorkspaceID      string `json:"workspace_id"`
	Name             string `json:"name"`
	Provider         string `json:"provider"`
	StartupTimeoutMS int    `json:"startup_timeout_ms,omitempty"`
	InitialPrompt    string `json:"initial_prompt,omitempty"`
}

type StopAgentInput struct {
	SessionInput
	Target         AgentTarget `json:"target"`
	WorkspaceID    string      `json:"workspace_id"`
	TabID          string      `json:"tab_id"`
	Provider       string      `json:"provider"`
	IdempotencyKey string      `json:"idempotency_key"`
}

type MutateAgentInput struct {
	AgentInput
	IdempotencyKey string `json:"idempotency_key"`
}

type PromptAgentInput struct {
	MutateAgentInput
	Text string `json:"text"`
}

type SendAgentInput struct {
	MutateAgentInput
	Text string   `json:"text,omitempty"`
	Keys []string `json:"keys,omitempty"`
}

type GetCommandInput struct {
	NodeInput
	CommandID string `json:"command_id"`
}
