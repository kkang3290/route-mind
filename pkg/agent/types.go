// Package agent is a transport-independent, step-wise model orchestration framework.
// A Run has one execution owner; persistent stores arbitrate ownership across processes.
package agent

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type Limits struct {
	MaxSteps          int `json:"max_steps" yaml:"max_steps"`
	MaxCalls          int `json:"max_calls" yaml:"max_calls"`
	MaxReservedOutput int `json:"max_reserved_output" yaml:"max_reserved_output"`
	WorkTokens        int `json:"work_tokens" yaml:"work_tokens"`
	DecisionTokens    int `json:"decision_tokens" yaml:"decision_tokens"`
	FinalTokens       int `json:"final_tokens" yaml:"final_tokens"`
	DeadlineSeconds   int `json:"deadline_seconds" yaml:"deadline_seconds"`
	ToolSeconds       int `json:"tool_seconds" yaml:"tool_seconds"`
	MaxHistoryBytes   int `json:"max_history_bytes" yaml:"max_history_bytes"`
	RetentionSeconds  int `json:"retention_seconds" yaml:"retention_seconds"`
}

func DefaultLimits() Limits {
	return Limits{MaxSteps: 6, MaxCalls: 16, MaxReservedOutput: 12288, WorkTokens: 1024, DecisionTokens: 256, FinalTokens: 2048, DeadlineSeconds: 180, ToolSeconds: 3, MaxHistoryBytes: 262144, RetentionSeconds: 86400}
}
func NormalizeLimits(v Limits) Limits {
	d := DefaultLimits()
	if v.MaxSteps <= 0 {
		v.MaxSteps = d.MaxSteps
	}
	if v.MaxCalls <= 0 {
		v.MaxCalls = d.MaxCalls
	}
	if v.MaxReservedOutput <= 0 {
		v.MaxReservedOutput = d.MaxReservedOutput
	}
	if v.WorkTokens <= 0 {
		v.WorkTokens = d.WorkTokens
	}
	if v.DecisionTokens <= 0 {
		v.DecisionTokens = d.DecisionTokens
	}
	if v.FinalTokens <= 0 {
		v.FinalTokens = d.FinalTokens
	}
	if v.DeadlineSeconds <= 0 {
		v.DeadlineSeconds = d.DeadlineSeconds
	}
	if v.ToolSeconds <= 0 {
		v.ToolSeconds = d.ToolSeconds
	}
	if v.MaxHistoryBytes <= 0 {
		v.MaxHistoryBytes = d.MaxHistoryBytes
	}
	if v.RetentionSeconds <= 0 {
		v.RetentionSeconds = d.RetentionSeconds
	}
	return v
}

type Counters struct {
	Steps          int `json:"steps"`
	Calls          int `json:"calls"`
	ReservedOutput int `json:"reserved_output"`
}
type ToolSpec struct {
	Executor   string   `json:"executor"`
	Definition llm.Tool `json:"definition"`
	Version    string   `json:"version,omitempty"`
}
type ToolResult struct {
	CallID  string `json:"call_id"`
	Content string `json:"content"`
	IsError bool   `json:"is_error,omitempty"`
}
type ToolCall struct {
	ID         string          `json:"id"`
	ProviderID string          `json:"provider_id"`
	Name       string          `json:"name"`
	Arguments  json.RawMessage `json:"arguments"`
	Executor   string          `json:"executor"`
	Step       int             `json:"step"`
	Status     string          `json:"status"`
	Result     *ToolResult     `json:"result,omitempty"`
}
type Step struct {
	Attempt   int        `json:"attempt"`
	Number    int        `json:"number"`
	Kind      string     `json:"kind"`
	Model     string     `json:"model"`
	Status    string     `json:"status"`
	Usage     llm.Usage  `json:"usage"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}
type PublicError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type Run struct {
	ID                      string        `json:"id"`
	Owner                   string        `json:"owner"`
	Revision                int64         `json:"revision"`
	SchemaVersion           int           `json:"schema_version"`
	Status                  string        `json:"status"`
	Input                   llm.Request   `json:"input"`
	History                 []llm.Message `json:"history"`
	Tools                   []ToolSpec    `json:"tools"`
	AllowedModels           []string      `json:"allowed_models"`
	AllowedProviders        []string      `json:"allowed_providers"`
	Compatibility           bool          `json:"compatibility"`
	Limits                  Limits        `json:"limits"`
	Counters                Counters      `json:"counters"`
	TotalUsage              llm.Usage     `json:"total_usage"`
	Steps                   []Step        `json:"steps"`
	ToolCalls               []ToolCall    `json:"tool_calls"`
	Batch                   []string      `json:"batch"`
	ToolConstraintSatisfied bool          `json:"tool_constraint_satisfied"`
	Output                  *llm.Response `json:"output,omitempty"`
	Error                   *PublicError  `json:"error,omitempty"`
	CreatedAt               time.Time     `json:"created_at"`
	DeadlineAt              time.Time     `json:"deadline_at"`
	ExpiresAt               time.Time     `json:"expires_at"`
	OperationID             string        `json:"operation_id,omitempty"`
	advanceMu               sync.Mutex
}
type RunOptions struct {
	Owner            string
	Tools            []ToolSpec
	AllowedModels    []string
	AllowedProviders []string
	Compatibility    bool
	Limits           Limits
}

func NewRun(input llm.Request, o RunOptions) (*Run, error) {
	limits := NormalizeLimits(o.Limits)
	if input.MaxTokens == 0 {
		input.MaxTokens = limits.FinalTokens
	}
	if input.MaxTokens > limits.FinalTokens {
		return nil, llm.Invalid("requested final output exceeds policy")
	}
	input.Model = "__validation__"
	if len(o.Tools) > 0 {
		input.Tools = nil
		for _, t := range o.Tools {
			input.Tools = append(input.Tools, t.Definition)
		}
	}
	if e := input.Validate(); e != nil {
		return nil, e
	}
	input.Model = "auto"
	if len(o.Tools) == 0 {
		for _, t := range input.Tools {
			o.Tools = append(o.Tools, ToolSpec{Executor: "client", Definition: t})
		}
	}
	for _, t := range o.Tools {
		if t.Executor != "client" && t.Executor != "server" {
			return nil, llm.Invalid("tool executor must be client or server")
		}
		if _, e := compileSchema(t.Definition.Parameters); e != nil {
			return nil, e
		}
	}
	for _, m := range o.AllowedModels {
		if m == "auto" || m == "" {
			return nil, llm.Invalid("auto cannot be an execution candidate")
		}
	}
	if limits.RetentionSeconds < limits.DeadlineSeconds {
		return nil, llm.Invalid("retention must cover the run deadline")
	}
	now := time.Now().UTC()
	r := &Run{ID: NewID(), Owner: o.Owner, Revision: 1, SchemaVersion: 1, Status: "running", Input: input, Tools: o.Tools, AllowedModels: append([]string(nil), o.AllowedModels...), AllowedProviders: append([]string(nil), o.AllowedProviders...), Compatibility: o.Compatibility, Limits: limits, TotalUsage: llm.Usage{Known: true}, CreatedAt: now, DeadlineAt: now.Add(time.Duration(limits.DeadlineSeconds) * time.Second), ExpiresAt: now.Add(time.Duration(limits.RetentionSeconds) * time.Second)}
	if len(mustMarshal(r.Input.Messages)) > limits.MaxHistoryBytes {
		return nil, llm.Invalid("input exceeds run history limit")
	}
	return r, nil
}
func NewID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("secure random source unavailable")
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func Terminal(status string) bool {
	return status == "completed" || status == "incomplete" || status == "failed" || status == "cancelled"
}

type PendingTool struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}
type Snapshot struct {
	ID              string        `json:"id"`
	Status          string        `json:"status"`
	Revision        int64         `json:"revision"`
	Counters        Counters      `json:"counters"`
	TotalUsage      llm.Usage     `json:"total_usage"`
	Steps           []Step        `json:"steps"`
	RequiredActions []PendingTool `json:"required_actions,omitempty"`
	Output          *llm.Response `json:"output,omitempty"`
	Error           *PublicError  `json:"error,omitempty"`
	DeadlineAt      time.Time     `json:"deadline_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
}

// Snapshot is a field allowlist, never a serialization of the private run state.
// Do not read the same Run pointer concurrently with Advance; use a store snapshot.
func (r *Run) Snapshot() Snapshot {
	s := Snapshot{ID: r.ID, Status: r.Status, Revision: r.Revision, Counters: r.Counters, TotalUsage: r.TotalUsage, Steps: append([]Step(nil), r.Steps...), Output: r.Output, Error: r.Error, DeadlineAt: r.DeadlineAt, ExpiresAt: r.ExpiresAt}
	for _, c := range r.ToolCalls {
		if r.Status == "awaiting_tool_results" && c.Executor == "client" && c.Status == "waiting" {
			s.RequiredActions = append(s.RequiredActions, PendingTool{ID: c.ID, Name: c.Name, Arguments: append(json.RawMessage(nil), c.Arguments...)})
		}
	}
	return s
}

type Checkpoint func(context.Context, *Run) error

// RunStore is the durable service boundary. Create/Resume must claim execution
// and deduplicate atomically; Save is fenced by OperationID and Revision.
type RunStore interface {
	Create(context.Context, *Run, string, string) (*Run, bool, error)
	Get(context.Context, string, string) (*Run, error)
	Resume(context.Context, string, string, int64, string, string, []ToolResult) (*Run, bool, error)
	Save(context.Context, *Run) error
}

func mustMarshal(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return b
}
