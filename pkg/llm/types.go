// Package llm defines provider-independent text and tool invocation contracts.
package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type Block struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	CallID    string          `json:"call_id,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}
type Message struct {
	Role    string  `json:"role"`
	Content []Block `json:"content"`
}

func TextMessage(role, text string) Message {
	return Message{Role: role, Content: []Block{{Type: "text", Text: text}}}
}

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}
type ToolChoice struct {
	Mode string `json:"mode,omitempty"`
	Name string `json:"name,omitempty"`
}
type Request struct {
	Model       string     `json:"model"`
	Messages    []Message  `json:"messages"`
	Tools       []Tool     `json:"tools,omitempty"`
	ToolChoice  ToolChoice `json:"tool_choice,omitempty"`
	MaxTokens   int        `json:"max_tokens"`
	Temperature *float64   `json:"temperature,omitempty"`
	TopP        *float64   `json:"top_p,omitempty"`
	Stop        []string   `json:"stop,omitempty"`
	Purpose     string     `json:"-"`
}
type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	Known        bool  `json:"known"`
}

func (u *Usage) Add(v Usage) {
	u.InputTokens += v.InputTokens
	u.OutputTokens += v.OutputTokens
	u.Known = u.Known && v.Known
}

type Response struct {
	ID           string  `json:"id"`
	Model        string  `json:"model"`
	Content      []Block `json:"content"`
	FinishReason string  `json:"finish_reason"`
	Usage        Usage   `json:"usage"`
}

func (r Response) Text() string {
	var s string
	for _, b := range r.Content {
		if b.Type == "text" {
			s += b.Text
		}
	}
	return s
}

type Event struct {
	Type  string
	Index int
	ID    string
	Name  string
	Text  string
	Usage Usage
}
type Sink func(Event) error

// Provider and Invoker have the same synchronous, backpressured streaming contract.
// Stream returns only after a verified terminal event; the caller owns public completion markers.
type Provider interface {
	Generate(context.Context, Request) (Response, error)
	Stream(context.Context, Request, Sink) (Response, error)
}
type Invoker = Provider

type Error struct {
	Code       string
	Message    string
	Status     int
	Retryable  bool
	RetryAfter string
	Cause      error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Cause }
func Invalid(message string) error {
	return &Error{Code: "invalid_request", Message: message, Status: http.StatusBadRequest}
}
func Failure(code, message string, status int) error {
	return &Error{Code: code, Message: message, Status: status}
}
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: "deadline_exceeded", Message: "request deadline exceeded", Status: 504}
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: "cancelled", Message: "request cancelled", Status: 499}
	}
	return &Error{Code: "internal_error", Message: "internal error", Status: 500, Cause: err}
}
func (r Request) InputBytes() int {
	b, _ := json.Marshal(struct {
		Messages []Message
		Tools    []Tool
	}{r.Messages, r.Tools})
	return len(b)
}
func (r Request) Validate() error {
	if r.Model == "" || r.Model == "auto" {
		return Invalid("a concrete registered model is required")
	}
	if len(r.Messages) == 0 || len(r.Messages) > 64 {
		return Invalid("messages must contain between 1 and 64 entries")
	}
	if r.MaxTokens <= 0 {
		return Invalid("max_tokens must be positive")
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return Invalid("temperature outside supported range")
	}
	if r.TopP != nil && (*r.TopP < 0 || *r.TopP > 1) {
		return Invalid("top_p outside supported range")
	}
	if len(r.Stop) > 4 {
		return Invalid("at most four stop sequences are supported")
	}
	tools := map[string]bool{}
	if len(r.Tools) > 16 {
		return Invalid("too many tools")
	}
	for _, t := range r.Tools {
		if t.Name == "" || len(t.Name) > 64 || tools[t.Name] || !json.Valid(t.Parameters) {
			return Invalid("invalid or duplicate tool definition")
		}
		var obj map[string]any
		if jsonUnmarshalObject(t.Parameters, &obj) != nil {
			return Invalid("tool schema must be an object")
		}
		tools[t.Name] = true
	}
	switch r.ToolChoice.Mode {
	case "", "auto", "none":
	case "required":
		if len(tools) == 0 {
			return Invalid("required tool choice needs tools")
		}
	case "named":
		if !tools[r.ToolChoice.Name] {
			return Invalid("named tool is not registered")
		}
	default:
		return Invalid("unsupported tool choice")
	}
	pending := map[string]bool{}
	seen := map[string]bool{}
	for _, m := range r.Messages {
		switch m.Role {
		case "system", "user", "assistant", "tool":
		default:
			return Invalid("unsupported message role")
		}
		if len(m.Content) == 0 {
			return Invalid("message content cannot be empty")
		}
		// A tool round must be resolved before starting another user/assistant turn.
		if len(pending) > 0 && m.Role != "tool" {
			onlyResults := true
			for _, b := range m.Content {
				if b.Type != "tool_result" {
					onlyResults = false
				}
			}
			if !onlyResults {
				return Invalid("unresolved tool calls")
			}
		}
		for _, b := range m.Content {
			switch b.Type {
			case "text":
				if m.Role == "tool" {
					return Invalid("tool messages require tool_result blocks")
				}
			case "tool_call":
				if m.Role != "assistant" || b.ID == "" || b.Name == "" || seen[b.ID] || len(b.Arguments) > 32768 {
					return Invalid("invalid tool call")
				}
				var a map[string]any
				if jsonUnmarshalObject(b.Arguments, &a) != nil {
					return Invalid("tool arguments must be a JSON object")
				}
				seen[b.ID] = true
				pending[b.ID] = true
			case "tool_result":
				if (m.Role != "tool" && m.Role != "user") || !pending[b.CallID] || len(b.Text) > 32768 {
					return Invalid("unknown or invalid tool result")
				}
				delete(pending, b.CallID)
			default:
				return Invalid("unsupported content block")
			}
		}
	}
	if len(pending) > 0 {
		return Invalid("unresolved tool calls")
	}
	if r.InputBytes() > 1<<20 {
		return Invalid("request exceeds input size limit")
	}
	return nil
}

type Model struct {
	ID              string `json:"id" yaml:"id"`
	Provider        string `json:"provider" yaml:"provider"`
	Upstream        string `json:"upstream" yaml:"upstream"`
	MaxInputBytes   int    `json:"max_input_bytes" yaml:"max_input_bytes"`
	MaxOutputTokens int    `json:"max_output_tokens" yaml:"max_output_tokens"`
	Stream          bool   `json:"stream" yaml:"stream"`
	Tools           bool   `json:"tools" yaml:"tools"`
	ForcedTools     bool   `json:"forced_tools" yaml:"forced_tools"`
	CostRank        int    `json:"cost_rank" yaml:"cost_rank"`
	QualityTier     int    `json:"quality_tier" yaml:"quality_tier"`
}
type Catalog struct {
	models []Model
	byID   map[string]Model
}

func NewCatalog(models []Model) (*Catalog, error) {
	c := &Catalog{byID: map[string]Model{}}
	for _, m := range models {
		if m.ID == "" || m.ID == "auto" || m.Provider == "" || m.Upstream == "" || m.MaxInputBytes <= 0 || m.MaxOutputTokens <= 0 {
			return nil, Invalid("invalid model configuration")
		}
		if _, ok := c.byID[m.ID]; ok {
			return nil, Invalid("duplicate model alias")
		}
		c.byID[m.ID] = m
		c.models = append(c.models, m)
	}
	if len(models) == 0 {
		return nil, Invalid("at least one model is required")
	}
	return c, nil
}
func (c *Catalog) Models() []Model             { return append([]Model(nil), c.models...) }
func (c *Catalog) Get(id string) (Model, bool) { m, ok := c.byID[id]; return m, ok }
func (c *Catalog) Resolve(r Request, stream bool) (Model, error) {
	if err := r.Validate(); err != nil {
		return Model{}, err
	}
	m, ok := c.byID[r.Model]
	if !ok {
		return Model{}, Failure("model_not_found", "model is not registered", 404)
	}
	if r.InputBytes() > m.MaxInputBytes || r.MaxTokens > m.MaxOutputTokens {
		return Model{}, Invalid("model context or output limit exceeded")
	}
	if stream && !m.Stream {
		return Model{}, Invalid("model does not support streaming")
	}
	if len(r.Tools) > 0 && r.ToolChoice.Mode != "none" && !m.Tools {
		return Model{}, Invalid("model does not support tools")
	}
	if (r.ToolChoice.Mode == "required" || r.ToolChoice.Mode == "named") && !m.ForcedTools {
		return Model{}, Invalid("model does not support forced tool choice")
	}
	return m, nil
}
func ProtocolError(detail string) error {
	return &Error{Code: "upstream_protocol_error", Message: "upstream returned an invalid response", Status: 502, Cause: fmt.Errorf("%s", detail)}
}
