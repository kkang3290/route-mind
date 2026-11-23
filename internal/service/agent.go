package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

type toolBinding struct {
	Executor    string          `json:"executor"`
	Name        string          `json:"name"`
	Version     string          `json:"version,omitempty"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}
type createRunRequest struct {
	Input            []llm.Message  `json:"input"`
	Tools            []toolBinding  `json:"tools,omitempty"`
	ToolChoice       llm.ToolChoice `json:"tool_choice,omitempty"`
	MaxTokens        int            `json:"max_tokens,omitempty"`
	Temperature      *float64       `json:"temperature,omitempty"`
	TopP             *float64       `json:"top_p,omitempty"`
	Stop             []string       `json:"stop,omitempty"`
	AllowedModels    []string       `json:"allowed_models,omitempty"`
	AllowedProviders []string       `json:"allowed_providers,omitempty"`
	Limits           map[string]int `json:"limits,omitempty"`
	Stream           bool           `json:"stream,omitempty"`
}
type resumeRequest struct {
	Revision int64              `json:"revision"`
	Results  []agent.ToolResult `json:"results"`
	Stream   bool               `json:"stream,omitempty"`
}

func decodeStrict(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return llm.Invalid("invalid request JSON or unsupported field")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return llm.Invalid("exactly one JSON object is required")
	}
	return nil
}
func limitOverrides(base agent.Limits, changes map[string]int) (agent.Limits, error) {
	if len(changes) == 0 {
		return base, nil
	}
	b, _ := json.Marshal(base)
	var values map[string]int
	_ = json.Unmarshal(b, &values)
	for name, n := range changes {
		max, ok := values[name]
		if !ok || n <= 0 || n > max {
			return base, llm.Invalid("run limits must be known positive values within server policy")
		}
		values[name] = n
	}
	b, _ = json.Marshal(values)
	_ = json.Unmarshal(b, &base)
	return base, nil
}
func (h *Handler) storeReady(w http.ResponseWriter) bool {
	if h.app.Store == nil {
		writeError(w, "openai", llm.Failure("storage_unavailable", "durable run storage is unavailable", 503))
		return false
	}
	return true
}
func idempotency(r *http.Request, b []byte) (string, string, error) {
	key := r.Header.Get("Idempotency-Key")
	if len(key) == 0 || len(key) > 128 || strings.TrimSpace(key) != key {
		return "", "", llm.Invalid("Idempotency-Key with 1–128 characters is required")
	}
	return key, fmt.Sprintf("%x", sha256.Sum256(b)), nil
}
func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") || !h.storeReady(w) {
		return
	}
	b, e := readBody(w, r, h.options.Server.MaxBodyBytes)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	key, digest, e := idempotency(r, b)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	var in createRunRequest
	if e = decodeStrict(b, &in); e != nil {
		writeError(w, "openai", e)
		return
	}
	p := r.Context().Value(principalKey{}).(config.Principal)
	limits, e := limitOverrides(h.app.Policy.Limits, in.Limits)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	specs := []agent.ToolSpec{}
	for _, binding := range in.Tools {
		switch binding.Executor {
		case "server":
			if !slices.Contains(p.ServerTools, binding.Name) {
				writeError(w, "openai", llm.Failure("permission_denied", "server tool is not permitted", 403))
				return
			}
			spec, ok := h.app.Tools.Spec(binding.Name)
			if !ok || len(binding.Parameters) > 0 || binding.Description != "" || (binding.Version != "" && binding.Version != spec.Version) {
				writeError(w, "openai", llm.Invalid("server tool must reference the current registered contract without overrides"))
				return
			}
			specs = append(specs, spec)
		case "client":
			if binding.Version != "" {
				writeError(w, "openai", llm.Invalid("client tool versions are not supported"))
				return
			}
			specs = append(specs, agent.ToolSpec{Executor: "client", Definition: llm.Tool{Name: binding.Name, Description: binding.Description, Parameters: binding.Parameters}})
		default:
			writeError(w, "openai", llm.Invalid("tool executor must be client or server"))
			return
		}
	}
	input := llm.Request{Model: "auto", Messages: in.Input, ToolChoice: in.ToolChoice, MaxTokens: in.MaxTokens, Temperature: in.Temperature, TopP: in.TopP, Stop: in.Stop}
	run, e := h.app.NewRun(p, input, specs, in.AllowedModels, in.AllowedProviders, limits, false)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	run, duplicate, e := h.app.Store.Create(r.Context(), run, key, digest)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	h.advanceRun(w, r, run, duplicate, in.Stream, 201)
}
func (h *Handler) runResource(w http.ResponseWriter, r *http.Request) {
	if !h.storeReady(w) {
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, "/v1/agent/runs/")
	id, action, hasAction := strings.Cut(tail, "/")
	if id == "" || (hasAction && action != "tool-results") {
		writeError(w, "openai", llm.Failure("not_found", "run route not found", 404))
		return
	}
	p := r.Context().Value(principalKey{}).(config.Principal)
	if !hasAction {
		if r.Method == http.MethodDelete {
			h.deleteRun(w, r, p.ID, id)
			return
		}
		if !method(w, r, "GET") {
			return
		}
		run, e := h.app.Store.Get(r.Context(), p.ID, id)
		if e != nil {
			writeError(w, "openai", e)
			return
		}
		writeJSON(w, 200, run.Snapshot())
		return
	}
	if !method(w, r, "POST") {
		return
	}
	b, e := readBody(w, r, h.options.Server.MaxBodyBytes)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	key, digest, e := idempotency(r, b)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	var in resumeRequest
	if e = decodeStrict(b, &in); e != nil {
		writeError(w, "openai", e)
		return
	}
	if in.Revision <= 0 {
		writeError(w, "openai", llm.Invalid("positive run revision is required"))
		return
	}
	existing, e := h.app.Store.Get(r.Context(), p.ID, id)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	if e = h.app.Revalidate(p, existing); e != nil {
		writeError(w, "openai", e)
		return
	}
	run, duplicate, e := h.app.Store.Resume(r.Context(), p.ID, id, in.Revision, key, digest, in.Results)
	if e != nil {
		writeError(w, "openai", e)
		return
	}
	h.advanceRun(w, r, run, duplicate, in.Stream, 200)
}
func (h *Handler) advanceRun(w http.ResponseWriter, r *http.Request, run *agent.Run, duplicate, stream bool, status int) {
	w.Header().Set("Location", "/v1/agent/runs/"+run.ID)
	if duplicate {
		if stream {
			writeError(w, "openai", llm.Failure("conflict", "stream replay is not supported; query the run Location", 409))
			return
		}
		writeJSON(w, 200, run.Snapshot())
		return
	}
	lazy := &streamWriter{w: w, seconds: h.options.Server.WriteSeconds, at: time.Now()}
	var sink llm.Sink
	first := true
	if stream {
		sink = func(v llm.Event) error {
			switch v.Type {
			case "usage", "block_end":
				return nil
			case "text_delta":
				if first && v.Text != "" {
					first = false
					h.firstTokens.Add(1)
					h.firstTokenNanos.Add(time.Since(lazy.at).Nanoseconds())
				}
				data, _ := json.Marshal(map[string]any{"type": "run.output_delta", "id": run.ID, "delta": v.Text})
				_, e := fmt.Fprintf(lazy, "event: run.output_delta\ndata: %s\n\n", data)
				return e
			default:
				return llm.ProtocolError("unsupported final output event")
			}
		}
	}
	err := h.app.Engine.Advance(r.Context(), run, sink, func(ctx context.Context, r *agent.Run) error { return h.app.Store.Save(ctx, r) })
	if err != nil {
		if lazy.started {
			h.streamFailed(w, r, err)
			data, _ := json.Marshal(protocol.ErrorPayload("openai", err))
			_, _ = fmt.Fprintf(lazy, "event: error\ndata: %s\n\n", data)
		} else {
			writeError(w, "openai", err)
		}
		return
	}
	if !stream {
		writeJSON(w, status, run.Snapshot())
		return
	}
	event := "run.completed"
	switch run.Status {
	case "awaiting_tool_results":
		event = "run.requires_action"
	case "incomplete":
		event = "run.incomplete"
	}
	data, _ := json.Marshal(map[string]any{"type": event, "run": run.Snapshot()})
	_, _ = fmt.Fprintf(lazy, "event: %s\ndata: %s\n\n", event, data)
}
