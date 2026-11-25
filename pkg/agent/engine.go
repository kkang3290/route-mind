package agent

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type Engine struct {
	catalog *llm.Catalog
	caller  llm.Invoker
	decider Decider
	tools   *ToolRegistry
}

func NewEngine(c *llm.Catalog, caller llm.Invoker, decider Decider, tools *ToolRegistry) (*Engine, error) {
	if c == nil || caller == nil {
		return nil, llm.Invalid("catalog and model caller are required")
	}
	if decider == nil {
		decider = RuleDecider{}
	}
	return &Engine{catalog: c, caller: caller, decider: decider, tools: tools}, nil
}
func (e *Engine) ValidateRun(r *Run) error {
	if r.SchemaVersion != 1 {
		return llm.Failure("incompatible_run", "unsupported persisted run version", 409)
	}
	for _, s := range r.Tools {
		if s.Executor == "server" {
			current, ok := e.tools.Spec(s.Definition.Name)
			if !ok || current.Version != s.Version || current.Definition.Name != s.Definition.Name || current.Definition.Description != s.Definition.Description || !sameJSON(current.Definition.Parameters, s.Definition.Parameters) {
				return llm.Failure("incompatible_run", "server tool contract changed", 409)
			}
		}
	}
	if d, ok := e.decider.(interface{ ModelID() string }); ok {
		m, exists := e.catalog.Get(d.ModelID())
		if !exists || m.ID == "auto" || (len(r.AllowedProviders) > 0 && !slices.Contains(r.AllowedProviders, m.Provider)) {
			return llm.Failure("no_eligible_model", "decision model violates the current routing policy", 400)
		}
	}
	return nil
}
func (e *Engine) Advance(parent context.Context, r *Run, sink llm.Sink, checkpoint Checkpoint) (resultErr error) {
	if !r.advanceMu.TryLock() {
		return llm.Failure("conflict", "run already executing", 409)
	}
	defer r.advanceMu.Unlock()
	if r.Status != "running" {
		return llm.Failure("conflict", "run is not ready to advance", 409)
	}
	ctx, cancel := context.WithDeadline(parent, r.DeadlineAt)
	defer cancel()
	save := func(c context.Context) error {
		if err := checkHistory(r); err != nil {
			return err
		}
		if checkpoint != nil {
			return checkpoint(c, r)
		}
		r.Revision++
		return nil
	}
	defer func() {
		if resultErr != nil {
			// A canceled persistence operation can return context.Canceled rather
			// than the infrastructure cause. Preserve storage/CAS failures, but
			// do not label lease loss as a user cancellation or ordinary tool error.
			code := llm.AsError(resultErr).Code
			if cause := llm.ContextError(ctx); cause != nil && llm.AsError(cause).Control && code != "storage_unavailable" && code != "conflict" {
				resultErr = cause
			}
			r.Status = "failed"
			if errors.Is(resultErr, context.Canceled) {
				r.Status = "cancelled"
			}
			ue := llm.AsError(resultErr)
			r.Error = &PublicError{Code: ue.Code, Message: ue.Message}
			r.Output = nil
			for i := range r.ToolCalls {
				if r.ToolCalls[i].Status == "running" {
					r.ToolCalls[i].Status = "outcome_unknown"
				}
			}
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			if checkpoint != nil {
				if err := checkpoint(cleanup, r); err != nil {
					resultErr = err
				}
			}
		}
	}()
	if resultErr = e.ValidateRun(r); resultErr != nil {
		return resultErr
	}
	var observationMu sync.Mutex
	observer := llm.ObserverFuncs{
		BeforeFunc: func(c context.Context, request llm.Request) error {
			observationMu.Lock()
			if err := llm.ContextError(c); err != nil {
				observationMu.Unlock()
				return err
			}
			if r.Counters.Calls >= r.Limits.MaxCalls || r.Counters.ReservedOutput+request.MaxTokens > r.Limits.MaxReservedOutput {
				observationMu.Unlock()
				return llm.Failure("budget_exhausted", "model call budget exhausted", 429)
			}
			m, ok := e.catalog.Get(request.Model)
			if !ok || (len(r.AllowedProviders) > 0 && !slices.Contains(r.AllowedProviders, m.Provider)) {
				observationMu.Unlock()
				return llm.Invalid("model outside run policy")
			}
			r.Counters.Calls++
			r.Counters.ReservedOutput += request.MaxTokens
			number := r.Counters.Steps
			if request.Purpose == "decision" {
				number++
			}
			r.Steps = append(r.Steps, Step{Attempt: r.Counters.Calls, Number: number, Kind: request.Purpose, Model: request.Model, Status: "running", StartedAt: time.Now().UTC()})
			if err := save(c); err != nil {
				observationMu.Unlock()
				return err
			}
			return nil
		},
		AfterFunc: func(c context.Context, _ llm.Request, response llm.Response, callErr error) error {
			defer observationMu.Unlock()
			i := len(r.Steps) - 1
			now := time.Now().UTC()
			r.Steps[i].EndedAt = &now
			r.Steps[i].Usage = response.Usage
			r.Steps[i].Status = "completed"
			if response.FinishReason == "length" {
				r.Steps[i].Status = "incomplete"
			}
			if callErr != nil {
				r.Steps[i].Status = "failed"
				response.Usage.Known = false
			}
			r.TotalUsage.Add(response.Usage)
			if c.Err() != nil {
				cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				return save(cleanup)
			}
			return save(c)
		}}
	ctx = llm.WithObserver(ctx, observer)
	for {
		if err := llm.ContextError(ctx); err != nil {
			return err
		}
		if r.Counters.Steps >= r.Limits.MaxSteps || r.Counters.Calls >= r.Limits.MaxCalls {
			return llm.Failure("budget_exhausted", "run execution limit reached", 429)
		}
		view, err := e.view(r, sink != nil)
		if err != nil {
			return err
		}
		decision, err := e.decider.Decide(ctx, view)
		if err != nil {
			ue := llm.AsError(err)
			code := ue.Code
			if ce := llm.ContextError(ctx); ce != nil {
				return ce
			}
			if ue.Control || code == "storage_unavailable" || code == "conflict" || code == "budget_exhausted" {
				return err
			}
		}
		if err != nil || !validDecision(view, decision) {
			decision, err = (RuleDecider{}).Decide(ctx, view)
			if err != nil {
				return err
			}
		}
		model, _ := e.catalog.Get(decision.Model)
		request, err := buildRequest(r, model, decision)
		if err != nil {
			return err
		}
		if _, err = e.catalog.Resolve(request, sink != nil && decision.Action == "respond"); err != nil {
			return err
		}
		r.Counters.Steps++
		r.Status = "running"
		if decision.Action == "respond" {
			r.Status = "responding"
		}
		if err = save(ctx); err != nil {
			return err
		}
		var response llm.Response
		if decision.Action == "respond" && sink != nil {
			response, err = e.caller.Stream(ctx, request, func(event llm.Event) error {
				if event.Type == "tool_start" || event.Type == "tool_delta" {
					return llm.ProtocolError("tool emitted during final response")
				}
				return sink(event)
			})
		} else {
			response, err = e.caller.Generate(ctx, request)
		}
		if err != nil {
			return err
		}
		if decision.Action == "respond" {
			r.Output = &response
			r.Status = "completed"
			if response.FinishReason == "length" {
				r.Status = "incomplete"
			}
			return save(ctx)
		}
		hasTools := false
		for _, b := range response.Content {
			if b.Type == "tool_call" {
				hasTools = true
			}
		}
		if hasTools {
			if err = e.handleTools(ctx, r, response, save); err != nil {
				return err
			}
			if r.Status == "awaiting_tool_results" {
				return nil
			}
		} else {
			r.History = append(r.History, llm.Message{Role: "assistant", Content: response.Content})
			if err = save(ctx); err != nil {
				return err
			}
		}
	}
}
func (e *Engine) handleTools(ctx context.Context, r *Run, response llm.Response, save func(context.Context) error) error {
	specs := map[string]ToolSpec{}
	for _, s := range r.Tools {
		specs[s.Definition.Name] = s
	}
	var calls []ToolCall
	for i, b := range response.Content {
		if b.Type != "tool_call" {
			continue
		}
		spec, ok := specs[b.Name]
		if !ok {
			return llm.ProtocolError("unknown tool")
		}
		if err := validateArgs(spec, b.Arguments); err != nil {
			return llm.ProtocolError("tool arguments failed schema validation")
		}
		id := remapID()
		calls = append(calls, ToolCall{ID: id, ProviderID: b.ID, Name: b.Name, Arguments: b.Arguments, Executor: spec.Executor, Step: r.Counters.Steps, Status: "planned"})
		response.Content[i].ID = id
	}
	if len(r.ToolCalls)+len(calls) > 64 {
		return llm.Invalid("run tool call limit exceeded")
	}
	r.ToolConstraintSatisfied = true
	r.History = append(r.History, llm.Message{Role: "assistant", Content: response.Content})
	start := len(r.ToolCalls)
	r.ToolCalls = append(r.ToolCalls, calls...)
	r.Batch = nil
	for _, c := range calls {
		r.Batch = append(r.Batch, c.ID)
	}
	if err := save(ctx); err != nil {
		return err
	}
	for i := start; i < len(r.ToolCalls); i++ {
		call := &r.ToolCalls[i]
		if call.Executor != "server" {
			continue
		}
		if err := llm.ContextError(ctx); err != nil {
			return err
		}
		tool, _ := e.tools.get(call.Name)
		call.Status = "running"
		if err := save(ctx); err != nil {
			return err
		}
		if err := llm.ContextError(ctx); err != nil {
			return err
		}
		toolCtx, cancel := context.WithTimeout(ctx, time.Duration(r.Limits.ToolSeconds)*time.Second)
		value, err := executeTool(toolCtx, tool, call.Arguments)
		if toolCtx.Err() != nil {
			err = toolCtx.Err()
		}
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errToolPanic) {
				call.Status = "outcome_unknown"
				_ = save(ctx)
				return llm.Failure("tool_outcome_unknown", "tool outcome is unknown; not replayed", 502)
			}
			value = "tool execution failed"
			call.Status = "failed"
		} else {
			call.Status = "succeeded"
		}
		if len(value) > 32768 {
			return llm.Failure("tool_output_limit", "tool result exceeded size limit", 502)
		}
		call.Result = &ToolResult{CallID: call.ID, Content: value, IsError: err != nil}
		if err = save(ctx); err != nil {
			return err
		}
	}
	waiting := false
	for i := start; i < len(r.ToolCalls); i++ {
		if r.ToolCalls[i].Executor == "client" {
			r.ToolCalls[i].Status = "waiting"
			waiting = true
		}
	}
	if waiting {
		r.Status = "awaiting_tool_results"
		if r.Compatibility {
			output := response
			output.Content = nil
			for _, b := range response.Content {
				if b.Type == "tool_call" && specs[b.Name].Executor == "client" {
					output.Content = append(output.Content, b)
				}
			}
			r.Output = &output
		}
		return save(ctx)
	}
	appendBatchResults(r)
	return save(ctx)
}

var errToolPanic = errors.New("tool panicked with unknown outcome")

func executeTool(ctx context.Context, tool ServerTool, args json.RawMessage) (value string, err error) {
	defer func() {
		if recover() != nil {
			err = errToolPanic
		}
	}()
	return tool.Execute(ctx, args)
}
