package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

type ServerTool struct {
	Spec    ToolSpec
	Execute func(context.Context, json.RawMessage) (string, error)
}
type ToolRegistry struct{ tools map[string]ServerTool }

func NewToolRegistry(tools []ServerTool) (*ToolRegistry, error) {
	r := &ToolRegistry{tools: map[string]ServerTool{}}
	for _, t := range tools {
		if t.Spec.Executor != "server" || t.Spec.Version == "" || t.Spec.Definition.Name == "" || t.Execute == nil {
			return nil, llm.Invalid("invalid server tool registration")
		}
		if _, ok := r.tools[t.Spec.Definition.Name]; ok {
			return nil, llm.Invalid("duplicate server tool")
		}
		if _, e := compileSchema(t.Spec.Definition.Parameters); e != nil {
			return nil, e
		}
		t.Spec.Definition.Parameters = append(json.RawMessage(nil), t.Spec.Definition.Parameters...)
		r.tools[t.Spec.Definition.Name] = t
	}
	return r, nil
}
func (r *ToolRegistry) Spec(name string) (ToolSpec, bool) {
	if r == nil {
		return ToolSpec{}, false
	}
	t, ok := r.tools[name]
	s := t.Spec
	s.Definition.Parameters = append(json.RawMessage(nil), s.Definition.Parameters...)
	return s, ok
}
func (r *ToolRegistry) get(name string) (ServerTool, bool) {
	if r == nil {
		return ServerTool{}, false
	}
	t, ok := r.tools[name]
	return t, ok
}

type denyLoader struct{}

func (denyLoader) Load(string) (any, error) {
	return nil, errors.New("external schema resources are forbidden")
}
func compileSchema(data json.RawMessage) (*jsonschema.Schema, error) {
	if len(data) > 32768 {
		return nil, llm.Invalid("tool schema exceeds size limit")
	}
	v, e := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if e != nil {
		return nil, llm.Invalid("invalid tool schema JSON")
	}
	nodes := 0
	if !boundedJSON(v, 0, &nodes, true) {
		return nil, llm.Invalid("tool schema references or complexity are not permitted")
	}
	c := jsonschema.NewCompiler()
	c.UseLoader(denyLoader{})
	if e = c.AddResource("urn:route-mind:tool", v); e != nil {
		return nil, llm.Invalid("invalid tool schema")
	}
	s, e := c.Compile("urn:route-mind:tool")
	if e != nil {
		return nil, llm.Invalid("invalid tool schema")
	}
	return s, nil
}
func boundedJSON(v any, depth int, nodes *int, schema bool) bool {
	*nodes++
	if depth > 32 || *nodes > 2048 {
		return false
	}
	switch x := v.(type) {
	case map[string]any:
		for k, y := range x {
			if schema && (k == "$ref" || k == "$dynamicRef" || k == "$recursiveRef") {
				s, ok := y.(string)
				if !ok || !strings.HasPrefix(s, "#") {
					return false
				}
			}
			if !boundedJSON(y, depth+1, nodes, schema) {
				return false
			}
		}
	case []any:
		for _, y := range x {
			if !boundedJSON(y, depth+1, nodes, schema) {
				return false
			}
		}
	}
	return true
}
func validateArgs(spec ToolSpec, args json.RawMessage) error {
	s, e := compileSchema(spec.Definition.Parameters)
	if e != nil {
		return e
	}
	if len(args) > 32768 {
		return llm.Invalid("tool arguments exceed size limit")
	}
	v, e := jsonschema.UnmarshalJSON(bytes.NewReader(args))
	if e != nil {
		return llm.Invalid("invalid tool arguments")
	}
	nodes := 0
	if !boundedJSON(v, 0, &nodes, false) {
		return llm.Invalid("tool arguments exceed complexity limit")
	}
	if e = s.Validate(v); e != nil {
		return llm.Invalid("tool arguments do not match schema")
	}
	return nil
}

// ApplyResults validates an entire client batch before changing any state.
func ApplyResults(r *Run, results []ToolResult) error {
	if r.Status != "awaiting_tool_results" {
		return llm.Failure("conflict", "run is not awaiting client tools", 409)
	}
	wanted := map[string]int{}
	for i, c := range r.ToolCalls {
		if slices.Contains(r.Batch, c.ID) && c.Executor == "client" && c.Status == "waiting" {
			wanted[c.ID] = i
		}
	}
	if len(wanted) == 0 || len(results) != len(wanted) {
		return llm.Invalid("submit the complete pending client tool batch")
	}
	seen := map[string]bool{}
	for _, v := range results {
		if _, ok := wanted[v.CallID]; !ok || seen[v.CallID] || len(v.Content) > 32768 {
			return llm.Invalid("unknown, duplicate or oversized client tool result")
		}
		seen[v.CallID] = true
	}
	for _, v := range results {
		i := wanted[v.CallID]
		copy := v
		r.ToolCalls[i].Result = &copy
		r.ToolCalls[i].Status = "succeeded"
		if v.IsError {
			r.ToolCalls[i].Status = "failed"
		}
	}
	appendBatchResults(r)
	r.Status = "running"
	return nil
}
func appendBatchResults(r *Run) {
	var blocks []llm.Block
	for _, id := range r.Batch {
		for _, c := range r.ToolCalls {
			if c.ID == id && c.Result != nil {
				blocks = append(blocks, llm.Block{Type: "tool_result", CallID: c.ID, Text: c.Result.Content, IsError: c.Result.IsError})
			}
		}
	}
	if len(blocks) > 0 {
		r.History = append(r.History, llm.Message{Role: "tool", Content: blocks})
	}
	r.Batch = nil
}
