// Package tools contains deliberately pure demonstration tools: no filesystem,
// arbitrary networking, command execution or other external side effects.
package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

func Execute(ctx context.Context, name string, args json.RawMessage) (string, error) {
	if e := ctx.Err(); e != nil {
		return "", e
	}
	d := json.NewDecoder(bytes.NewReader(args))
	d.DisallowUnknownFields()
	var result string
	switch name {
	case "sum":
		var v struct {
			A *int64 `json:"a"`
			B *int64 `json:"b"`
		}
		if d.Decode(&v) != nil || v.A == nil || v.B == nil {
			return "", fmt.Errorf("two integer arguments are required")
		}
		a, b := *v.A, *v.B
		if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
			return "", fmt.Errorf("integer overflow")
		}
		result = strconv.FormatInt(a+b, 10)
	case "lookup":
		var v struct {
			Key string `json:"key"`
		}
		if d.Decode(&v) != nil || v.Key == "" || len(v.Key) > 128 {
			return "", fmt.Errorf("a bounded key is required")
		}
		result = map[string]string{"gateway": "An LLM gateway normalizes provider protocols.", "agent": "An agent selects actions using previous observations."}[v.Key]
		if result == "" {
			result = "not found"
		}
	default:
		return "", fmt.Errorf("unregistered tool")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return "", fmt.Errorf("exactly one argument object is required")
	}
	return result, nil
}
func Registry() (*agent.ToolRegistry, error) {
	schemas := map[string]string{"sum": `{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false}`, "lookup": `{"type":"object","properties":{"key":{"type":"string","minLength":1,"maxLength":128}},"required":["key"],"additionalProperties":false}`}
	descriptions := map[string]string{"sum": "Add two signed 64-bit integers without side effects.", "lookup": "Look up gateway or agent in a fixed local glossary; never fetch a URL."}
	list := []agent.ServerTool{}
	for _, name := range []string{"sum", "lookup"} {
		list = append(list, agent.ServerTool{Spec: agent.ToolSpec{Executor: "server", Version: "1", Definition: llm.Tool{Name: name, Description: descriptions[name], Parameters: json.RawMessage(schemas[name])}}, Execute: func(ctx context.Context, b json.RawMessage) (string, error) { return Execute(ctx, name, b) }})
	}
	return agent.NewToolRegistry(list)
}
