// Package demo implements a deterministic LOCAL test provider, not an AI model.
// It never forwards content or makes network requests. Do not deploy it publicly.
package demo

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "ok")
			return
		}
		f := "openai"
		if r.URL.Path == "/v1/messages" {
			f = "anthropic"
		} else if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		if !mockAuthorized(r) {
			w.WriteHeader(401)
			return
		}
		b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if e != nil {
			w.WriteHeader(413)
			return
		}
		req, opts, e := protocol.DecodeRequest(f, b)
		if e != nil {
			w.WriteHeader(400)
			return
		}
		out := llm.Response{ID: "mock_" + agent.NewID(), Model: req.Model, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 10, OutputTokens: 8}}
		if req.Model == "mock-decision" {
			var view agent.View
			if json.Unmarshal([]byte(req.Messages[len(req.Messages)-1].Content[0].Text), &view) != nil {
				w.WriteHeader(400)
				return
			}
			action, model, instruction := "respond", "small", "demo final response"
			if len(view.Tools) > 0 {
				if len(view.History) == 0 || view.RequireTool {
					action, model, instruction = "work", "small", "demo tools"
				}
			} else {
				switch view.Counters.Steps {
				case 0:
					action, model, instruction = "work", "small", "demo extract"
				case 1:
					action, model, instruction = "work", "strong", "demo analyze"
				}
			}
			found := false
			for _, c := range view.Candidates {
				if c.Model.ID == model {
					for _, a := range c.Actions {
						if a == action {
							found = true
						}
					}
				}
			}
			if !found {
				d, e := (agent.RuleDecider{}).Decide(r.Context(), view)
				if e != nil {
					w.WriteHeader(400)
					return
				}
				action, model = d.Action, d.Model
			}
			d, _ := json.Marshal(agent.Decision{Action: action, Model: model, Instruction: instruction})
			out.Content = []llm.Block{{Type: "text", Text: string(d)}}
		} else if len(req.Tools) > 0 && req.ToolChoice.Mode != "none" {
			out.Content = []llm.Block{{Type: "text", Text: "PRIVATE mock tool planning"}}
			for i, t := range req.Tools {
				if req.ToolChoice.Mode == "named" && req.ToolChoice.Name != t.Name {
					continue
				}
				var args map[string]any
				switch t.Name {
				case "sum":
					args = map[string]any{"a": 2, "b": 3}
				case "lookup":
					args = map[string]any{"key": "gateway"}
				default:
					args = map[string]any{}
					var schema struct {
						Properties map[string]struct {
							Type string `json:"type"`
						} `json:"properties"`
					}
					_ = json.Unmarshal(t.Parameters, &schema)
					for name, p := range schema.Properties {
						switch p.Type {
						case "integer", "number":
							args[name] = 2
						case "boolean":
							args[name] = true
						case "object":
							args[name] = map[string]any{}
						case "array":
							args[name] = []any{}
						default:
							args[name] = "mock-value"
						}
					}
				}
				raw, _ := json.Marshal(args)
				out.Content = append(out.Content, llm.Block{Type: "tool_call", ID: "mock_call_" + string(rune('a'+i)), Name: t.Name, Arguments: raw})
			}
			out.FinishReason = "tool_calls"
		} else {
			text := "Local mock answer (no real LLM was called)."
			for _, m := range req.Messages {
				for _, b := range m.Content {
					if b.Type == "text" && strings.Contains(b.Text, "demo extract") {
						text = "PRIVATE mock extracted facts"
					}
					if b.Type == "text" && strings.Contains(b.Text, "demo analyze") {
						text = "PRIVATE mock analysis"
					}
					if b.Type == "text" && strings.Contains(b.Text, "demo final response") {
						text = "Local mock answer after small → strong → small (no real LLM was called)."
					}
					if b.Type == "tool_result" {
						text = "Local mock answer with tool result: " + b.Text
					}
				}
			}
			out.Content = []llm.Block{{Type: "text", Text: text}}
		}
		if !opts.Stream {
			b, _ = protocol.EncodeResponse(f, out)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		encoder := protocol.NewEncoder(f, w, out.ID, req.Model, true)
		for _, block := range out.Content {
			if block.Type == "text" {
				for _, part := range strings.Fields(block.Text) {
					if e = encoder.Event(llm.Event{Type: "text_delta", Text: part + " "}); e != nil {
						return
					}
					if e = http.NewResponseController(w).Flush(); e != nil {
						return
					}
				}
			}
		}
		_ = encoder.Finish(out)
	})
}
