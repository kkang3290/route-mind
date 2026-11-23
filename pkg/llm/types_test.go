package llm_test

import (
	"encoding/json"
	"testing"

	"github.com/kkang3290/route-mind/pkg/llm"
)

func TestRequestRejectsInvalidConstraints(t *testing.T) {
	base := func() llm.Request {
		return llm.Request{Model: "small", Messages: []llm.Message{llm.TextMessage("user", "hello")}, MaxTokens: 32}
	}
	cases := []struct {
		name   string
		change func(*llm.Request)
	}{
		{"recursive auto", func(r *llm.Request) { r.Model = "auto" }},
		{"missing conversation", func(r *llm.Request) { r.Messages = nil }},
		{"negative output", func(r *llm.Request) { r.MaxTokens = -1 }},
		{"forced unknown tool", func(r *llm.Request) { r.ToolChoice = llm.ToolChoice{Mode: "named", Name: "missing"} }},
		{"orphan result", func(r *llm.Request) {
			r.Messages = append(r.Messages, llm.Message{Role: "tool", Content: []llm.Block{{Type: "tool_result", CallID: "missing", Text: "oops"}}})
		}},
		{"invalid arguments", func(r *llm.Request) {
			r.Messages = append(r.Messages, llm.Message{Role: "assistant", Content: []llm.Block{{Type: "tool_call", ID: "c1", Name: "sum", Arguments: json.RawMessage(`{`)}}})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := base()
			tc.change(&r)
			if r.Validate() == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}

func TestToolJSONMustBeAnObject(t *testing.T) {
	for _, value := range []string{`{}`, `null`, `[]`, `"text"`, `1`} {
		for _, site := range []string{"schema", "arguments"} {
			t.Run(site+"/"+value, func(t *testing.T) {
				r := llm.Request{Model: "small", MaxTokens: 32, Messages: []llm.Message{llm.TextMessage("user", "hello")}}
				if site == "schema" {
					r.Tools = []llm.Tool{{Name: "sum", Parameters: json.RawMessage(value)}}
				} else {
					r.Messages = append(r.Messages,
						llm.Message{Role: "assistant", Content: []llm.Block{{Type: "tool_call", ID: "c", Name: "sum", Arguments: json.RawMessage(value)}}},
						llm.Message{Role: "tool", Content: []llm.Block{{Type: "tool_result", CallID: "c", Text: "result"}}})
				}
				if e := r.Validate(); (e == nil) != (value == `{}`) {
					t.Fatalf("%s %s: %v", site, value, e)
				}
			})
		}
	}
}

func TestToolHistoryAndModelCapabilities(t *testing.T) {
	r := llm.Request{Model: "small", MaxTokens: 32, Messages: []llm.Message{
		llm.TextMessage("user", "add"),
		{Role: "assistant", Content: []llm.Block{{Type: "tool_call", ID: "c1", Name: "sum", Arguments: json.RawMessage(`{"a":1}`)}}},
		{Role: "tool", Content: []llm.Block{{Type: "tool_result", CallID: "c1", Text: "1"}}},
	}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	catalog, err := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "m", MaxInputBytes: 100, MaxOutputTokens: 64, Stream: true}, {ID: "strong", Provider: "p", Upstream: "n", MaxInputBytes: 4096, MaxOutputTokens: 1024, Stream: true, Tools: true, ForcedTools: true}})
	if err != nil {
		t.Fatal(err)
	}
	r.Tools = []llm.Tool{{Name: "sum", Parameters: json.RawMessage(`{"type":"object"}`)}}
	r.ToolChoice = llm.ToolChoice{Mode: "required"}
	if _, err := catalog.Resolve(r, true); err == nil {
		t.Fatal("small model accepted unsupported tools")
	}
	r.Model = "strong"
	if _, err := catalog.Resolve(r, true); err != nil {
		t.Fatal(err)
	}
	r.MaxTokens = 2048
	if _, err := catalog.Resolve(r, false); err == nil {
		t.Fatal("output limit ignored")
	}
	if _, err := llm.NewCatalog([]llm.Model{{ID: "auto"}}); err == nil {
		t.Fatal("reserved model registered")
	}
}
