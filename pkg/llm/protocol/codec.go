// Package protocol translates the documented OpenAI/Anthropic text and tool subset.
package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type Options struct {
	Stream       bool
	IncludeUsage bool
}
type function struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Arguments   string          `json:"arguments,omitempty"`
}
type oaCall struct {
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Function function `json:"function"`
}
type oaMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCalls  []oaCall        `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}
type oaRequest struct {
	Model               string          `json:"model"`
	Messages            []oaMessage     `json:"messages"`
	MaxTokens           int             `json:"max_tokens,omitempty"`
	MaxCompletionTokens int             `json:"max_completion_tokens,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	TopP                *float64        `json:"top_p,omitempty"`
	Stop                json.RawMessage `json:"stop,omitempty"`
	Stream              bool            `json:"stream,omitempty"`
	Tools               []struct {
		Type     string   `json:"type"`
		Function function `json:"function"`
	} `json:"tools,omitempty"`
	ToolChoice    json.RawMessage `json:"tool_choice,omitempty"`
	N             int             `json:"n,omitempty"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options,omitempty"`
}
type anthBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}
type anthMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}
type anthRequest struct {
	Model       string          `json:"model"`
	Messages    []anthMessage   `json:"messages"`
	System      json.RawMessage `json:"system,omitempty"`
	MaxTokens   int             `json:"max_tokens"`
	Temperature *float64        `json:"temperature,omitempty"`
	TopP        *float64        `json:"top_p,omitempty"`
	Stop        []string        `json:"stop_sequences,omitempty"`
	Stream      bool            `json:"stream,omitempty"`
	Tools       []struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		InputSchema json.RawMessage `json:"input_schema"`
	} `json:"tools,omitempty"`
	ToolChoice json.RawMessage `json:"tool_choice,omitempty"`
}

func strict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return llm.Invalid("invalid JSON or unsupported field")
	}
	if d.Decode(new(any)) != io.EOF {
		return llm.Invalid("request must contain one JSON value")
	}
	return nil
}
func DecodeRequest(format string, data []byte) (llm.Request, Options, error) {
	r := llm.Request{}
	o := Options{}
	switch format {
	case "openai":
		var v oaRequest
		if e := strict(data, &v); e != nil {
			return r, o, e
		}
		if v.N != 0 && v.N != 1 {
			return r, o, llm.Invalid("only n=1 is supported")
		}
		if v.MaxTokens != 0 && v.MaxCompletionTokens != 0 {
			return r, o, llm.Invalid("specify only one output limit")
		}
		r.Model = v.Model
		r.MaxTokens = v.MaxTokens
		if v.MaxCompletionTokens != 0 {
			r.MaxTokens = v.MaxCompletionTokens
		}
		if r.MaxTokens == 0 {
			r.MaxTokens = 1024
		}
		r.Temperature = v.Temperature
		r.TopP = v.TopP
		o.Stream = v.Stream
		if v.StreamOptions != nil {
			o.IncludeUsage = v.StreamOptions.IncludeUsage
		}
		if len(v.Stop) > 0 && string(v.Stop) != "null" {
			var s string
			if json.Unmarshal(v.Stop, &s) == nil {
				r.Stop = []string{s}
			} else if json.Unmarshal(v.Stop, &r.Stop) != nil {
				return r, o, llm.Invalid("invalid stop sequences")
			}
		}
		for _, m := range v.Messages {
			cm := llm.Message{Role: m.Role}
			if m.Role == "tool" {
				var text string
				if json.Unmarshal(m.Content, &text) != nil {
					return r, o, llm.Invalid("tool result must be text")
				}
				cm.Content = []llm.Block{{Type: "tool_result", CallID: m.ToolCallID, Text: text}}
			} else {
				blocks, e := textBlocks(m.Content)
				if e != nil {
					return r, o, e
				}
				cm.Content = blocks
				for _, c := range m.ToolCalls {
					if c.Type != "function" {
						return r, o, llm.Invalid("only function tools are supported")
					}
					cm.Content = append(cm.Content, llm.Block{Type: "tool_call", ID: c.ID, Name: c.Function.Name, Arguments: json.RawMessage(c.Function.Arguments)})
				}
			}
			r.Messages = append(r.Messages, cm)
		}
		for _, t := range v.Tools {
			if t.Type != "function" {
				return r, o, llm.Invalid("only function tools are supported")
			}
			r.Tools = append(r.Tools, llm.Tool{Name: t.Function.Name, Description: t.Function.Description, Parameters: t.Function.Parameters})
		}
		if len(v.ToolChoice) > 0 {
			var s string
			if json.Unmarshal(v.ToolChoice, &s) == nil {
				r.ToolChoice.Mode = s
			} else {
				var c struct {
					Type     string `json:"type"`
					Function struct {
						Name string `json:"name"`
					} `json:"function"`
				}
				if e := strict(v.ToolChoice, &c); e != nil || c.Type != "function" {
					return r, o, llm.Invalid("invalid tool_choice")
				}
				r.ToolChoice = llm.ToolChoice{Mode: "named", Name: c.Function.Name}
			}
		}
	case "anthropic":
		var v anthRequest
		if e := strict(data, &v); e != nil {
			return r, o, e
		}
		r.Model = v.Model
		r.MaxTokens = v.MaxTokens
		r.Temperature = v.Temperature
		r.TopP = v.TopP
		r.Stop = v.Stop
		o.Stream = v.Stream
		if len(v.System) > 0 {
			blocks, e := textBlocks(v.System)
			if e != nil {
				return r, o, e
			}
			if len(blocks) > 0 {
				r.Messages = append(r.Messages, llm.Message{Role: "system", Content: blocks})
			}
		}
		for _, m := range v.Messages {
			if m.Role != "assistant" && m.Role != "user" {
				return r, o, llm.Invalid("invalid Anthropic message role")
			}
			blocks, e := anthBlocks(m.Content)
			if e != nil {
				return r, o, e
			}
			role := m.Role
			allResults := len(blocks) > 0
			for _, b := range blocks {
				if b.Type != "tool_result" {
					allResults = false
				}
			}
			if allResults {
				role = "tool"
			}
			r.Messages = append(r.Messages, llm.Message{Role: role, Content: blocks})
		}
		for _, t := range v.Tools {
			r.Tools = append(r.Tools, llm.Tool{Name: t.Name, Description: t.Description, Parameters: t.InputSchema})
		}
		if len(v.ToolChoice) > 0 {
			var c struct {
				Type string `json:"type"`
				Name string `json:"name,omitempty"`
			}
			if e := strict(v.ToolChoice, &c); e != nil {
				return r, o, e
			}
			switch c.Type {
			case "auto", "none":
				r.ToolChoice.Mode = c.Type
			case "any":
				r.ToolChoice.Mode = "required"
			case "tool":
				r.ToolChoice = llm.ToolChoice{Mode: "named", Name: c.Name}
			default:
				return r, o, llm.Invalid("unsupported tool choice")
			}
		}
	default:
		return r, o, llm.Invalid("unsupported protocol")
	}
	original := r.Model
	if original == "auto" {
		r.Model = "__validation__"
	}
	e := r.Validate()
	r.Model = original
	return r, o, e
}
func textBlocks(data []byte) ([]llm.Block, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var s string
	if json.Unmarshal(data, &s) == nil {
		return []llm.Block{{Type: "text", Text: s}}, nil
	}
	var arr []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if e := strict(data, &arr); e != nil {
		return nil, e
	}
	var out []llm.Block
	for _, b := range arr {
		if b.Type != "text" {
			return nil, llm.Invalid("only text content is supported")
		}
		out = append(out, llm.Block{Type: "text", Text: b.Text})
	}
	return out, nil
}
func anthBlocks(data []byte) ([]llm.Block, error) {
	var s string
	if json.Unmarshal(data, &s) == nil {
		return []llm.Block{{Type: "text", Text: s}}, nil
	}
	var arr []anthBlock
	if e := strict(data, &arr); e != nil {
		return nil, e
	}
	var out []llm.Block
	for _, b := range arr {
		switch b.Type {
		case "text":
			out = append(out, llm.Block{Type: "text", Text: b.Text})
		case "tool_use":
			out = append(out, llm.Block{Type: "tool_call", ID: b.ID, Name: b.Name, Arguments: b.Input})
		case "tool_result":
			parts, e := textBlocks(b.Content)
			if e != nil {
				return nil, e
			}
			var text string
			for _, p := range parts {
				text += p.Text
			}
			out = append(out, llm.Block{Type: "tool_result", CallID: b.ToolUseID, Text: text, IsError: b.IsError})
		default:
			return nil, llm.Invalid("unsupported content block")
		}
	}
	return out, nil
}
func EncodeRequest(format string, r llm.Request, stream bool) ([]byte, error) {
	m := map[string]any{"model": r.Model, "max_tokens": r.MaxTokens, "stream": stream}
	if r.Temperature != nil {
		m["temperature"] = r.Temperature
	}
	if r.TopP != nil {
		m["top_p"] = r.TopP
	}
	var messages []any
	switch format {
	case "openai":
		for _, msg := range r.Messages {
			var text string
			var calls []oaCall
			for _, b := range msg.Content {
				switch b.Type {
				case "text":
					text += b.Text
				case "tool_call":
					calls = append(calls, oaCall{ID: b.ID, Type: "function", Function: function{Name: b.Name, Arguments: string(b.Arguments)}})
				case "tool_result":
					result := b.Text
					if b.IsError {
						result = "Tool error: " + result
					}
					messages = append(messages, map[string]any{"role": "tool", "tool_call_id": b.CallID, "content": result})
				}
			}
			if msg.Role != "tool" {
				item := map[string]any{"role": msg.Role, "content": text}
				if len(calls) > 0 {
					item["tool_calls"] = calls
					if text == "" {
						item["content"] = nil
					}
				}
				messages = append(messages, item)
			}
		}
		if len(r.Stop) > 0 {
			m["stop"] = r.Stop
		}
		if stream {
			m["stream_options"] = map[string]bool{"include_usage": true}
		}
		if len(r.Tools) > 0 {
			var ts []any
			for _, t := range r.Tools {
				ts = append(ts, map[string]any{"type": "function", "function": map[string]any{"name": t.Name, "description": t.Description, "parameters": t.Parameters}})
			}
			m["tools"] = ts
			if r.ToolChoice.Mode == "named" {
				m["tool_choice"] = map[string]any{"type": "function", "function": map[string]string{"name": r.ToolChoice.Name}}
			} else if r.ToolChoice.Mode != "" {
				m["tool_choice"] = r.ToolChoice.Mode
			}
		}
	case "anthropic":
		var system []string
		for _, msg := range r.Messages {
			if msg.Role == "system" {
				for _, b := range msg.Content {
					if b.Type != "text" {
						return nil, llm.Invalid("system content must be text")
					}
					system = append(system, b.Text)
				}
				continue
			}
			role := msg.Role
			if role == "tool" {
				role = "user"
			}
			blocks := anthEncodeBlocks(msg.Content)
			if len(messages) > 0 {
				prev := messages[len(messages)-1].(map[string]any)
				if prev["role"] == role {
					prev["content"] = append(prev["content"].([]any), blocks...)
					continue
				}
			}
			messages = append(messages, map[string]any{"role": role, "content": blocks})
		}
		if len(system) > 0 {
			m["system"] = strings.Join(system, "\n\n")
		}
		if len(r.Stop) > 0 {
			m["stop_sequences"] = r.Stop
		}
		if len(r.Tools) > 0 {
			var ts []any
			for _, t := range r.Tools {
				ts = append(ts, map[string]any{"name": t.Name, "description": t.Description, "input_schema": t.Parameters})
			}
			m["tools"] = ts
			choice := map[string]string{"type": "auto"}
			switch r.ToolChoice.Mode {
			case "none":
				choice["type"] = "none"
			case "required":
				choice["type"] = "any"
			case "named":
				choice["type"] = "tool"
				choice["name"] = r.ToolChoice.Name
			}
			m["tool_choice"] = choice
		}
	default:
		return nil, llm.Invalid("unsupported protocol")
	}
	m["messages"] = messages
	return json.Marshal(m)
}
func anthEncodeBlocks(blocks []llm.Block) []any {
	out := make([]any, 0, len(blocks))
	for _, b := range blocks {
		switch b.Type {
		case "text":
			out = append(out, map[string]any{"type": "text", "text": b.Text})
		case "tool_call":
			out = append(out, map[string]any{"type": "tool_use", "id": b.ID, "name": b.Name, "input": b.Arguments})
		case "tool_result":
			out = append(out, map[string]any{"type": "tool_result", "tool_use_id": b.CallID, "content": b.Text, "is_error": b.IsError})
		}
	}
	return out
}
func DecodeResponse(format string, data []byte) (llm.Response, error) {
	var out llm.Response
	switch format {
	case "openai":
		var v struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Choices []struct {
				Message oaMessage `json:"message"`
				Finish  string    `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				Input  int64 `json:"prompt_tokens"`
				Output int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(data, &v) != nil || len(v.Choices) != 1 {
			return out, llm.ProtocolError("invalid completion")
		}
		out.ID = v.ID
		out.Model = v.Model
		out.FinishReason = v.Choices[0].Finish
		m := v.Choices[0].Message
		blocks, e := textBlocks(m.Content)
		if e != nil {
			return out, llm.ProtocolError("invalid assistant content")
		}
		out.Content = blocks
		for _, c := range m.ToolCalls {
			if c.Type != "function" {
				return out, llm.ProtocolError("unsupported tool type")
			}
			out.Content = append(out.Content, llm.Block{Type: "tool_call", ID: c.ID, Name: c.Function.Name, Arguments: json.RawMessage(c.Function.Arguments)})
		}
		if v.Usage != nil {
			out.Usage = llm.Usage{InputTokens: v.Usage.Input, OutputTokens: v.Usage.Output, Known: true}
		}
	case "anthropic":
		var v struct {
			ID      string          `json:"id"`
			Model   string          `json:"model"`
			Content json.RawMessage `json:"content"`
			Stop    string          `json:"stop_reason"`
			Usage   *struct {
				Input  int64 `json:"input_tokens"`
				Output int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(data, &v) != nil {
			return out, llm.ProtocolError("invalid message")
		}
		blocks, e := anthBlocks(v.Content)
		if e != nil {
			return out, llm.ProtocolError("invalid content")
		}
		out.ID = v.ID
		out.Model = v.Model
		out.Content = blocks
		out.FinishReason = StopFromAnthropic(v.Stop)
		if v.Usage != nil {
			out.Usage = llm.Usage{InputTokens: v.Usage.Input, OutputTokens: v.Usage.Output, Known: true}
		}
	default:
		return out, llm.Invalid("unsupported protocol")
	}
	if out.FinishReason != "stop" && out.FinishReason != "tool_calls" && out.FinishReason != "length" {
		return out, llm.ProtocolError("unsupported stop reason")
	}
	return out, nil
}
func StopFromAnthropic(reason string) string {
	switch reason {
	case "end_turn", "stop_sequence":
		return "stop"
	case "tool_use":
		return "tool_calls"
	case "max_tokens":
		return "length"
	}
	return ""
}
func StopToAnthropic(reason string) string {
	switch reason {
	case "stop":
		return "end_turn"
	case "tool_calls":
		return "tool_use"
	case "length":
		return "max_tokens"
	}
	return "end_turn"
}
func EncodeResponse(format string, r llm.Response) ([]byte, error) {
	if format == "openai" {
		msg := map[string]any{"role": "assistant", "content": r.Text()}
		var calls []oaCall
		for _, b := range r.Content {
			if b.Type == "tool_call" {
				calls = append(calls, oaCall{ID: b.ID, Type: "function", Function: function{Name: b.Name, Arguments: string(b.Arguments)}})
			}
		}
		if len(calls) > 0 {
			msg["tool_calls"] = calls
			if r.Text() == "" {
				msg["content"] = nil
			}
		}
		v := map[string]any{"id": r.ID, "object": "chat.completion", "created": time.Now().Unix(), "model": r.Model, "choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": r.FinishReason}}}
		if r.Usage.Known {
			v["usage"] = map[string]int64{"prompt_tokens": r.Usage.InputTokens, "completion_tokens": r.Usage.OutputTokens, "total_tokens": r.Usage.InputTokens + r.Usage.OutputTokens}
		}
		return json.Marshal(v)
	}
	if format == "anthropic" {
		v := map[string]any{"id": r.ID, "type": "message", "role": "assistant", "model": r.Model, "content": anthEncodeBlocks(r.Content), "stop_reason": StopToAnthropic(r.FinishReason), "stop_sequence": nil}
		if r.Usage.Known {
			v["usage"] = map[string]int64{"input_tokens": r.Usage.InputTokens, "output_tokens": r.Usage.OutputTokens}
		}
		return json.Marshal(v)
	}
	return nil, fmt.Errorf("unknown protocol")
}
