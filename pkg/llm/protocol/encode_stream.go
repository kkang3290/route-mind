package protocol

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type wireBlock struct {
	wire                    int
	kind, id, name, pending string
	started, ended, closed  bool
}
type Encoder struct {
	format                  string
	writer                  io.Writer
	id, model               string
	includeUsage            bool
	started, done, haveText bool
	created                 int64
	usage                   llm.Usage
	blocks                  map[int]*wireBlock
	queue                   []*wireBlock
	tools                   map[int]int
	toolIDs                 map[string]bool
	nextBlock, totalBytes   int
}

func NewEncoder(format string, w io.Writer, id, model string, includeUsage bool) *Encoder {
	return &Encoder{format: format, writer: w, id: id, model: model, includeUsage: includeUsage, created: time.Now().Unix(), blocks: map[int]*wireBlock{}, tools: map[int]int{}, toolIDs: map[string]bool{}}
}
func (e *Encoder) frame(event string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	prefix := ""
	if event != "" {
		prefix = "event: " + event + "\n"
	}
	_, err = fmt.Fprintf(e.writer, "%sdata: %s\n\n", prefix, data)
	return err
}
func (e *Encoder) chunk(delta any, finish any) error {
	return e.frame("", map[string]any{"id": e.id, "object": "chat.completion.chunk", "created": e.created, "model": e.model, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}})
}
func (e *Encoder) begin() error {
	if e.started {
		return nil
	}
	e.started = true
	if e.format == "openai" {
		return e.chunk(map[string]any{"role": "assistant", "content": ""}, nil)
	}
	if e.format == "anthropic" {
		return e.frame("message_start", map[string]any{"type": "message_start", "message": map[string]any{"id": e.id, "type": "message", "role": "assistant", "model": e.model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]int64{"input_tokens": e.usage.InputTokens, "output_tokens": 0}}})
	}
	return llm.Invalid("unsupported protocol")
}
func (e *Encoder) Event(v llm.Event) error {
	if e.done {
		return llm.ProtocolError("event after stream completion")
	}
	if v.Type == "usage" {
		e.usage = v.Usage
		return nil
	}
	if v.Type == "block_end" && e.format == "openai" {
		return nil
	}
	e.totalBytes += len(v.Text)
	if e.totalBytes > 2<<20 {
		return llm.ProtocolError("encoded stream exceeds limit")
	}
	if e.format == "openai" {
		if err := e.begin(); err != nil {
			return err
		}
		switch v.Type {
		case "text_delta":
			e.haveText = true
			return e.chunk(map[string]any{"content": v.Text}, nil)
		case "tool_start":
			if _, ok := e.tools[v.Index]; ok {
				return llm.ProtocolError("duplicate tool start")
			}
			index := len(e.tools)
			e.tools[v.Index] = index
			e.toolIDs[v.ID] = true
			return e.chunk(map[string]any{"tool_calls": []any{map[string]any{"index": index, "id": v.ID, "type": "function", "function": map[string]string{"name": v.Name, "arguments": ""}}}}, nil)
		case "tool_delta":
			index, ok := e.tools[v.Index]
			if !ok {
				return llm.ProtocolError("tool delta without start")
			}
			return e.chunk(map[string]any{"tool_calls": []any{map[string]any{"index": index, "function": map[string]string{"arguments": v.Text}}}}, nil)
		default:
			return llm.ProtocolError("unsupported output event")
		}
	}
	if e.format != "anthropic" {
		return llm.Invalid("unsupported protocol")
	}
	switch v.Type {
	case "text_delta":
		e.haveText = true
		b := e.blocks[v.Index]
		if b == nil || b.closed {
			b = &wireBlock{kind: "text"}
			e.blocks[v.Index] = b
			e.queue = append(e.queue, b)
		}
		if b.kind != "text" {
			return llm.ProtocolError("content type changed")
		}
		b.pending += v.Text
	case "tool_start":
		if b := e.blocks[v.Index]; b != nil && !b.closed {
			return llm.ProtocolError("duplicate tool block")
		}
		if len(e.queue) > 0 && e.queue[0].kind == "text" {
			e.queue[0].ended = true
			if err := e.drain(); err != nil {
				return err
			}
		}
		b := &wireBlock{kind: "tool_call", id: v.ID, name: v.Name}
		e.blocks[v.Index] = b
		e.queue = append(e.queue, b)
		e.toolIDs[v.ID] = true
	case "tool_delta":
		b := e.blocks[v.Index]
		if b == nil || b.closed || b.kind != "tool_call" {
			return llm.ProtocolError("tool delta without open block")
		}
		b.pending += v.Text
	case "block_end":
		if b := e.blocks[v.Index]; b != nil && !b.closed {
			b.ended = true
		}
	default:
		return llm.ProtocolError("unsupported output event")
	}
	return e.drain()
}
func (e *Encoder) drain() error {
	for len(e.queue) > 0 {
		b := e.queue[0]
		if b.closed {
			e.queue = e.queue[1:]
			continue
		}
		if err := e.begin(); err != nil {
			return err
		}
		if !b.started {
			b.started = true
			b.wire = e.nextBlock
			e.nextBlock++
			content := map[string]any{"type": "text", "text": ""}
			if b.kind == "tool_call" {
				content = map[string]any{"type": "tool_use", "id": b.id, "name": b.name, "input": map[string]any{}}
			}
			if err := e.frame("content_block_start", map[string]any{"type": "content_block_start", "index": b.wire, "content_block": content}); err != nil {
				return err
			}
		}
		if b.pending != "" {
			delta := map[string]any{"type": "text_delta", "text": b.pending}
			if b.kind == "tool_call" {
				delta = map[string]any{"type": "input_json_delta", "partial_json": b.pending}
			}
			if err := e.frame("content_block_delta", map[string]any{"type": "content_block_delta", "index": b.wire, "delta": delta}); err != nil {
				return err
			}
			b.pending = ""
		}
		if !b.ended {
			break
		}
		if err := e.frame("content_block_stop", map[string]any{"type": "content_block_stop", "index": b.wire}); err != nil {
			return err
		}
		b.closed = true
		e.queue = e.queue[1:]
	}
	return nil
}
func (e *Encoder) Finish(r llm.Response) error {
	if e.done {
		return llm.ProtocolError("stream already complete")
	}
	if !e.haveText && r.Text() != "" {
		if err := e.Event(llm.Event{Type: "text_delta", Index: -1000, Text: r.Text()}); err != nil {
			return err
		}
	}
	for i, b := range r.Content {
		if b.Type == "tool_call" && !e.toolIDs[b.ID] {
			index := -2000 - i
			for _, event := range []llm.Event{{Type: "tool_start", Index: index, ID: b.ID, Name: b.Name}, {Type: "tool_delta", Index: index, Text: string(b.Arguments)}, {Type: "block_end", Index: index}} {
				if err := e.Event(event); err != nil {
					return err
				}
			}
		}
	}
	e.usage = r.Usage
	if err := e.begin(); err != nil {
		return err
	}
	if e.format == "openai" {
		if err := e.chunk(map[string]any{}, r.FinishReason); err != nil {
			return err
		}
		if e.includeUsage && r.Usage.Known {
			if err := e.frame("", map[string]any{"id": e.id, "object": "chat.completion.chunk", "created": e.created, "model": e.model, "choices": []any{}, "usage": map[string]int64{"prompt_tokens": r.Usage.InputTokens, "completion_tokens": r.Usage.OutputTokens, "total_tokens": r.Usage.InputTokens + r.Usage.OutputTokens}}); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(e.writer, "data: [DONE]\n\n"); err != nil {
			return err
		}
	} else {
		for _, b := range e.queue {
			b.ended = true
		}
		if err := e.drain(); err != nil {
			return err
		}
		v := map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": StopToAnthropic(r.FinishReason), "stop_sequence": nil}}
		if r.Usage.Known {
			v["usage"] = map[string]int64{"input_tokens": r.Usage.InputTokens, "output_tokens": r.Usage.OutputTokens}
		}
		if err := e.frame("message_delta", v); err != nil {
			return err
		}
		if err := e.frame("message_stop", map[string]string{"type": "message_stop"}); err != nil {
			return err
		}
	}
	e.done = true
	return nil
}
func ErrorPayload(format string, err error) any {
	e := llm.AsError(err)
	kind := "api_error"
	switch e.Status {
	case 400, 413, 422:
		kind = "invalid_request_error"
	case 401:
		kind = "authentication_error"
	case 403:
		kind = "permission_error"
	case 404:
		kind = "not_found_error"
	case 429:
		kind = "rate_limit_error"
	}
	if format == "anthropic" {
		return map[string]any{"type": "error", "error": map[string]string{"type": kind, "message": e.Message}}
	}
	return map[string]any{"error": map[string]any{"type": kind, "message": e.Message, "code": e.Code, "param": nil}}
}
func (e *Encoder) Error(err error) error {
	if e.done {
		return nil
	}
	e.done = true
	name := ""
	if e.format == "anthropic" {
		name = "error"
	}
	return e.frame(name, ErrorPayload(e.format, err))
}
