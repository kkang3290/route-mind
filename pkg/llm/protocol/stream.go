package protocol

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"strings"

	"github.com/kkang3290/route-mind/pkg/llm"
)

func readSSE(r io.Reader, callback func(string, []byte) (bool, error)) (bool, error) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), 1<<20)
	var event string
	var data []string
	size := 0
	count := 0
	dispatch := func() (bool, error) {
		if len(data) == 0 {
			event = ""
			return false, nil
		}
		count++
		if count > 65536 {
			return false, llm.ProtocolError("too many SSE events")
		}
		done, e := callback(event, []byte(strings.Join(data, "\n")))
		event = ""
		data = nil
		size = 0
		return done, e
	}
	for scan.Scan() {
		line := strings.TrimSuffix(scan.Text(), "\r")
		if line == "" {
			done, e := dispatch()
			if done || e != nil {
				return done, e
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch key {
		case "event":
			event = value
		case "data":
			size += len(value)
			if size > 1<<20 {
				return false, llm.ProtocolError("SSE event exceeds limit")
			}
			data = append(data, value)
		}
	}
	if scan.Err() != nil {
		return false, llm.ProtocolError("SSE read failed")
	}
	if len(data) > 0 {
		return dispatch()
	}
	return false, nil
}
func DecodeStream(format string, r io.Reader, sink llm.Sink) (llm.Response, error) {
	if format == "openai" {
		return openAIStream(r, sink)
	}
	if format == "anthropic" {
		return anthropicStream(r, sink)
	}
	return llm.Response{}, llm.Invalid("unsupported protocol")
}

type toolBuffer struct {
	ID, Name, Args string
	Started        bool
}

func openAIStream(reader io.Reader, sink llm.Sink) (llm.Response, error) {
	out := llm.Response{}
	var text strings.Builder
	tools := map[int]*toolBuffer{}
	textStarted := false
	ended, e := readSSE(reader, func(_ string, data []byte) (bool, error) {
		if string(data) == "[DONE]" {
			if out.FinishReason == "" {
				return false, llm.ProtocolError("completion without finish reason")
			}
			return true, nil
		}
		var v struct {
			ID      string          `json:"id"`
			Model   string          `json:"model"`
			Error   json.RawMessage `json:"error"`
			Choices []struct {
				Index int `json:"index"`
				Delta struct {
					Content   *string         `json:"content"`
					Reasoning json.RawMessage `json:"reasoning_content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				Finish *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				Input  int64 `json:"prompt_tokens"`
				Output int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(data, &v) != nil || len(v.Error) > 0 || len(v.Choices) > 1 {
			return false, llm.ProtocolError("malformed stream chunk")
		}
		if v.ID != "" {
			out.ID = v.ID
		}
		if v.Model != "" {
			out.Model = v.Model
		}
		if v.Usage != nil {
			out.Usage = llm.Usage{InputTokens: v.Usage.Input, OutputTokens: v.Usage.Output, Known: true}
			if e := sink(llm.Event{Type: "usage", Usage: out.Usage}); e != nil {
				return false, e
			}
		}
		for _, choice := range v.Choices {
			if choice.Index != 0 || len(choice.Delta.Reasoning) > 0 {
				return false, llm.ProtocolError("unsupported stream choice")
			}
			if out.FinishReason != "" && (choice.Delta.Content != nil || len(choice.Delta.ToolCalls) > 0) {
				return false, llm.ProtocolError("content after finish")
			}
			if choice.Delta.Content != nil {
				delta := *choice.Delta.Content
				if text.Len()+len(delta) > 1<<20 {
					return false, llm.ProtocolError("response exceeds limit")
				}
				text.WriteString(delta)
				textStarted = true
				if e := sink(llm.Event{Type: "text_delta", Index: 0, Text: delta}); e != nil {
					return false, e
				}
			}
			for _, call := range choice.Delta.ToolCalls {
				if call.Index < 0 || call.Index >= 16 {
					return false, llm.ProtocolError("invalid tool index")
				}
				tb := tools[call.Index]
				if tb == nil {
					tb = &toolBuffer{}
					tools[call.Index] = tb
				}
				if call.ID != "" {
					if tb.ID != "" && tb.ID != call.ID {
						return false, llm.ProtocolError("tool ID changed")
					}
					tb.ID = call.ID
				}
				if call.Function.Name != "" {
					if tb.Started {
						return false, llm.ProtocolError("tool name changed")
					}
					tb.Name += call.Function.Name
				}
				if len(tb.Name) > 64 || len(tb.Args)+len(call.Function.Arguments) > 32768 {
					return false, llm.ProtocolError("tool delta exceeds limit")
				}
				tb.Args += call.Function.Arguments
				if !tb.Started && tb.ID != "" && tb.Name != "" && len(tb.Args) > 0 {
					tb.Started = true
					if e := sink(llm.Event{Type: "tool_start", Index: call.Index + 1, ID: tb.ID, Name: tb.Name}); e != nil {
						return false, e
					}
					if e := sink(llm.Event{Type: "tool_delta", Index: call.Index + 1, Text: tb.Args}); e != nil {
						return false, e
					}
				} else if tb.Started && call.Function.Arguments != "" {
					if e := sink(llm.Event{Type: "tool_delta", Index: call.Index + 1, Text: call.Function.Arguments}); e != nil {
						return false, e
					}
				}
			}
			if choice.Finish != nil {
				out.FinishReason = *choice.Finish
			}
		}
		return false, nil
	})
	if e != nil {
		return out, e
	}
	if !ended {
		return out, llm.ProtocolError("stream ended without DONE")
	}
	if textStarted {
		out.Content = append(out.Content, llm.Block{Type: "text", Text: text.String()})
		if e := sink(llm.Event{Type: "block_end", Index: 0}); e != nil {
			return out, e
		}
	}
	keys := make([]int, 0, len(tools))
	for key := range tools {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, key := range keys {
		tb := tools[key]
		if !tb.Started {
			if e := sink(llm.Event{Type: "tool_start", Index: key + 1, ID: tb.ID, Name: tb.Name}); e != nil {
				return out, e
			}
			if e := sink(llm.Event{Type: "tool_delta", Index: key + 1, Text: tb.Args}); e != nil {
				return out, e
			}
		}
		if e := sink(llm.Event{Type: "block_end", Index: key + 1}); e != nil {
			return out, e
		}
		out.Content = append(out.Content, llm.Block{Type: "tool_call", ID: tb.ID, Name: tb.Name, Arguments: json.RawMessage(tb.Args)})
	}
	return out, nil
}
func anthropicStream(reader io.Reader, sink llm.Sink) (llm.Response, error) {
	out := llm.Response{}
	blocks := map[int]llm.Block{}
	texts := map[int]*strings.Builder{}
	closed := map[int]bool{}
	started := false
	inputKnown := false
	outputKnown := false
	ended, e := readSSE(reader, func(event string, data []byte) (bool, error) {
		var v struct {
			Type    string `json:"type"`
			Index   int    `json:"index"`
			Message struct {
				ID    string `json:"id"`
				Model string `json:"model"`
				Usage *struct {
					Input  int64 `json:"input_tokens"`
					Output int64 `json:"output_tokens"`
				} `json:"usage"`
			} `json:"message"`
			Block anthBlock `json:"content_block"`
			Delta struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
				Stop        string `json:"stop_reason"`
			} `json:"delta"`
			Usage *struct {
				Input  *int64 `json:"input_tokens"`
				Output int64  `json:"output_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(data, &v) != nil {
			return false, llm.ProtocolError("invalid Anthropic stream")
		}
		if event != "" && event != v.Type {
			return false, llm.ProtocolError("SSE type mismatch")
		}
		switch v.Type {
		case "ping":
			return false, nil
		case "error":
			return false, llm.ProtocolError("upstream stream error")
		case "message_start":
			if started {
				return false, llm.ProtocolError("duplicate message start")
			}
			started = true
			out.ID = v.Message.ID
			out.Model = v.Message.Model
			if v.Message.Usage != nil {
				inputKnown = true
				out.Usage.InputTokens = v.Message.Usage.Input
				if e := sink(llm.Event{Type: "usage", Usage: llm.Usage{InputTokens: out.Usage.InputTokens}}); e != nil {
					return false, e
				}
			}
		case "content_block_start":
			if !started || v.Index < 0 || v.Index >= 32 {
				return false, llm.ProtocolError("invalid block start")
			}
			if _, ok := blocks[v.Index]; ok {
				return false, llm.ProtocolError("duplicate block index")
			}
			switch v.Block.Type {
			case "text":
				blocks[v.Index] = llm.Block{Type: "text"}
				texts[v.Index] = &strings.Builder{}
				texts[v.Index].WriteString(v.Block.Text)
				if v.Block.Text != "" {
					if e := sink(llm.Event{Type: "text_delta", Index: v.Index, Text: v.Block.Text}); e != nil {
						return false, e
					}
				}
			case "tool_use":
				args := ""
				if len(v.Block.Input) > 0 && string(v.Block.Input) != "{}" {
					args = string(v.Block.Input)
				}
				blocks[v.Index] = llm.Block{Type: "tool_call", ID: v.Block.ID, Name: v.Block.Name, Arguments: json.RawMessage(args)}
				if e := sink(llm.Event{Type: "tool_start", Index: v.Index, ID: v.Block.ID, Name: v.Block.Name}); e != nil {
					return false, e
				}
				if args != "" {
					if e := sink(llm.Event{Type: "tool_delta", Index: v.Index, Text: args}); e != nil {
						return false, e
					}
				}
			default:
				return false, llm.ProtocolError("unsupported block type")
			}
		case "content_block_delta":
			b, ok := blocks[v.Index]
			if !ok || closed[v.Index] {
				return false, llm.ProtocolError("delta outside block")
			}
			switch v.Delta.Type {
			case "text_delta":
				if b.Type != "text" || texts[v.Index].Len()+len(v.Delta.Text) > 1<<20 {
					return false, llm.ProtocolError("invalid text delta")
				}
				texts[v.Index].WriteString(v.Delta.Text)
				if e := sink(llm.Event{Type: "text_delta", Index: v.Index, Text: v.Delta.Text}); e != nil {
					return false, e
				}
			case "input_json_delta":
				if b.Type != "tool_call" || len(b.Arguments)+len(v.Delta.PartialJSON) > 32768 {
					return false, llm.ProtocolError("invalid tool delta")
				}
				b.Arguments = append(b.Arguments, v.Delta.PartialJSON...)
				if e := sink(llm.Event{Type: "tool_delta", Index: v.Index, Text: v.Delta.PartialJSON}); e != nil {
					return false, e
				}
			default:
				return false, llm.ProtocolError("unsupported delta")
			}
			blocks[v.Index] = b
		case "content_block_stop":
			b, ok := blocks[v.Index]
			if !ok || closed[v.Index] {
				return false, llm.ProtocolError("invalid block stop")
			}
			if b.Type == "text" {
				b.Text = texts[v.Index].String()
				delete(texts, v.Index)
			} else if b.Type == "tool_call" && len(b.Arguments) == 0 {
				b.Arguments = json.RawMessage(`{}`)
			}
			blocks[v.Index] = b
			closed[v.Index] = true
			if e := sink(llm.Event{Type: "block_end", Index: v.Index}); e != nil {
				return false, e
			}
		case "message_delta":
			out.FinishReason = StopFromAnthropic(v.Delta.Stop)
			if v.Usage != nil {
				if v.Usage.Input != nil {
					out.Usage.InputTokens = *v.Usage.Input
					inputKnown = true
				}
				outputKnown = true
				out.Usage.OutputTokens = v.Usage.Output
			}
		case "message_stop":
			if !started || out.FinishReason == "" {
				return false, llm.ProtocolError("incomplete message")
			}
			for k := range blocks {
				if !closed[k] {
					return false, llm.ProtocolError("unclosed content block")
				}
			}
			return true, nil
		default:
			return false, llm.ProtocolError("unsupported message event")
		}
		return false, nil
	})
	if e != nil {
		return out, e
	}
	if !ended {
		return out, llm.ProtocolError("stream ended without message_stop")
	}
	keys := make([]int, 0, len(blocks))
	for k := range blocks {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		out.Content = append(out.Content, blocks[k])
	}
	out.Usage.Known = inputKnown && outputKnown
	if e := sink(llm.Event{Type: "usage", Usage: out.Usage}); e != nil {
		return out, e
	}
	return out, nil
}
