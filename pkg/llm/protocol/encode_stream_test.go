package protocol_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

func TestEncodersExposeTextImmediatelyAndUseNativeTermination(t *testing.T) {
	for _, format := range []string{"openai", "anthropic"} {
		t.Run(format, func(t *testing.T) {
			var buf bytes.Buffer
			encoder := protocol.NewEncoder(format, &buf, "gateway-id", "small", true)
			if e := encoder.Event(llm.Event{Type: "text_delta", Index: 0, Text: "early"}); e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(buf.String(), "early") {
				t.Fatal("text buffered until finish")
			}
			if strings.Contains(buf.String(), "[DONE]") || strings.Contains(buf.String(), "message_stop") {
				t.Fatal("premature success")
			}
			out := llm.Response{Content: []llm.Block{{Type: "text", Text: "early"}}, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 3, OutputTokens: 1}}
			if e := encoder.Finish(out); e != nil {
				t.Fatal(e)
			}
			if format == "openai" {
				if !strings.Contains(buf.String(), "[DONE]") || strings.Contains(buf.String(), "event: message_start") {
					t.Fatal(buf.String())
				}
			} else if !strings.Contains(buf.String(), "event: message_stop") {
				t.Fatal(buf.String())
			}
			decoded, e := protocol.DecodeStream(format, bytes.NewReader(buf.Bytes()), func(llm.Event) error { return nil })
			if e != nil || decoded.Text() != "early" || decoded.Usage.InputTokens != 3 {
				t.Fatalf("decoded=%+v err=%v", decoded, e)
			}
		})
	}
}
func TestAnthropicEncoderOrdersInterleavedOpenAITools(t *testing.T) {
	var buf bytes.Buffer
	e := protocol.NewEncoder("anthropic", &buf, "r", "m", true)
	events := []llm.Event{{Type: "tool_start", Index: 1, ID: "c1", Name: "sum"}, {Type: "tool_delta", Index: 1, Text: `{"a":`}, {Type: "tool_start", Index: 2, ID: "c2", Name: "sum"}, {Type: "tool_delta", Index: 2, Text: `{"a":2}`}, {Type: "tool_delta", Index: 1, Text: `1}`}, {Type: "block_end", Index: 1}, {Type: "block_end", Index: 2}}
	for _, v := range events {
		if err := e.Event(v); err != nil {
			t.Fatal(err)
		}
	}
	out := llm.Response{Content: []llm.Block{{Type: "tool_call", ID: "c1", Name: "sum", Arguments: json.RawMessage(`{"a":1}`)}, {Type: "tool_call", ID: "c2", Name: "sum", Arguments: json.RawMessage(`{"a":2}`)}}, FinishReason: "tool_calls", Usage: llm.Usage{Known: true}}
	if err := e.Finish(out); err != nil {
		t.Fatal(err)
	}
	v, err := protocol.DecodeStream("anthropic", bytes.NewReader(buf.Bytes()), func(llm.Event) error { return nil })
	if err != nil || len(v.Content) != 2 || string(v.Content[0].Arguments) != `{"a":1}` || v.Content[1].ID != "c2" {
		t.Fatalf("response=%+v err=%v wire=%s", v, err, buf.String())
	}
}
func TestStreamErrorNeverEmitsSuccessTerminator(t *testing.T) {
	for _, f := range []string{"openai", "anthropic"} {
		var buf bytes.Buffer
		e := protocol.NewEncoder(f, &buf, "r", "m", false)
		_ = e.Event(llm.Event{Type: "text_delta", Text: "partial"})
		if err := e.Error(llm.Failure("upstream_error", "stream failed", 502)); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(buf.String(), "[DONE]") || strings.Contains(buf.String(), "message_stop") {
			t.Fatal("error pretended to complete")
		}
	}
}
