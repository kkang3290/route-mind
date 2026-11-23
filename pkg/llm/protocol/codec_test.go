package protocol_test

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

func TestOpenAICompletionTimestampIsCurrent(t *testing.T) {
	before := time.Now().Unix()
	b, e := protocol.EncodeResponse("openai", llm.Response{ID: "r", Model: "small", Content: []llm.Block{{Type: "text", Text: "answer"}}, FinishReason: "stop"})
	if e != nil {
		t.Fatal(e)
	}
	var out struct {
		Created int64 `json:"created"`
	}
	if e = json.Unmarshal(b, &out); e != nil || out.Created < before || out.Created > time.Now().Unix() {
		t.Fatalf("invalid created timestamp: %s error=%v", b, e)
	}
}

func TestRequestsNormalizeBothProtocolsAndRejectUnsupportedFields(t *testing.T) {
	fixtures := map[string]string{
		"openai":    `{"model":"small","messages":[{"role":"system","content":"Be precise"},{"role":"user","content":"add"},{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"sum","arguments":"{\"a\":1}"}}]},{"role":"tool","tool_call_id":"c1","content":"1"}],"max_tokens":64,"tools":[{"type":"function","function":{"name":"sum","parameters":{"type":"object"}}}],"tool_choice":"auto"}`,
		"anthropic": `{"model":"small","system":"Be precise","messages":[{"role":"user","content":"add"},{"role":"assistant","content":[{"type":"tool_use","id":"c1","name":"sum","input":{"a":1}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"c1","content":"1"}]}],"max_tokens":64,"tools":[{"name":"sum","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"}}`,
	}
	for f, data := range fixtures {
		t.Run(f, func(t *testing.T) {
			r, _, e := protocol.DecodeRequest(f, []byte(data))
			if e != nil {
				t.Fatal(e)
			}
			if r.Model != "small" || len(r.Messages) != 4 || r.Messages[2].Content[0].ID != "c1" || r.Messages[3].Content[0].CallID != "c1" || r.Tools[0].Name != "sum" {
				t.Fatalf("incorrect normalization: %+v", r)
			}
			if e = r.Validate(); e != nil {
				t.Fatal(e)
			}
			for _, target := range []string{"openai", "anthropic"} {
				b, e := protocol.EncodeRequest(target, r, false)
				if e != nil {
					t.Fatal(e)
				}
				if !json.Valid(b) || !strings.Contains(string(b), "c1") {
					t.Fatalf("invalid %s conversion: %s", target, b)
				}
			}
		})
	}
	for _, f := range []string{"openai", "anthropic"} {
		if _, _, e := protocol.DecodeRequest(f, []byte(`{"model":"small","messages":[{"role":"user","content":"x"}],"max_tokens":8,"thinking":{"type":"enabled"}}`)); e == nil {
			t.Fatal("unsupported thinking silently ignored")
		}
	}
}
func TestResponseDecodingUsesRealUsageAndToolCorrelation(t *testing.T) {
	fixtures := map[string]string{
		"openai":    `{"id":"r","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"sum","arguments":"{\"a\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`,
		"anthropic": `{"id":"r","type":"message","role":"assistant","model":"m","content":[{"type":"tool_use","id":"c1","name":"sum","input":{"a":1}}],"stop_reason":"tool_use","usage":{"input_tokens":7,"output_tokens":3}}`,
	}
	for f, data := range fixtures {
		v, e := protocol.DecodeResponse(f, []byte(data))
		if e != nil {
			t.Fatal(e)
		}
		if v.FinishReason != "tool_calls" || len(v.Content) != 1 || v.Content[0].ID != "c1" || v.Usage.InputTokens != 7 || !v.Usage.Known {
			t.Fatalf("wrong %s response: %+v", f, v)
		}
	}
	v, e := protocol.DecodeResponse("openai", []byte(`{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}]}`))
	if e != nil || v.Text() != "hello" || v.Usage.Known {
		t.Fatalf("missing usage invented: %+v %v", v, e)
	}
}
func TestSSEReadsFragmentedEventsAndRequiresTerminalMarker(t *testing.T) {
	fixtures := map[string]string{
		"openai":    ": ping\r\ndata: {\"id\":\"r\",\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\r\n\r\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n",
		"anthropic": "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"r\",\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}
	for f, data := range fixtures {
		t.Run(f, func(t *testing.T) {
			var output string
			v, e := protocol.DecodeStream(f, &fragmentReader{s: data}, func(ev llm.Event) error {
				if ev.Type == "text_delta" {
					output += ev.Text
				}
				return nil
			})
			if e != nil || output != "hello" || v.Text() != "hello" || v.Usage.OutputTokens != 1 {
				t.Fatalf("stream output=%q response=%+v err=%v", output, v, e)
			}
			incomplete := data[:strings.LastIndex(data, "data:")]
			if _, e = protocol.DecodeStream(f, strings.NewReader(incomplete), func(llm.Event) error { return nil }); e == nil {
				t.Fatal("truncated SSE accepted")
			}
		})
	}
}

type fragmentReader struct{ s string }

func (r *fragmentReader) Read(b []byte) (int, error) {
	if len(r.s) == 0 {
		return 0, io.EOF
	}
	n := 2
	if n > len(r.s) {
		n = len(r.s)
	}
	if n > len(b) {
		n = len(b)
	}
	copy(b, r.s[:n])
	r.s = r.s[n:]
	return n, nil
}
