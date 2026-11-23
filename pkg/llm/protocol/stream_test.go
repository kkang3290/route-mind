package protocol_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

func anthropicTextDeltas(count int) string {
	var stream strings.Builder
	stream.WriteString("data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\"}}\n\n")
	stream.WriteString("data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"seed\"}}\n\n")
	for range count {
		fmt.Fprintf(&stream, "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"%s\"}}\n\n", strings.Repeat("x", 64))
	}
	stream.WriteString("data: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
	stream.WriteString("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n")
	stream.WriteString("data: {\"type\":\"message_stop\"}\n\n")
	return stream.String()
}

func TestAnthropicManyTextDeltasPreserveContent(t *testing.T) {
	var emitted strings.Builder
	out, e := protocol.DecodeStream("anthropic", strings.NewReader(anthropicTextDeltas(1024)), func(event llm.Event) error {
		if event.Type == "text_delta" {
			emitted.WriteString(event.Text)
		}
		return nil
	})
	want := "seed" + strings.Repeat("x", 64*1024)
	if e != nil || out.Text() != want || emitted.String() != want {
		t.Fatalf("text length=%d emitted=%d error=%v", len(out.Text()), emitted.Len(), e)
	}
}

func BenchmarkAnthropicTextDeltas(b *testing.B) {
	stream := anthropicTextDeltas(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, e := protocol.DecodeStream("anthropic", strings.NewReader(stream), func(llm.Event) error { return nil }); e != nil {
			b.Fatal(e)
		}
	}
}
