package tools_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kkang3290/route-mind/internal/tools"
)

func TestBuiltinsArePureBoundedAndRejectInvalidInputs(t *testing.T) {
	for _, tc := range []struct {
		name, args, want string
		bad              bool
	}{{"sum", `{"a":2,"b":3}`, "5", false}, {"sum", `{"a":9223372036854775807,"b":1}`, "", true}, {"sum", `{"a":2}`, "", true}, {"lookup", `{"key":"gateway"}`, "An LLM gateway normalizes provider protocols.", false}, {"lookup", `{"key":"https://example.com"}`, "not found", false}} {
		v, e := tools.Execute(context.Background(), tc.name, json.RawMessage(tc.args))
		if (e != nil) != tc.bad || (!tc.bad && v != tc.want) {
			t.Fatalf("%s result=%s err=%v", tc.name, v, e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := tools.Execute(ctx, "sum", json.RawMessage(`{"a":1,"b":2}`)); e == nil {
		t.Fatal("cancelled operation executed")
	}
	if _, e := tools.Registry(); e != nil {
		t.Fatal(e)
	}
}
