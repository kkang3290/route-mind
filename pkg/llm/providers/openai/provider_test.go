package openai_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/providers/anthropic"
	"github.com/kkang3290/route-mind/pkg/llm/providers/openai"
	"github.com/kkang3290/route-mind/pkg/llm/providers/transport"
)

func TestProvidersSendNativeHeadersAndActuallyStream(t *testing.T) {
	for _, format := range []string{"openai", "anthropic"} {
		t.Run(format, func(t *testing.T) {
			for _, stream := range []bool{false, true} {
				released := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body map[string]any
					if json.NewDecoder(r.Body).Decode(&body) != nil {
						t.Error("bad body")
					}
					if body["model"] != "native" {
						t.Error("wrong upstream model")
					}
					if format == "openai" {
						if r.Header.Get("Authorization") != "Bearer test" || r.URL.Path != "/v1/chat/completions" {
							t.Error("missing OpenAI auth/path")
						}
					} else if r.Header.Get("x-api-key") != "test" || r.Header.Get("anthropic-version") == "" || r.URL.Path != "/v1/messages" {
						t.Error("missing Anthropic auth/path")
					}
					if !stream {
						if format == "openai" {
							fmt.Fprint(w, `{"id":"r","choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
						} else {
							fmt.Fprint(w, `{"id":"r","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1}}`)
						}
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					if format == "openai" {
						fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
					} else {
						fmt.Fprint(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":2}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n")
					}
					w.(http.Flusher).Flush()
					select {
					case <-released:
					case <-r.Context().Done():
						return
					}
					if format == "openai" {
						fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
					} else {
						fmt.Fprint(w, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
					}
				}))
				defer server.Close()
				options := transport.Options{BaseURL: server.URL + "/v1", APIKey: "test", AllowHTTP: true}
				var p llm.Provider
				var e error
				if format == "openai" {
					p, e = openai.New(options)
				} else {
					p, e = anthropic.New(options)
				}
				if e != nil {
					t.Fatal(e)
				}
				r := llm.Request{Model: "native", Messages: []llm.Message{llm.TextMessage("user", "hello")}, MaxTokens: 32}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var out llm.Response
				if stream {
					var text string
					out, e = p.Stream(ctx, r, func(ev llm.Event) error {
						if ev.Type == "text_delta" {
							text += ev.Text
							if text == "hello" {
								close(released)
							}
						}
						return nil
					})
					if text != "hello" {
						t.Fatal(text)
					}
				} else {
					out, e = p.Generate(ctx, r)
				}
				if e != nil || out.Text() != "hello" || !out.Usage.Known {
					t.Fatalf("out=%+v error=%v", out, e)
				}
			}
		})
	}
}
func TestProviderErrorsAndRedirectsDoNotExposeSecrets(t *testing.T) {
	destinationCalled := false
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationCalled = true }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect/chat/completions" {
			http.Redirect(w, r, destination.URL, 302)
			return
		}
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":{"message":"secret-upstream-key"}}`)
	}))
	defer server.Close()
	r := llm.Request{Model: "native", Messages: []llm.Message{llm.TextMessage("user", "hello")}, MaxTokens: 32}
	p, e := openai.New(transport.Options{BaseURL: server.URL, AllowHTTP: true, APIKey: "test"})
	if e != nil {
		t.Fatal(e)
	}
	_, e = p.Generate(context.Background(), r)
	ue := llm.AsError(e)
	if ue.Status != 429 || !ue.Retryable || ue.RetryAfter != "1" || strings.Contains(e.Error(), "secret") {
		t.Fatalf("unsafe error: %+v", ue)
	}
	p, e = openai.New(transport.Options{BaseURL: server.URL + "/redirect", AllowHTTP: true, APIKey: "test"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.Generate(context.Background(), r); e == nil || destinationCalled {
		t.Fatal("redirect followed")
	}
	if _, e = openai.New(transport.Options{BaseURL: "http://example.com/v1", AllowHTTP: true, APIKey: "test"}); e == nil {
		t.Fatal("plaintext external endpoint accepted")
	}
}
