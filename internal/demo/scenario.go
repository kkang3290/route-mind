package demo

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// NewScenario adds bounded, local-only verification controls to the mock. The
// binary still enforces a literal loopback listener. No request body controls
// faults; deployment flags select them before serving any traffic.
func NewScenario(delay time.Duration, failFirst int) (http.Handler, error) {
	if delay < 0 || delay > 60*time.Second || failFirst < 0 || failFirst > 1000 {
		return nil, errors.New("invalid bounded mock scenario")
	}
	handler := Handler()
	var mu sync.Mutex
	stats := struct {
		Calls  int `json:"calls"`
		Active int `json:"active"`
		Peak   int `json:"peak"`
	}{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_stats" {
			if !mockAuthorized(r) {
				w.WriteHeader(401)
				return
			}
			if r.Method != "GET" {
				w.WriteHeader(405)
				return
			}
			mu.Lock()
			snapshot := stats
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(snapshot)
			return
		}
		if r.Method != "POST" || (r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/messages") || !mockAuthorized(r) {
			handler.ServeHTTP(w, r)
			return
		}
		// Consume the bounded wire body before waiting: net/http can only monitor
		// an HTTP/1 peer disconnect after the incoming body has reached EOF.
		original := r.Body
		data, err := io.ReadAll(http.MaxBytesReader(w, original, 1<<20))
		_ = original.Close()
		if err != nil {
			w.WriteHeader(413)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		mu.Lock()
		stats.Calls++
		stats.Active++
		if stats.Active > stats.Peak {
			stats.Peak = stats.Active
		}
		fail := stats.Calls <= failFirst
		mu.Unlock()
		defer func() { mu.Lock(); stats.Active--; mu.Unlock() }()
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
		}
		if fail {
			w.WriteHeader(503)
			return
		}
		handler.ServeHTTP(w, r)
	}), nil
}
func mockAuthorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer local-mock-only" || r.Header.Get("x-api-key") == "local-mock-only"
}
