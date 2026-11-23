package service

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/kkang3290/route-mind/pkg/llm"
)

func (h *Handler) deleteRun(w http.ResponseWriter, r *http.Request, owner, id string) {
	deleter, ok := h.app.Store.(interface {
		Delete(context.Context, string, string, int64) error
	})
	if !ok {
		writeError(w, "openai", llm.Failure("storage_unavailable", "run deletion is unavailable", 503))
		return
	}
	value := r.Header.Get("If-Match")
	value = strings.TrimSuffix(strings.TrimPrefix(value, "\""), "\"")
	revision, e := strconv.ParseInt(value, 10, 64)
	if e != nil || revision <= 0 {
		writeError(w, "openai", llm.Invalid("If-Match with a positive exact run revision is required"))
		return
	}
	if e = deleter.Delete(r.Context(), owner, id, revision); e != nil {
		writeError(w, "openai", e)
		return
	}
	w.WriteHeader(204)
}
