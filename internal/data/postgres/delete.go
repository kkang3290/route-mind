package postgres

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/kkang3290/route-mind/pkg/llm"
)

// Delete removes one owned, quiescent run and its children using an exact revision.
// Active operations cannot be deleted underneath external model/tool execution.
func (s *Store) Delete(parent context.Context, owner, id string, revision int64) error {
	if !publicID.MatchString(id) {
		return notFound()
	}
	if revision <= 0 {
		return llm.Invalid("positive run revision is required")
	}
	ctx, cancel := context.WithTimeout(parent, s.options.Timeout)
	defer cancel()
	var deleted string
	e := s.db.QueryRow(ctx, "DELETE FROM "+s.table("agent_runs")+" WHERE principal=$1 AND public_id=$2 AND revision=$3 AND operation_id IS NULL AND status IN ('awaiting_tool_results','completed','incomplete','failed','cancelled') AND expires_at>now() RETURNING public_id::text", owner, id, revision).Scan(&deleted)
	if e == nil {
		return nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return unavailable(e)
	}
	if _, e = s.Get(ctx, owner, id); e != nil {
		return e
	}
	return conflict()
}
