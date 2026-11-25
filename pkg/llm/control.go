package llm

import "context"

// ContextError preserves trusted cancellation causes (including lease loss).
func ContextError(ctx context.Context) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

func classifyOutcome(err error, neutral bool) AttemptOutcome {
	out := AttemptOutcome{Kind: OutcomeNeutral}
	if neutral {
		return out
	}
	if err == nil {
		out.Kind = OutcomeSuccess
		return out
	}
	ue := AsError(err)
	out.RetryAfter = ue.RetryAfter
	if ue.Status >= 500 && !ue.Control && ue.Code != "upstream_authentication_error" && ue.Code != "deadline_exceeded" {
		out.Kind = OutcomeFailure
	}
	return out
}

// OutcomeKind describes provider health, not the success of local persistence.
type OutcomeKind string

const (
	OutcomeSuccess OutcomeKind = "success"
	OutcomeFailure OutcomeKind = "provider_failure"
	OutcomeNeutral OutcomeKind = "neutral"
)

type AttemptOutcome struct {
	Kind       OutcomeKind
	Dispatched bool
	RetryAfter string
}

// Permit owns one expiring resource allocation. Check must be local, bounded,
// and safe alongside renewal. Complete stops renewal and releases exactly once;
// it uses its own bounded cleanup context, including after caller cancellation.
// It may dispose Context on completion; callers must use their original
// context for post-attempt accounting, not the completed permit context.
type Permit interface {
	Context() context.Context
	Check() error
	Complete(AttemptOutcome) error
}

// AttemptController atomically authorizes shared provider health and capacity.
// The gateway retains independent local hard capacity limits. Implementations
// must return control errors for denials or loss, never retryable model errors.
type AttemptController interface {
	Acquire(context.Context, string) (Permit, error)
}
