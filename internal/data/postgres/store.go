// Package postgres implements the durable, fenced RunStore without holding locks
// during model or tool execution.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}
type Options struct {
	MaxRuns             int
	MaxRunsPerPrincipal int
	Timeout             time.Duration
}
type Store struct {
	db      DB
	schema  string
	options Options
}

var safeSchema = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
var publicID = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func New(db DB, schema string, o Options) (*Store, error) {
	if db == nil || !safeSchema.MatchString(schema) {
		return nil, llm.Invalid("invalid PostgreSQL store configuration")
	}
	if o.MaxRuns <= 0 {
		o.MaxRuns = 4096
	}
	if o.MaxRunsPerPrincipal <= 0 {
		o.MaxRunsPerPrincipal = 128
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	return &Store{db: db, schema: schema, options: o}, nil
}
func (s *Store) table(name string) string { return pgx.Identifier{s.schema, name}.Sanitize() }
func unavailable(e error) error {
	return &llm.Error{Code: "storage_unavailable", Message: "database operation unavailable; no external operation was replayed", Status: 503, Cause: e}
}
func conflict() error             { return llm.Failure("conflict", "run state or revision changed", 409) }
func notFound() error             { return llm.Failure("not_found", "run not found", 404) }
func keyDigest(key string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(key))) }
func validateIdentity(owner, key, digest string) error {
	if owner == "" || len(owner) > 128 || len(key) == 0 || len(key) > 128 || !hashPattern.MatchString(digest) {
		return llm.Invalid("valid principal, idempotency key and request digest are required")
	}
	return nil
}

type record struct {
	id  int64
	run *agent.Run
}

func scan(row pgx.Row) (record, error) {
	var item record
	var data []byte
	var rev int64
	var status, operation string
	var deadline, expiry time.Time
	if e := row.Scan(&item.id, &data, &rev, &status, &operation, &deadline, &expiry); e != nil {
		return item, e
	}
	var run agent.Run
	if e := json.Unmarshal(data, &run); e != nil {
		return item, e
	}
	run.Revision = rev
	run.Status = status
	run.OperationID = operation
	run.DeadlineAt = deadline
	run.ExpiresAt = expiry
	item.run = &run
	return item, nil
}

const runColumns = "id,state,revision,status,COALESCE(operation_id::text,''),deadline_at,expires_at"

func (s *Store) Get(parent context.Context, owner, id string) (*agent.Run, error) {
	if !publicID.MatchString(id) {
		return nil, notFound()
	}
	ctx, cancel := context.WithTimeout(parent, s.options.Timeout)
	defer cancel()
	r, e := scan(s.db.QueryRow(ctx, "SELECT "+runColumns+" FROM "+s.table("agent_runs")+" WHERE principal=$1 AND public_id=$2 AND expires_at>now()", owner, id))
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, notFound()
	}
	if e != nil {
		return nil, unavailable(e)
	}
	return r.run, nil
}
func (s *Store) lookup(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, owner, scope, key, requestHash string) (*agent.Run, error) {
	var id, digest string
	e := q.QueryRow(ctx, "SELECT r.public_id::text,k.request_digest FROM "+s.table("request_idempotency")+" k JOIN "+s.table("agent_runs")+" r ON r.id=k.run_id WHERE k.principal=$1 AND k.scope=$2 AND k.key_digest=$3", owner, scope, keyDigest(key)).Scan(&id, &digest)
	if e != nil {
		return nil, e
	}
	if digest != requestHash {
		return nil, llm.Failure("conflict", "idempotency key has different request content", 409)
	}
	r, e := scan(q.QueryRow(ctx, "SELECT "+runColumns+" FROM "+s.table("agent_runs")+" WHERE principal=$1 AND public_id=$2", owner, id))
	if e != nil {
		return nil, e
	}
	if time.Now().After(r.run.ExpiresAt) {
		return nil, notFound()
	}
	return r.run, nil
}
func (s *Store) Create(parent context.Context, r *agent.Run, key, digest string) (*agent.Run, bool, error) {
	if e := validateIdentity(r.Owner, key, digest); e != nil {
		return nil, false, e
	}
	ctx, cancel := context.WithTimeout(parent, s.options.Timeout)
	defer cancel()
	tx, e := s.db.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return nil, false, unavailable(e)
	}
	defer tx.Rollback(context.Background())
	// A short global allocation lock makes retained-run quotas atomic. It is never
	// held during model execution and is independent of per-run execution fencing.
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "route-mind:"+s.schema+":allocate"); e != nil {
		return nil, false, unavailable(e)
	}
	existing, e := s.lookup(ctx, tx, r.Owner, "create", key, digest)
	if e == nil {
		return existing, true, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		var ue *llm.Error
		if errors.As(e, &ue) {
			return nil, false, e
		}
		return nil, false, unavailable(e)
	}
	var total, owned int
	if e = tx.QueryRow(ctx, "SELECT count(*),count(*) FILTER (WHERE principal=$1) FROM "+s.table("agent_runs")+" WHERE expires_at>now()", r.Owner).Scan(&total, &owned); e != nil {
		return nil, false, unavailable(e)
	}
	if total >= s.options.MaxRuns {
		return nil, false, llm.Failure("overloaded", "retained run capacity reached", 503)
	}
	if owned >= s.options.MaxRunsPerPrincipal {
		return nil, false, llm.Failure("rate_limited", "principal retained run capacity reached", 429)
	}
	r.OperationID = agent.NewID()
	r.Revision = 1
	data, e := json.Marshal(r)
	if e != nil {
		return nil, false, e
	}
	var internalID int64
	e = tx.QueryRow(ctx, "INSERT INTO "+s.table("agent_runs")+" (public_id,principal,status,revision,schema_version,state,operation_id,deadline_at,expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id", r.ID, r.Owner, r.Status, r.Revision, r.SchemaVersion, data, r.OperationID, r.DeadlineAt, r.ExpiresAt, r.CreatedAt).Scan(&internalID)
	if e != nil {
		return nil, false, unavailable(e)
	}
	if e = s.insertKey(ctx, tx, r, internalID, "create", key, digest); e != nil {
		return nil, false, e
	}
	if e = tx.Commit(ctx); e != nil {
		reconcile, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		found, re := s.lookup(reconcile, s.db, r.Owner, "create", key, digest)
		if re == nil && found.OperationID == r.OperationID {
			return found, false, nil
		}
		return nil, false, unavailable(e)
	}
	return r, false, nil
}
func (s *Store) insertKey(ctx context.Context, tx pgx.Tx, r *agent.Run, id int64, scope, key, digest string) error {
	_, e := tx.Exec(ctx, "INSERT INTO "+s.table("request_idempotency")+" (principal,scope,key_digest,request_digest,operation_id,checkpoint_revision,run_id,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", r.Owner, scope, keyDigest(key), digest, r.OperationID, r.Revision, id, r.ExpiresAt)
	if e != nil {
		return unavailable(e)
	}
	return nil
}
func (s *Store) Resume(parent context.Context, owner, id string, revision int64, key, digest string, results []agent.ToolResult) (*agent.Run, bool, error) {
	if e := validateIdentity(owner, key, digest); e != nil {
		return nil, false, e
	}
	if !publicID.MatchString(id) {
		return nil, false, notFound()
	}
	ctx, cancel := context.WithTimeout(parent, s.options.Timeout)
	defer cancel()
	tx, e := s.db.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return nil, false, unavailable(e)
	}
	defer tx.Rollback(context.Background())
	item, e := scan(tx.QueryRow(ctx, "SELECT "+runColumns+" FROM "+s.table("agent_runs")+" WHERE principal=$1 AND public_id=$2 AND expires_at>now() FOR UPDATE", owner, id))
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, false, notFound()
	}
	if e != nil {
		return nil, false, unavailable(e)
	}
	scope := "resume:" + id
	existing, e := s.lookup(ctx, tx, owner, scope, key, digest)
	if e == nil {
		return existing, true, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		var ue *llm.Error
		if errors.As(e, &ue) {
			return nil, false, e
		}
		return nil, false, unavailable(e)
	}
	r := item.run
	if r.Status != "awaiting_tool_results" || r.Revision != revision || time.Now().After(r.DeadlineAt) {
		return nil, false, conflict()
	}
	if e = agent.ApplyResults(r, results); e != nil {
		return nil, false, e
	}
	r.OperationID = agent.NewID()
	r.Revision++
	data, e := json.Marshal(r)
	if e != nil {
		return nil, false, e
	}
	tag, e := tx.Exec(ctx, "UPDATE "+s.table("agent_runs")+" SET state=$1,status=$2,revision=$3,operation_id=$4,updated_at=now() WHERE id=$5 AND revision=$6 AND operation_id IS NULL AND deadline_at>now()", data, r.Status, r.Revision, r.OperationID, item.id, revision)
	if e != nil {
		return nil, false, unavailable(e)
	}
	if tag.RowsAffected() != 1 {
		return nil, false, conflict()
	}
	if e = s.persistChildren(ctx, tx, item.id, r); e != nil {
		return nil, false, e
	}
	if e = s.insertKey(ctx, tx, r, item.id, scope, key, digest); e != nil {
		return nil, false, e
	}
	if e = tx.Commit(ctx); e != nil {
		reconcile, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		found, re := s.lookup(reconcile, s.db, owner, scope, key, digest)
		if re == nil && found.OperationID == r.OperationID {
			return found, false, nil
		}
		return nil, false, unavailable(e)
	}
	return r, false, nil
}
func (s *Store) Save(parent context.Context, r *agent.Run) error {
	if !publicID.MatchString(r.ID) || r.OperationID == "" {
		return conflict()
	}
	ctx, cancel := context.WithTimeout(parent, s.options.Timeout)
	defer cancel()
	tx, e := s.db.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return unavailable(e)
	}
	defer tx.Rollback(context.Background())
	oldRevision := r.Revision
	operation := r.OperationID
	next := oldRevision + 1
	retainedOperation := operation
	if r.Status != "running" && r.Status != "responding" {
		retainedOperation = ""
	}
	// Marshal the prospective revision without mutating the caller on rollback.
	r.Revision = next
	r.OperationID = retainedOperation
	data, marshalErr := json.Marshal(r)
	r.Revision = oldRevision
	r.OperationID = operation
	if marshalErr != nil {
		return marshalErr
	}
	var internalID int64
	e = tx.QueryRow(ctx, "UPDATE "+s.table("agent_runs")+" SET state=$1,status=$2,revision=$3,operation_id=NULLIF($4,'')::uuid,deadline_at=$5,expires_at=$6,updated_at=now() WHERE public_id=$7 AND principal=$8 AND revision=$9 AND operation_id=$10::uuid AND status IN ('running','responding') RETURNING id", data, r.Status, next, retainedOperation, r.DeadlineAt, r.ExpiresAt, r.ID, r.Owner, oldRevision, operation).Scan(&internalID)
	if errors.Is(e, pgx.ErrNoRows) {
		return conflict()
	}
	if e != nil {
		return unavailable(e)
	}
	if e = s.persistChildren(ctx, tx, internalID, r); e != nil {
		return e
	}
	tag, e := tx.Exec(ctx, "UPDATE "+s.table("request_idempotency")+" SET checkpoint_revision=$1 WHERE run_id=$2 AND operation_id=$3::uuid", next, internalID, operation)
	if e != nil {
		return unavailable(e)
	}
	if tag.RowsAffected() != 1 {
		return conflict()
	}
	if e = tx.Commit(ctx); e != nil {
		reconcile, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		var confirmed int64
		re := s.db.QueryRow(reconcile, "SELECT k.checkpoint_revision FROM "+s.table("request_idempotency")+" k JOIN "+s.table("agent_runs")+" r ON r.id=k.run_id WHERE r.principal=$1 AND r.public_id=$2 AND k.operation_id=$3::uuid", r.Owner, r.ID, operation).Scan(&confirmed)
		if re != nil || confirmed < next {
			return unavailable(e)
		}
	}
	r.Revision = next
	r.OperationID = retainedOperation
	return nil
}
func (s *Store) persistChildren(ctx context.Context, tx pgx.Tx, id int64, r *agent.Run) error {
	batch := &pgx.Batch{}
	for _, step := range r.Steps {
		usage, e := json.Marshal(step.Usage)
		if e != nil {
			return e
		}
		batch.Queue("INSERT INTO "+s.table("agent_steps")+" (run_id,attempt,step_number,phase,model,status,usage,started_at,ended_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(run_id,attempt) DO UPDATE SET status=EXCLUDED.status,usage=EXCLUDED.usage,ended_at=EXCLUDED.ended_at", id, step.Attempt, step.Number, step.Kind, step.Model, step.Status, usage, step.StartedAt, step.EndedAt)
	}
	for _, call := range r.ToolCalls {
		var result []byte
		if call.Result != nil {
			var e error
			result, e = json.Marshal(call.Result)
			if e != nil {
				return e
			}
		}
		batch.Queue("INSERT INTO "+s.table("agent_tool_calls")+" (run_id,call_id,provider_id,step_number,tool_name,executor,arguments,status,result) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(run_id,call_id) DO UPDATE SET status=EXCLUDED.status,result=EXCLUDED.result", id, call.ID, call.ProviderID, call.Step, call.Name, call.Executor, []byte(call.Arguments), call.Status, result)
	}
	if batch.Len() == 0 {
		return nil
	}
	if e := tx.SendBatch(ctx, batch).Close(); e != nil {
		return unavailable(e)
	}
	return nil
}
func (s *Store) Reap(parent context.Context, limit int) error {
	if limit <= 0 || limit > 256 {
		limit = 64
	}
	ctx, cancel := context.WithTimeout(parent, s.options.Timeout)
	defer cancel()
	tx, e := s.db.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return unavailable(e)
	}
	defer tx.Rollback(context.Background())
	rows, e := tx.Query(ctx, "SELECT "+runColumns+" FROM "+s.table("agent_runs")+" WHERE status IN ('running','responding','awaiting_tool_results') AND deadline_at<=now() ORDER BY deadline_at LIMIT $1 FOR UPDATE SKIP LOCKED", limit)
	if e != nil {
		return unavailable(e)
	}
	var items []record
	for rows.Next() {
		item, e := scan(rows)
		if e != nil {
			rows.Close()
			return unavailable(e)
		}
		items = append(items, item)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return unavailable(e)
	}
	for _, item := range items {
		r := item.run
		r.Status = "failed"
		r.Revision++
		r.OperationID = ""
		r.Output = nil
		r.Error = &agent.PublicError{Code: "deadline_exceeded", Message: "run expired; in-flight operations were not replayed"}
		for i := range r.ToolCalls {
			if r.ToolCalls[i].Status == "running" {
				r.ToolCalls[i].Status = "outcome_unknown"
			}
		}
		for i := range r.Steps {
			if r.Steps[i].Status == "running" {
				r.Steps[i].Status = "failed"
				r.Steps[i].Usage.Known = false
				r.TotalUsage.Known = false
			}
		}
		data, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "UPDATE "+s.table("agent_runs")+" SET status='failed',revision=$1,operation_id=NULL,state=$2,updated_at=now() WHERE id=$3", r.Revision, data, item.id); e != nil {
			return unavailable(e)
		}
		if e = s.persistChildren(ctx, tx, item.id, r); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, "DELETE FROM "+s.table("agent_runs")+" WHERE id IN (SELECT id FROM "+s.table("agent_runs")+" WHERE status IN ('completed','incomplete','failed','cancelled') AND expires_at<=now() ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)", limit); e != nil {
		return unavailable(e)
	}
	if e = tx.Commit(ctx); e != nil {
		return unavailable(e)
	}
	return nil
}
