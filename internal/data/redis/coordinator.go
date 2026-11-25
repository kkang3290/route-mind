// Package redis coordinates expiring gateway permissions. It never stores LLM
// input, output, credentials, tools or Run state.
package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kkang3290/route-mind/internal/reliability"
	"github.com/kkang3290/route-mind/pkg/llm"
	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

type ProviderIdentity struct{ ID, Format, Endpoint string }
type Policy struct {
	RequestLimit, PrincipalLimit, RatePerMinute, Burst int
	UpstreamLimit, ProviderLimit, CircuitFailures      int
	CircuitCooldown                                    time.Duration
	Providers                                          []ProviderIdentity
}

type Metrics struct {
	Operations, Errors, Nanoseconds, RateDenied, CapacityDenied, CircuitDenied, LeaseLost, Probes atomic.Int64
}

type Coordinator struct {
	client         *goredis.Client
	settings       reliability.Settings
	policy         Policy
	digest, prefix string
	providers      map[string]bool
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	closed         bool
	wg             sync.WaitGroup
	once           sync.Once
	closeErr       error
	Metrics        Metrics
}

type controller struct {
	coordinator *Coordinator
	kind        string
}

func New(parent context.Context, raw string, s reliability.Settings, p Policy) (*Coordinator, error) {
	if s.Mode != "redis" {
		return nil, errors.New("Redis coordinator requires redis mode")
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := reliability.ValidateRedisURL(raw, s.AllowInsecureLocal, false); err != nil {
		return nil, err
	}
	if p.RequestLimit < 1 || p.RequestLimit > 4096 || p.PrincipalLimit < 1 || p.PrincipalLimit > p.RequestLimit || p.RatePerMinute < 1 || p.RatePerMinute > 100000000 || p.Burst < 1 || p.Burst > 1000000 || p.UpstreamLimit < 1 || p.UpstreamLimit > 4096 || p.ProviderLimit < 1 || p.ProviderLimit > p.UpstreamLimit || p.CircuitFailures < 0 || p.CircuitFailures > 1000 || p.CircuitCooldown < time.Millisecond || p.CircuitCooldown > 300*time.Second || len(p.Providers) == 0 {
		return nil, errors.New("invalid coordination policy")
	}
	p.Providers = append([]ProviderIdentity(nil), p.Providers...)
	providers := make(map[string]bool, len(p.Providers))
	for i := range p.Providers {
		item := &p.Providers[i]
		if item.ID == "" || len(item.ID) > 64 || providers[item.ID] || (item.Format != "openai" && item.Format != "anthropic") {
			return nil, errors.New("invalid coordination provider identity")
		}
		providers[item.ID] = true
		if item.Endpoint == "" {
			if item.Format == "openai" {
				item.Endpoint = "https://api.openai.com/v1"
			} else {
				item.Endpoint = "https://api.anthropic.com/v1"
			}
		}
		u, err := url.Parse(strings.TrimRight(item.Endpoint, "/"))
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
			return nil, errors.New("invalid coordination provider endpoint")
		}
		u.Host = strings.ToLower(u.Host)
		item.Endpoint = u.String()
	}
	sort.Slice(p.Providers, func(i, j int) bool { return p.Providers[i].ID < p.Providers[j].ID })
	// Exclude secrets and instance-specific transport/listener settings. A protocol
	// change must bump this version and use the documented drain procedure.
	encoded, _ := json.Marshal(struct {
		Version               int
		Policy                Policy
		TTL, Renew, Operation int
	}{1, p, s.LeaseTTLMS, s.RenewIntervalMS, s.OperationTimeoutMS})
	sum := sha256.Sum256(encoded)
	opts, err := goredis.ParseURL(raw)
	if err != nil {
		return nil, errors.New("invalid Redis connection configuration")
	}
	opts.Protocol = 2
	opts.MaxRetries = -1
	opts.DialerRetries = 1
	opts.DialTimeout = time.Duration(s.DialTimeoutMS) * time.Millisecond
	opts.ReadTimeout = time.Duration(s.OperationTimeoutMS) * time.Millisecond
	opts.WriteTimeout = opts.ReadTimeout
	opts.PoolTimeout = opts.ReadTimeout
	opts.ContextTimeoutEnabled = true
	opts.PoolSize = s.MaxConnections
	opts.MaxActiveConns = s.MaxConnections
	opts.MaxConcurrentDials = s.MaxConnections
	opts.PipelinePoolSize = -1
	opts.DisableIdentity = true
	opts.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	if opts.TLSConfig != nil {
		opts.TLSConfig.MinVersion = tls.VersionTLS12
		opts.TLSConfig.InsecureSkipVerify = false
	}
	ctx, cancel := context.WithCancel(parent)
	c := &Coordinator{client: goredis.NewClient(opts), settings: s, policy: p, digest: hex.EncodeToString(sum[:]), prefix: "route-mind:{" + s.Namespace + "}:", providers: providers, ctx: ctx, cancel: cancel}
	initCtx, stop := context.WithTimeout(ctx, time.Duration(s.OperationTimeoutMS)*time.Millisecond)
	_, err = c.client.SetNX(initCtx, c.prefix+"policy", c.digest, 0).Result()
	stop()
	if err == nil {
		err = c.Ready(ctx)
	} else {
		err = unavailable()
	}
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Coordinator) Admission() reliability.Admission { return controller{c, "request"} }
func (c *Coordinator) Upstream() llm.AttemptController  { return controller{c, "upstream"} }
func (c *Coordinator) Ready(ctx context.Context) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed || c.ctx.Err() != nil {
		return unavailable()
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(c.settings.OperationTimeoutMS)*time.Millisecond)
	defer cancel()
	value, err := c.client.Get(bounded, c.prefix+"policy").Result()
	if err == goredis.Nil {
		return policyMismatch()
	}
	if err != nil {
		return unavailable()
	}
	if value != c.digest {
		return policyMismatch()
	}
	return nil
}
func (c *Coordinator) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.wg.Wait()
		c.closeErr = c.client.Close()
	})
	return c.closeErr
}

func (c controller) Acquire(ctx context.Context, scope string) (llm.Permit, error) {
	owner := c.coordinator
	if err := llm.ContextError(ctx); err != nil {
		return nil, err
	}
	if scope == "" || len(scope) > 64 || (c.kind == "upstream" && !owner.providers[scope]) {
		return nil, &llm.Error{Control: true, Code: "invalid_request", Message: "invalid coordination scope", Status: 400}
	}
	id := uniqueID()
	keys := owner.keys(c.kind, scope, id)
	started := time.Now()
	err := owner.execute(ctx, keys, "acquire", c.kind, id, llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
	if err != nil {
		return nil, err
	}
	permit := newLease(owner, ctx, c.kind, id, keys, started)
	if err = permit.Check(); err != nil {
		_ = permit.Complete(llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
		return nil, err
	}
	owner.mu.Lock()
	if owner.closed || owner.ctx.Err() != nil {
		owner.mu.Unlock()
		permit.cancel(unavailable())
		_ = permit.Complete(llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
		return nil, unavailable()
	}
	owner.wg.Add(1)
	permit.mu.Lock()
	permit.started = true
	permit.mu.Unlock()
	go permit.maintain()
	owner.mu.Unlock()
	return permit, nil
}

func (c *Coordinator) keys(kind, scope, id string) []string {
	hash := sha256.Sum256([]byte(scope))
	subject := hex.EncodeToString(hash[:])
	state := "bucket:"
	if kind == "upstream" {
		state = "circuit:"
	}
	return []string{c.prefix + "policy", c.prefix + kind + ":global", c.prefix + kind + ":scope:" + subject, c.prefix + kind + ":" + state + subject, c.prefix + kind + ":permit:" + id}
}

func (c *Coordinator) execute(parent context.Context, keys []string, action, kind, id string, o llm.AttemptOutcome) error {
	if c.ctx.Err() != nil && action != "complete" {
		return unavailable()
	}
	started := time.Now()
	defer func() { c.Metrics.Nanoseconds.Add(time.Since(started).Nanoseconds()) }()
	c.Metrics.Operations.Add(1)
	ctx, cancel := context.WithTimeout(parent, time.Duration(c.settings.OperationTimeoutMS)*time.Millisecond)
	defer cancel()
	global, scoped := c.policy.RequestLimit, c.policy.PrincipalLimit
	if kind == "upstream" {
		global, scoped = c.policy.UpstreamLimit, c.policy.ProviderLimit
	}
	dispatched := "0"
	if o.Dispatched {
		dispatched = "1"
	}
	retry := int64(0)
	if n, e := strconv.ParseInt(o.RetryAfter, 10, 64); e == nil && n > 0 {
		if n > 300 {
			n = 300
		}
		retry = n * 1000
	} else if date, e := time.Parse(time.RFC1123, o.RetryAfter); e == nil {
		retry = time.Until(date).Milliseconds()
		if retry < 0 {
			retry = 0
		}
		if retry > 300000 {
			retry = 300000
		}
	}
	epoch := uniqueID()
	args := []interface{}{c.digest, action, kind, id, c.settings.LeaseTTLMS, global, scoped, c.policy.RatePerMinute, c.policy.Burst, c.policy.CircuitFailures, c.policy.CircuitCooldown.Milliseconds(), string(o.Kind), dispatched, retry, epoch}
	result, err := coordinationScript.Run(ctx, c.client, keys, args...).Slice()
	if err != nil {
		c.Metrics.Errors.Add(1)
		if e := llm.ContextError(parent); e != nil {
			return e
		}
		return unavailable()
	}
	if len(result) < 2 {
		c.Metrics.Errors.Add(1)
		return unavailable()
	}
	code, ok := result[0].(string)
	if !ok {
		return unavailable()
	}
	if code == "ok" {
		if len(result) > 2 && result[2] == "probe" {
			c.Metrics.Probes.Add(1)
		}
		return nil
	}
	delay, _ := strconv.ParseFloat(toString(result[1]), 64)
	seconds := int64((delay + 999) / 1000)
	if seconds < 1 {
		seconds = 1
	}
	if seconds > 300 {
		seconds = 300
	}
	var failure *llm.Error
	switch code {
	case "rate":
		c.Metrics.RateDenied.Add(1)
		failure = &llm.Error{Code: "rate_limited", Message: "principal rate or concurrency limit reached", Status: 429}
	case "capacity":
		c.Metrics.CapacityDenied.Add(1)
		failure = &llm.Error{Code: "overloaded", Message: "shared concurrency capacity reached", Status: 503}
	case "open":
		c.Metrics.CircuitDenied.Add(1)
		failure = &llm.Error{Code: "circuit_open", Message: "upstream circuit is open", Status: 503}
	case "lost":
		c.Metrics.LeaseLost.Add(1)
		return lostLease()
	case "policy":
		return policyMismatch()
	default:
		c.Metrics.Errors.Add(1)
		return unavailable()
	}
	failure.Control = true
	failure.RetryAfter = strconv.FormatInt(seconds, 10)
	return failure
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	if n, ok := v.(int64); ok {
		return strconv.FormatInt(n, 10)
	}
	return ""
}
func uniqueID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
func unavailable() error {
	return &llm.Error{Control: true, Code: "coordination_unavailable", Message: "shared coordination unavailable", Status: 503}
}
func lostLease() error {
	return &llm.Error{Control: true, Code: "lease_lost", Message: "coordination lease lost", Status: 503}
}
func policyMismatch() error {
	return &llm.Error{Control: true, Code: "coordination_policy_mismatch", Message: "coordination policy missing or incompatible", Status: 503}
}
