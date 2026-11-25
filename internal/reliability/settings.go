// Package reliability defines application-level admission and Redis settings.
// The reusable LLM/Agent packages do not depend on this package or Redis.
package reliability

import (
	"context"
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type Admission interface {
	Acquire(context.Context, string) (llm.Permit, error)
}

type Settings struct {
	Mode               string `yaml:"mode"`
	Namespace          string `yaml:"namespace"`
	URLEnv             string `yaml:"url_env"`
	AllowInsecureLocal bool   `yaml:"allow_insecure_local"`
	MaxConnections     int    `yaml:"max_connections"`
	DialTimeoutMS      int    `yaml:"dial_timeout_ms"`
	OperationTimeoutMS int    `yaml:"operation_timeout_ms"`
	LeaseTTLMS         int    `yaml:"lease_ttl_ms"`
	RenewIntervalMS    int    `yaml:"renew_interval_ms"`
}

func DefaultSettings() Settings {
	return Settings{Mode: "local", URLEnv: "REDIS_URL", MaxConnections: 32, DialTimeoutMS: 1000, OperationTimeoutMS: 500, LeaseTTLMS: 30000, RenewIntervalMS: 10000}
}

var namespacePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

func (s Settings) Validate() error {
	if s.Mode != "local" && s.Mode != "redis" {
		return errors.New("coordination mode must be local or redis")
	}
	if s.Mode == "local" {
		return nil
	}
	if !namespacePattern.MatchString(s.Namespace) || s.URLEnv == "" {
		return errors.New("Redis coordination requires a valid namespace and URL environment reference")
	}
	if s.MaxConnections < 1 || s.MaxConnections > 256 || s.DialTimeoutMS < 10 || s.DialTimeoutMS > 5000 || s.OperationTimeoutMS < 10 || s.OperationTimeoutMS > 2000 || s.LeaseTTLMS < 300 || s.LeaseTTLMS > 300000 || s.RenewIntervalMS < 20 || s.RenewIntervalMS > s.LeaseTTLMS/3 || s.OperationTimeoutMS >= s.RenewIntervalMS {
		return errors.New("invalid Redis pool, timeout or lease limits")
	}
	return nil
}

// ValidateRedisURL deliberately rejects all query options; SDK URL parameters
// must not override TLS verification, finite timeouts or non-retry semantics.
func ValidateRedisURL(raw string, allowLocal, production bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("invalid Redis connection configuration")
	}
	if u.Port() != "" {
		p, err := strconv.Atoi(u.Port())
		if err != nil || p < 1 || p > 65535 {
			return errors.New("invalid Redis port")
		}
	}
	if u.Path != "" && u.Path != "/" {
		db, err := strconv.Atoi(u.Path[1:])
		if err != nil || db < 0 || db > 15 {
			return errors.New("Redis database index must be between 0 and 15")
		}
	}
	switch u.Scheme {
	case "redis":
		ip := net.ParseIP(u.Hostname())
		if production || !allowLocal || ip == nil || !ip.IsLoopback() {
			return errors.New("plaintext Redis is restricted to explicit literal-loopback development")
		}
	case "rediss":
	default:
		return errors.New("Redis requires rediss or explicit loopback redis")
	}
	if production {
		password, ok := u.User.Password()
		if !ok || password == "" {
			return errors.New("production Redis requires authentication")
		}
	}
	return nil
}
