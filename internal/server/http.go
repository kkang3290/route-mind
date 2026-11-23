// Package server binds the shared native HTTP boundary to Kratos.
package server

import (
	kratoshttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/kkang3290/route-mind/internal/service"
	"time"
)

type Options = service.Options
type Credential = service.Credential
type Dependencies = service.Dependencies

func New(o Options, d Dependencies) (*kratoshttp.Server, error) {
	handler, e := service.New(o, d)
	if e != nil {
		return nil, e
	}
	s := kratoshttp.NewServer(kratoshttp.Address(o.Server.Address), kratoshttp.Timeout(0), kratoshttp.StrictSlash(false))
	s.Server.ReadHeaderTimeout = 5 * time.Second
	s.Server.IdleTimeout = 60 * time.Second
	s.Server.MaxHeaderBytes = 16 << 10
	// Raw routes do not inherit generated-service middleware: the handler itself
	// applies authentication, admission, deadlines, recovery and response headers.
	s.HandlePrefix("/", handler)
	return s, nil
}
