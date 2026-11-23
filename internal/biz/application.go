// Package biz applies caller policy before delegating to the reusable framework.
package biz

import (
	"context"
	"slices"

	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

type Application struct {
	Catalog *llm.Catalog
	Gateway *llm.Gateway
	Engine  *agent.Engine
	Tools   *agent.ToolRegistry
	Store   agent.RunStore
	Policy  config.Agent
	Ready   func(context.Context) error
}

func restricted(base, requested []string) ([]string, error) {
	if len(requested) == 0 {
		return append([]string(nil), base...), nil
	}
	out := []string{}
	for _, s := range requested {
		if !slices.Contains(base, s) {
			return nil, llm.Failure("permission_denied", "requested routing policy is not permitted", 403)
		}
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out, nil
}
func (a *Application) Routing(p config.Principal, models, providers []string) ([]string, []string, error) {
	base := []string{}
	for _, id := range a.Policy.Models {
		if slices.Contains(p.Models, id) {
			base = append(base, id)
		}
	}
	ps := []string{}
	for _, id := range a.Policy.Providers {
		if slices.Contains(p.Providers, id) {
			ps = append(ps, id)
		}
	}
	ms, e := restricted(base, models)
	if e != nil {
		return nil, nil, e
	}
	ps, e = restricted(ps, providers)
	if e != nil {
		return nil, nil, e
	}
	if len(ms) == 0 || len(ps) == 0 {
		return nil, nil, llm.Failure("permission_denied", "no permitted agent route", 403)
	}
	return ms, ps, nil
}
func (a *Application) NewRun(p config.Principal, input llm.Request, tools []agent.ToolSpec, models, providers []string, limits agent.Limits, compat bool) (*agent.Run, error) {
	if !p.AllowAgent {
		return nil, llm.Failure("permission_denied", "agent execution is not permitted", 403)
	}
	ms, ps, e := a.Routing(p, models, providers)
	if e != nil {
		return nil, e
	}
	r, e := agent.NewRun(input, agent.RunOptions{Owner: p.ID, Tools: tools, AllowedModels: ms, AllowedProviders: ps, Limits: limits, Compatibility: compat})
	if e != nil {
		return nil, e
	}
	if e = a.Engine.ValidateRun(r); e != nil {
		return nil, e
	}
	return r, nil
}
func (a *Application) Execute(ctx context.Context, p config.Principal, r llm.Request, sink llm.Sink) (llm.Response, error) {
	if r.Model == "auto" {
		run, e := a.NewRun(p, r, nil, nil, nil, a.Policy.Limits, true)
		if e != nil {
			return llm.Response{}, e
		}
		if e = a.Engine.Advance(ctx, run, sink, nil); e != nil {
			return llm.Response{}, e
		}
		if run.Output == nil {
			return llm.Response{}, llm.Failure("internal_error", "run produced no public response", 500)
		}
		out := *run.Output
		out.Model = "auto"
		return out, nil
	}
	m, ok := a.Catalog.Get(r.Model)
	if !ok {
		return llm.Response{}, llm.Failure("model_not_found", "model is not registered", 404)
	}
	if !slices.Contains(p.Models, r.Model) || !slices.Contains(p.Providers, m.Provider) {
		return llm.Response{}, llm.Failure("permission_denied", "model is not permitted", 403)
	}
	if sink != nil {
		return a.Gateway.Stream(ctx, r, sink)
	}
	return a.Gateway.Generate(ctx, r)
}
func (a *Application) Revalidate(p config.Principal, r *agent.Run) error {
	if !p.AllowAgent {
		return llm.Failure("permission_denied", "agent execution is not permitted", 403)
	}
	if _, _, e := a.Routing(p, r.AllowedModels, r.AllowedProviders); e != nil {
		return e
	}
	for _, t := range r.Tools {
		if t.Executor == "server" && !slices.Contains(p.ServerTools, t.Definition.Name) {
			return llm.Failure("permission_denied", "server tool permission changed", 403)
		}
	}
	return a.Engine.ValidateRun(r)
}
