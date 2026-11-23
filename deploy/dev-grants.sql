-- DEVELOPMENT grant job, run as route_mind_migrator after every migration job.
-- In production substitute the reviewed schema/role and explicit table list.
GRANT USAGE ON SCHEMA public TO route_mind_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
  public.agent_runs, public.agent_steps, public.agent_tool_calls,
  public.request_idempotency TO route_mind_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO route_mind_app;
GRANT SELECT ON TABLE public.schema_migrations TO route_mind_app;
REVOKE INSERT, UPDATE, DELETE ON TABLE public.schema_migrations FROM route_mind_app;
