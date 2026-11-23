-- Development/test rollback only. This removes run data; production uses forward fixes.
BEGIN;
DROP TABLE request_idempotency;
DROP TABLE agent_tool_calls;
DROP TABLE agent_steps;
DROP TABLE agent_runs;
COMMIT;
