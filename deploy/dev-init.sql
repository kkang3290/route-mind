-- Disposable DEVELOPMENT database only, run by the official postgres entrypoint.
-- psql reads secrets from the environment, not SQL literals or CLI arguments.
\getenv app_password POSTGRES_APP_PASSWORD
\getenv migration_password POSTGRES_MIGRATION_PASSWORD
CREATE ROLE route_mind_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'app_password';
CREATE ROLE route_mind_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'migration_password';
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO route_mind_migrator;
GRANT USAGE ON SCHEMA public TO route_mind_app;
