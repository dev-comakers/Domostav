-- Initial schema layout for the Domostav application.
-- Executed once by the postgres docker-entrypoint on first container start,
-- or manually via `psql -f` on production servers.

CREATE SCHEMA IF NOT EXISTS spp AUTHORIZATION domostav_app;
CREATE SCHEMA IF NOT EXISTS mzdovy AUTHORIZATION domostav_app;

ALTER ROLE domostav_app SET search_path TO spp, mzdovy, public;
