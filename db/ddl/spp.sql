-- SPP module schema (module 1: Kontrola odpisu podle SPP).
-- Idempotent: safe to run on every app start.

CREATE TABLE IF NOT EXISTS projects (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code         TEXT UNIQUE NOT NULL,
    name         TEXT NOT NULL,
    prompt       TEXT DEFAULT '',
    created_at   TEXT NOT NULL
);

ALTER TABLE projects ADD COLUMN IF NOT EXISTS archived_at TEXT;

CREATE TABLE IF NOT EXISTS sessions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_code    TEXT NOT NULL,
    period_month    TEXT NOT NULL,
    spp_path        TEXT NOT NULL,
    inventory_path  TEXT NOT NULL,
    nf45_path       TEXT,
    output_path     TEXT NOT NULL,
    stats_json      TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mappings (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_code  TEXT NOT NULL,
    file_type     TEXT NOT NULL,
    mapping_json  TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    UNIQUE(project_code, file_type)
);

CREATE TABLE IF NOT EXISTS overrides (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_code   TEXT NOT NULL,
    article        TEXT NOT NULL,
    spp_rows_json  TEXT NOT NULL,
    reason         TEXT DEFAULT '',
    updated_at     TEXT NOT NULL,
    UNIQUE(project_code, article)
);

CREATE TABLE IF NOT EXISTS scoped_overrides (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope_type     TEXT NOT NULL,
    scope_value    TEXT NOT NULL,
    item_key       TEXT NOT NULL,
    spp_rows_json  TEXT NOT NULL,
    reason         TEXT DEFAULT '',
    updated_at     TEXT NOT NULL,
    UNIQUE(scope_type, scope_value, item_key)
);

CREATE TABLE IF NOT EXISTS analysis_drafts (
    id                     TEXT PRIMARY KEY,
    project_code           TEXT NOT NULL,
    project_name           TEXT NOT NULL,
    period_month           TEXT NOT NULL,
    spp_path               TEXT NOT NULL,
    inventory_path         TEXT NOT NULL,
    nf45_path              TEXT,
    rules_path             TEXT,
    nomenclature_path      TEXT,
    project_prompt         TEXT DEFAULT '',
    spp_mapping_json       TEXT,
    inventory_mapping_json TEXT,
    created_at             TEXT NOT NULL,
    updated_at             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rules_registry (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule_type        TEXT NOT NULL,
    scope_type       TEXT NOT NULL,
    scope_value      TEXT NOT NULL,
    rule_key         TEXT NOT NULL,
    rule_value_json  TEXT NOT NULL,
    reason           TEXT DEFAULT '',
    priority         INTEGER NOT NULL DEFAULT 100,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    UNIQUE(rule_type, scope_type, scope_value, rule_key)
);

CREATE TABLE IF NOT EXISTS rules_versions (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_code  TEXT NOT NULL,
    label         TEXT NOT NULL,
    rules_json    TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
