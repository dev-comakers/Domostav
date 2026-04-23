-- Mzdovy prehled schema (module 2).
-- Idempotent: safe to run on every app start.

CREATE TABLE IF NOT EXISTS payroll_imports (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    period      TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payroll_import_files (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id     BIGINT NOT NULL,
    filename      TEXT NOT NULL,
    report_type   TEXT NOT NULL,
    company_name  TEXT,
    period        TEXT,
    parser_mode   TEXT NOT NULL,
    saved_path    TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payroll_parsed_rows (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id            BIGINT NOT NULL,
    import_file_id       BIGINT NOT NULL,
    report_type          TEXT NOT NULL,
    company_name         TEXT NOT NULL,
    period               TEXT NOT NULL,
    employee_name        TEXT NOT NULL,
    normalized_name      TEXT NOT NULL,
    person_code          TEXT,
    gross_wage           DOUBLE PRECISION DEFAULT 0,
    social_employee      DOUBLE PRECISION DEFAULT 0,
    social_employer      DOUBLE PRECISION DEFAULT 0,
    health_employee      DOUBLE PRECISION DEFAULT 0,
    health_employer      DOUBLE PRECISION DEFAULT 0,
    tax_amount           DOUBLE PRECISION DEFAULT 0,
    payout_amount        DOUBLE PRECISION DEFAULT 0,
    settlement_amount    DOUBLE PRECISION DEFAULT 0,
    srazky               DOUBLE PRECISION DEFAULT 0,
    zaloha               DOUBLE PRECISION DEFAULT 0,
    health_insurance_name TEXT,
    source_row_index     INTEGER NOT NULL,
    parser_mode          TEXT NOT NULL,
    raw_json             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payroll_employees (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name        TEXT NOT NULL,
    normalized_name  TEXT UNIQUE NOT NULL,
    project_name     TEXT,
    coordinator_name TEXT,
    company_code     TEXT,
    company_name     TEXT,
    notes            TEXT,
    odvody_strhavame DOUBLE PRECISION DEFAULT 0,
    mesicni_mzda     DOUBLE PRECISION DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payroll_preview_rows (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id             BIGINT NOT NULL,
    period                TEXT NOT NULL,
    display_name          TEXT NOT NULL,
    normalized_name       TEXT NOT NULL,
    company_name          TEXT NOT NULL,
    employee_id           BIGINT,
    project_name          TEXT,
    coordinator_name      TEXT,
    company_code          TEXT,
    gross_wage            DOUBLE PRECISION DEFAULT 0,
    social_employee       DOUBLE PRECISION DEFAULT 0,
    social_employer       DOUBLE PRECISION DEFAULT 0,
    health_employee       DOUBLE PRECISION DEFAULT 0,
    health_employer       DOUBLE PRECISION DEFAULT 0,
    tax_amount            DOUBLE PRECISION DEFAULT 0,
    odvody_platime        DOUBLE PRECISION DEFAULT 0,
    odvody_strhavame      DOUBLE PRECISION DEFAULT 0,
    mesicni_mzda          DOUBLE PRECISION DEFAULT 0,
    control_sum_parsed    DOUBLE PRECISION DEFAULT 0,
    control_sum_expected  DOUBLE PRECISION DEFAULT 0,
    match_status          TEXT NOT NULL,
    warnings_json         TEXT NOT NULL,
    source_files_json     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payroll_export_runs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id   BIGINT NOT NULL,
    output_path TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payroll_employee_change_log (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_id  BIGINT NOT NULL,
    action       TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
