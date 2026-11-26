-- pg_bolt--0.1.1.sql
-- Schema and SQL entry points for the pg_bolt extension.

\echo Use "CREATE EXTENSION pg_bolt" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS "bolt";

CREATE FUNCTION "bolt"."load_table"(
    "pg_relation" text,
    "bolt_alias" text DEFAULT NULL
)
RETURNS TABLE (
    "bolt_table" text,
    "rows_cached" bigint,
    "columns_cached" integer,
    "memory_bytes" bigint
)
AS 'MODULE_PATHNAME', 'load_table_wrapper'
LANGUAGE c VOLATILE;

CREATE FUNCTION "bolt"."cache_table"(
    "pg_relation" text,
    "bolt_alias" text DEFAULT NULL
)
RETURNS TABLE (
    "bolt_table" text,
    "rows_cached" bigint,
    "columns_cached" integer,
    "memory_bytes" bigint
)
AS 'MODULE_PATHNAME', 'cache_table_wrapper'
LANGUAGE c VOLATILE;

CREATE FUNCTION "bolt"."evict_table"(
    "bolt_name" text
)
RETURNS boolean
AS 'MODULE_PATHNAME', 'evict_table_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."sync_table"(
    "pg_relation" text,
    "bolt_alias" text DEFAULT NULL
)
RETURNS TABLE (
    "bolt_table" text,
    "rows_cached" bigint,
    "columns_cached" integer,
    "memory_bytes" bigint
)
AS 'MODULE_PATHNAME', 'sync_table_wrapper'
LANGUAGE c VOLATILE;

CREATE FUNCTION "bolt"."tables"()
RETURNS TABLE (
    "bolt_table" text,
    "source_relation" text,
    "num_rows" bigint,
    "approx_bytes" bigint
)
AS 'MODULE_PATHNAME', 'tables_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."query"(
    "sql" text
)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME', 'query_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."query_json"(
    "sql" text
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'query_json_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."explain_tier"(
    "sql" text
)
RETURNS text
AS 'MODULE_PATHNAME', 'explain_tier_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."explain_sql"(
    "sql" text
)
RETURNS text
AS 'MODULE_PATHNAME', 'explain_sql_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."status"()
RETURNS jsonb
AS 'MODULE_PATHNAME', 'status_wrapper'
LANGUAGE c VOLATILE STRICT;

CREATE FUNCTION "bolt"."reset_vram"()
RETURNS text
AS 'MODULE_PATHNAME', 'reset_vram_wrapper'
LANGUAGE c VOLATILE STRICT;
