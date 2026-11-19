<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security Policy

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report suspected security vulnerabilities privately to **opensource@craton.com.ar** (Craton Software Company) with:
- Affected version or commit hash (`pg_bolt` and `craton-bolt`)
- PostgreSQL version, OS, and CUDA Toolkit version
- Minimal reproduction SQL or heap/IPC input
- Potential impact (memory corruption, privilege escalation, crash/DoS, data leak)

Maintainers aim to acknowledge receipt within 3 business days and coordinate disclosure once a fix is verified.

## Threat Model & Security Boundaries

- **Superuser Extension (`superuser = true`)**: `pg_bolt` is registered as a superuser extension in [`pg_bolt.control`](pg_bolt.control). Only trusted database administrators should execute `CREATE EXTENSION pg_bolt` or install `shared_preload_libraries = 'pg_bolt'`.
- **Unsafe PostgreSQL / CUDA FFI Boundaries**: `CustomScan` execution hooks, `planner_hook`, SPI extraction, and raw 8 KiB page parsing interact directly with PostgreSQL `pg_sys` structures and CUDA driver/NVRTC libraries.
- **SQL Identifier Quoting**: Relation names passed to `bolt.load_table` / `bolt.cache_table` / `bolt.sync_table` are sanitized through `pg_bolt::runtime::quote_qualified_identifier` before constructing SPI extraction SQL.
- **Dependency Audits**: `cargo-deny` advisory and source verification runs in CI using [`deny.toml`](deny.toml).

## Supported Versions

| Version | Supported | Notes |
| :--- | :--- | :--- |
| `0.1.x` / `main` | Pre-release active | PostgreSQL `15`–`18` (`pgrx` `0.19`) |
