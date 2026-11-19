<!-- SPDX-License-Identifier: Apache-2.0 -->

# Maintainers

`pg_bolt` is developed and maintained by **Craton Software Company** ([https://craton.com.ar](https://craton.com.ar)).

## Current Maintainers

| Role | Maintainer | Contact |
| :--- | :--- | :--- |
| Lead Maintainer | `@victor-craton` | `opensource@craton.com.ar` |

Default code ownership across the repository is declared in [`CODEOWNERS`](CODEOWNERS).

## Governance

- Pull requests require review and CI green status across pure-Rust `cuda-stub`, `clippy -D warnings`, PostgreSQL `15`–`18` `cargo pgrx test` checks, and `cargo-deny` audits.
- All commits must be signed off under the [Developer Certificate of Origin 1.1](DCO).
