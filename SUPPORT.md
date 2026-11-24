<!-- SPDX-License-Identifier: Apache-2.0 -->

# Support

## Community Support

- **Bug reports & feature requests**: Open an issue on [GitHub Issues](https://github.com/craton-co/pg-bolt/issues) including:
  - `pg_bolt` commit SHA and `craton-bolt` version (`0.12.0`)
  - PostgreSQL major version (`15`–`18`) and `pgrx` build features (`cuda` vs `cuda-stub`)
  - GPU model, NVIDIA driver version, and CUDA Toolkit version (or CPU-only for `cuda-stub`)
  - `SELECT bolt.status();` output and relevant PostgreSQL server log lines
- **Documentation**: Start with [docs/INSTALL.md](docs/INSTALL.md), [docs/CONFIGURATION.md](docs/CONFIGURATION.md), and [docs/SQL_API.md](docs/SQL_API.md).
- **Security issues**: Email **opensource@craton.com.ar** directly instead of filing a public issue (see [SECURITY.md](SECURITY.md)).

## Commercial Licensing & Enterprise Support

While `pg_bolt` source code is licensed under [Apache-2.0](LICENSE), compiled extension binaries link `craton-bolt` (`AGPL-3.0-only`). Organizations seeking commercial licensing or dedicated enterprise support can contact **Craton Software Company** via [https://craton.com.ar](https://craton.com.ar) or **opensource@craton.com.ar**.
