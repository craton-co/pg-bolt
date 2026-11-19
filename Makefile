# SPDX-License-Identifier: Apache-2.0
#
# PGXN / PGXS-compatible Makefile wrapper around cargo-pgrx.

PG_CONFIG ?= pg_config
CARGO     ?= cargo
PG_MAJOR  := $(shell $(PG_CONFIG) --version | sed -E 's/^PostgreSQL ([0-9]+).*/\1/')
PKGLIBDIR := $(shell $(PG_CONFIG) --pkglibdir)
SHAREDIR  := $(shell $(PG_CONFIG) --sharedir)
FEATURES  ?= pg$(PG_MAJOR) cuda

PKG_OUT := target/release/pg_bolt-pg$(PG_MAJOR)

.PHONY: all install uninstall clean

all:
	@if [ ! -d ../bolt ]; then \
		sed -i.bak 's|path = "../bolt", ||g' Cargo.toml && rm -f Cargo.toml.bak; \
	fi
	$(CARGO) pgrx package --pg-config "$(PG_CONFIG)" --no-default-features --features "$(FEATURES)"

install: all
	install -d "$(DESTDIR)$(PKGLIBDIR)" "$(DESTDIR)$(SHAREDIR)/extension"
	install -m 755 "$(PKG_OUT)$(PKGLIBDIR)/pg_bolt.so" "$(DESTDIR)$(PKGLIBDIR)/pg_bolt.so"
	install -m 644 "$(PKG_OUT)$(SHAREDIR)/extension/pg_bolt.control" "$(DESTDIR)$(SHAREDIR)/extension/pg_bolt.control"
	install -m 644 "$(PKG_OUT)$(SHAREDIR)/extension/"pg_bolt--*.sql "$(DESTDIR)$(SHAREDIR)/extension/"

uninstall:
	rm -f "$(DESTDIR)$(PKGLIBDIR)/pg_bolt.so"
	rm -f "$(DESTDIR)$(SHAREDIR)/extension/pg_bolt.control"
	rm -f "$(DESTDIR)$(SHAREDIR)/extension/"pg_bolt--*.sql

clean:
	$(CARGO) clean
