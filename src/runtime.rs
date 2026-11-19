// SPDX-License-Identifier: Apache-2.0

//! Per-process / per-backend `craton_bolt::Engine` lifecycle, fork-safety guard,
//! LRU VRAM eviction, SQL identifier validation, and `cuda-stub` plan fidelity.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::LazyLock;
#[cfg(all(feature = "pgrx", not(feature = "cuda-stub")))]
use std::sync::{Condvar, Mutex as StdMutex};
#[cfg(not(feature = "cuda-stub"))]
use std::time::Duration;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use craton_bolt::jit::ptx_cache_stats;
use craton_bolt::plan::optimizer::run_to_fixpoint;
use craton_bolt::plan::{
    default_passes, lower_physical, parse_sql, DataType as BoltDataType, Field as BoltField,
    MemTableProvider, PhysicalPlan, Schema as BoltSchema, TableProvider,
};
#[cfg(feature = "cuda-stub")]
use craton_bolt::plan::{format_logical, format_physical};
#[cfg(not(feature = "cuda-stub"))]
use craton_bolt::CancellationToken;
use craton_bolt::{pool_stats, Engine, ExecutionTier, PoolStats, SqlSemantics};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlparser::ast::{
    visit_relations_mut, Expr, Ident, JoinConstraint, JoinOperator, ObjectName, Query, Select,
    SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{PgBoltError, PgBoltResult};
use crate::guc::{current_config, BoltConfig};

static PROCESS_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Monotonic catalog invalidation epoch counter for cross-backend catalog sync (W1-D-005).
static RUNTIME_CATALOG_EPOCH: AtomicU64 = AtomicU64::new(1);

/// Returns the current monotonic catalog invalidation epoch.
#[must_use]
pub fn current_catalog_epoch() -> u64 {
    RUNTIME_CATALOG_EPOCH.load(Ordering::Acquire)
}

/// Advances the monotonic catalog invalidation epoch and returns the updated value.
pub fn bump_catalog_epoch() -> u64 {
    RUNTIME_CATALOG_EPOCH.fetch_add(1, Ordering::AcqRel) + 1
}

/// Acquires the process-wide test serialization lock.
///
/// `craton_bolt::Engine` permits only one active engine instance per OS process;
/// parallel unit/integration tests that initialize an `Engine` (or share process-wide
/// Bolt pools/caches) should hold this guard for their duration.
pub fn test_process_lock() -> MutexGuard<'static, ()> {
    PROCESS_LOCK.lock()
}

/// Parses a PostgreSQL qualified identifier (`table`, `schema.table`, or `catalog.schema.table`)
/// into `(unescaped_segment, was_quoted)` pairs using PostgreSQL delimiter rules.
pub(crate) fn parse_qualified_segments(ident: &str) -> PgBoltResult<Vec<(String, bool)>> {
    if ident.contains('\0') {
        return Err(PgBoltError::Validation(
            "identifier must not contain NUL bytes".into(),
        ));
    }
    let trimmed = ident.trim();
    if trimmed.is_empty() {
        return Err(PgBoltError::Validation(
            "identifier must not be empty".into(),
        ));
    }

    let bytes = trimmed.as_bytes();
    let mut segments: Vec<(String, bool)> = Vec::with_capacity(3);
    let mut idx = 0usize;

    loop {
        if idx >= bytes.len() {
            return Err(PgBoltError::Validation(format!(
                "qualified identifier `{ident}` contains an empty segment"
            )));
        }
        if segments.len() >= 3 {
            return Err(PgBoltError::Validation(format!(
                "qualified identifier `{ident}` has more than 3 dot-separated segments"
            )));
        }

        if bytes[idx] == b'"' {
            idx += 1;
            let mut unescaped = String::new();
            let mut closed = false;
            while idx < bytes.len() {
                if bytes[idx] == b'"' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                        unescaped.push('"');
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    closed = true;
                    break;
                }
                let ch = trimmed[idx..].chars().next().ok_or_else(|| {
                    PgBoltError::Validation(format!(
                        "qualified identifier `{ident}` contains invalid UTF-8"
                    ))
                })?;
                unescaped.push(ch);
                idx += ch.len_utf8();
            }
            if !closed {
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` has an unterminated quoted segment"
                )));
            }
            if unescaped.is_empty() {
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` contains an empty segment"
                )));
            }
            if unescaped.chars().any(char::is_control) {
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` contains control characters"
                )));
            }
            segments.push((unescaped, true));
            if idx < bytes.len() {
                if bytes[idx] == b'.' {
                    idx += 1;
                    continue;
                }
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` has unexpected whitespace or trailing characters after quoted segment"
                )));
            }
            break;
        } else {
            let start = idx;
            while idx < bytes.len() && bytes[idx] != b'.' {
                idx += 1;
            }
            let part = &trimmed[start..idx];
            if part.is_empty() {
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` contains an empty segment"
                )));
            }
            if part != part.trim() {
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` has unexpected whitespace around `.`"
                )));
            }
            if part.chars().any(char::is_control) {
                return Err(PgBoltError::Validation(format!(
                    "qualified identifier `{ident}` contains control characters"
                )));
            }
            segments.push((part.to_string(), false));
            if idx < bytes.len() {
                idx += 1;
                continue;
            }
            break;
        }
    }

    Ok(segments)
}

/// Validates and safely double-quotes a PostgreSQL identifier (`table`, `schema.table`,
/// or `catalog.schema.table`) to prevent SQL injection in SPI extraction queries.
pub fn quote_qualified_identifier(ident: &str) -> PgBoltResult<String> {
    let segments = parse_qualified_segments(ident)?;
    let quoted: Vec<String> = segments
        .into_iter()
        .map(|(part, _)| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect();
    Ok(quoted.join("."))
}

/// Metadata tracked for each table pinned in the active Bolt runtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedTableMeta {
    pub pg_relation: String,
    pub bolt_name: String,
    pub num_rows: usize,
    pub num_cols: usize,
    pub approx_bytes: usize,
    pub loaded_at_ms: u128,
    pub last_accessed_seq: u64,
    pub dirty: bool,
}

/// Result of a Bolt SQL query execution.
#[derive(Debug)]
pub struct BoltExecutionResult {
    pub batch: RecordBatch,
    pub elapsed_us: u128,
    pub tier: ExecutionTier,
}

/// Transaction or subtransaction checkpoint frame tracking uncommitted table mutations (W1-A-007).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionTxFrame {
    /// Unique identifier for this subtransaction level (e.g. `pg_sys::SubTransactionId` or sequence).
    pub subid: u64,
    /// Cached tables newly registered or replaced in this frame that must be evicted on abort.
    pub added_tables: Vec<String>,
    /// Tables dirtied during this frame alongside their prior dirty status (`(bolt_name, was_dirty)`).
    pub dirtied_tables: Vec<(String, bool)>,
    /// Tables marked for eviction upon successful top-level transaction commit (e.g. `DROP TABLE`).
    pub pending_evict_on_commit: HashSet<String>,
}

/// Serde serialization adapter for `craton_bolt::PoolStats`.
///
/// **Upstream Ergonomics Note (W1-A-005)**:
/// In `craton-bolt` 0.12.0, `PoolStats` does not derive `serde::Serialize` / `serde::Deserialize`
/// directly, requiring downstream callers to proxy through a representative struct.
/// This module provides a lossless serialization bridge for `BoltRuntimeStatus`.
/// When upstream PR for W1-A-005 merges optional serde derives into `craton-bolt`,
/// this proxy can be replaced by direct `#[derive(Serialize, Deserialize)]`.
mod pool_stats_serde {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct PoolStatsRepr {
        total_pooled_bytes: usize,
        bucket_count: usize,
        oom_recovery_count: u64,
        proactive_eviction_count: u64,
    }

    pub fn serialize<S: Serializer>(stats: &PoolStats, serializer: S) -> Result<S::Ok, S::Error> {
        PoolStatsRepr {
            total_pooled_bytes: stats.total_pooled_bytes,
            bucket_count: stats.bucket_count,
            oom_recovery_count: stats.oom_recovery_count,
            proactive_eviction_count: stats.proactive_eviction_count,
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PoolStats, D::Error> {
        let repr = PoolStatsRepr::deserialize(deserializer)?;
        Ok(PoolStats {
            total_pooled_bytes: repr.total_pooled_bytes,
            bucket_count: repr.bucket_count,
            oom_recovery_count: repr.oom_recovery_count,
            proactive_eviction_count: repr.proactive_eviction_count,
        })
    }
}

/// Snapshot of runtime telemetry, VRAM cache usage, and PTX JIT / pool statistics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoltRuntimeStatus {
    pub cuda_stub: bool,
    pub owner_pid: u32,
    pub cached_tables: usize,
    pub total_cached_bytes: usize,
    pub max_vram_bytes: Option<usize>,
    pub ptx_cache_hits: usize,
    pub ptx_cache_misses: usize,
    pub ptx_cache_evictions: usize,
    #[serde(with = "pool_stats_serde")]
    pub pool_stats: PoolStats,
    pub queries_executed: u64,
    pub eviction_count: u64,
}

/// `MemTableProvider` wrapper exposing PostgreSQL-compatible `SqlSemantics::Standard`.
///
/// **Upstream Ergonomics Note (W1-A-005)**:
/// `MemTableProvider::set_sql_semantics` is `pub(crate)` in `craton-bolt` 0.12.0 (`sql_frontend.rs`),
/// preventing external crates from setting `SqlSemantics::Standard` on a standalone `MemTableProvider`.
/// `StandardTableProvider` delegates all table and schema queries to the inner `MemTableProvider`
/// while explicitly overriding `TableProvider::sql_semantics` to return `SqlSemantics::Standard`.
/// Once `craton-bolt` exposes public builder methods (`with_sql_semantics`), this wrapper can be deleted.
#[derive(Debug)]
struct StandardTableProvider(MemTableProvider);

impl StandardTableProvider {
    fn new() -> Self {
        Self(MemTableProvider::new())
    }

    fn inner_mut(&mut self) -> &mut MemTableProvider {
        &mut self.0
    }
}

impl TableProvider for StandardTableProvider {
    fn schema(&self, table_name: &str) -> craton_bolt::BoltResult<BoltSchema> {
        self.0.schema(table_name)
    }

    fn has_nulls(&self, table_name: &str, col_idx: usize) -> bool {
        <MemTableProvider as TableProvider>::has_nulls(&self.0, table_name, col_idx)
    }

    fn null_count(&self, table_name: &str, col_idx: usize) -> Option<usize> {
        <MemTableProvider as TableProvider>::null_count(&self.0, table_name, col_idx)
    }

    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }

    fn sql_semantics(&self) -> SqlSemantics {
        SqlSemantics::Standard
    }
}

fn arrow_schema_to_bolt(schema: &SchemaRef) -> PgBoltResult<BoltSchema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let dtype = BoltDataType::from_arrow(field.data_type())?;
        fields.push(BoltField::new(field.name(), dtype, field.is_nullable()));
    }
    Ok(BoltSchema::new(fields))
}

/// Normalized SQL identifier folding matching `craton-bolt` rules:
/// quoted identifiers preserve case verbatim; unquoted fold to ASCII lowercase.
#[inline]
fn sql_ident_to_name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

#[inline]
fn make_sql_ident(name: &str) -> Ident {
    Ident::with_quote('"', name)
}

fn canonical_bolt_segments(bolt_name: &str) -> Vec<String> {
    if let Ok(segs) = parse_qualified_segments(bolt_name) {
        segs.into_iter()
            .map(|(val, quoted)| {
                if quoted {
                    val
                } else {
                    val.to_ascii_lowercase()
                }
            })
            .collect()
    } else {
        vec![bolt_name.trim().to_string()]
    }
}

fn exact_bolt_segments(bolt_name: &str) -> Vec<String> {
    if let Ok(segs) = parse_qualified_segments(bolt_name) {
        segs.into_iter().map(|(val, _)| val).collect()
    } else {
        vec![bolt_name.trim().to_string()]
    }
}

fn sanitize_engine_ident_part(part: &str) -> String {
    let mut out = String::with_capacity(part.len());
    for ch in part.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "tbl".to_string()
    } else if trimmed.as_bytes()[0].is_ascii_digit() {
        format!("t_{trimmed}")
    } else {
        trimmed.to_string()
    }
}

fn is_simple_ascii_ident(s: &str) -> bool {
    let bytes = s.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

fn resolve_table_from_map(
    name: &ObjectName,
    bolt_to_engine: &HashMap<String, String>,
) -> Option<(String, String)> {
    if bolt_to_engine.is_empty() || name.0.is_empty() {
        return None;
    }

    let q_segs: Vec<String> = name.0.iter().map(sql_ident_to_name).collect();
    let mut exact_matches: Vec<(&String, &String)> = bolt_to_engine
        .iter()
        .filter(|(bolt_name, _)| canonical_bolt_segments(bolt_name) == q_segs)
        .collect();
    if exact_matches.len() == 1 {
        return Some((exact_matches[0].0.clone(), exact_matches[0].1.clone()));
    }
    if exact_matches.len() > 1 {
        exact_matches.sort_by(|a, b| a.0.cmp(b.0));
        return Some((exact_matches[0].0.clone(), exact_matches[0].1.clone()));
    }

    // Match verbatim alias casing (e.g. `FROM "MyAlias"` when `bolt_name == "MyAlias"`).
    let mut raw_matches: Vec<(&String, &String)> = bolt_to_engine
        .iter()
        .filter(|(bolt_name, _)| exact_bolt_segments(bolt_name) == q_segs)
        .collect();
    if raw_matches.len() == 1 {
        return Some((raw_matches[0].0.clone(), raw_matches[0].1.clone()));
    }
    if raw_matches.len() > 1 {
        raw_matches.sort_by(|a, b| a.0.cmp(b.0));
        return Some((raw_matches[0].0.clone(), raw_matches[0].1.clone()));
    }

    // Unique case-insensitive fallback across full qualified segments.
    let ci_matches: Vec<(&String, &String)> = bolt_to_engine
        .iter()
        .filter(|(bolt_name, _)| {
            let c_segs = canonical_bolt_segments(bolt_name);
            c_segs.len() == q_segs.len()
                && c_segs
                    .iter()
                    .zip(&q_segs)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
        })
        .collect();
    if ci_matches.len() == 1 {
        return Some((ci_matches[0].0.clone(), ci_matches[0].1.clone()));
    }

    // Bare `table` reference can resolve a cached `public.<table>` when no exact match exists.
    if q_segs.len() == 1 {
        let public_segs = ["public", q_segs[0].as_str()];
        let pub_exact: Vec<(&String, &String)> = bolt_to_engine
            .iter()
            .filter(|(bolt_name, _)| {
                let c_segs = canonical_bolt_segments(bolt_name);
                c_segs.len() == 2 && c_segs[0] == public_segs[0] && c_segs[1] == public_segs[1]
            })
            .collect();
        if pub_exact.len() == 1 {
            return Some((pub_exact[0].0.clone(), pub_exact[0].1.clone()));
        }
        let pub_ci: Vec<(&String, &String)> = bolt_to_engine
            .iter()
            .filter(|(bolt_name, _)| {
                let c_segs = canonical_bolt_segments(bolt_name);
                c_segs.len() == 2
                    && c_segs[0].eq_ignore_ascii_case("public")
                    && c_segs[1].eq_ignore_ascii_case(&q_segs[0])
            })
            .collect();
        if pub_ci.len() == 1 {
            return Some((pub_ci[0].0.clone(), pub_ci[0].1.clone()));
        }
    }

    None
}

#[inline]
fn is_exact_single_ident(name: &ObjectName, engine_ident: &str) -> bool {
    name.0.len() == 1 && sql_ident_to_name(&name.0[0]) == engine_ident
}

struct SqlRewriteContext<'a> {
    bolt_to_engine: &'a HashMap<String, String>,
    rewritten: bool,
    refs: Vec<String>,
    seen_refs: HashSet<String>,
    next_subq_id: usize,
}

impl<'a> SqlRewriteContext<'a> {
    fn new(bolt_to_engine: &'a HashMap<String, String>) -> Self {
        Self {
            bolt_to_engine,
            rewritten: false,
            refs: Vec::new(),
            seen_refs: HashSet::new(),
            next_subq_id: 0,
        }
    }

    fn record_ref(&mut self, bolt_name: String) {
        if self.seen_refs.insert(bolt_name.clone()) {
            self.refs.push(bolt_name);
        }
    }

    fn alloc_sentinel(&mut self) -> String {
        self.next_subq_id = self.next_subq_id.saturating_add(1);
        format!("__PGB_SUBQ_{}__", self.next_subq_id)
    }

    fn rewrite_statement(&mut self, stmt: &mut Statement) {
        match stmt {
            Statement::Query(query) => self.rewrite_query(query, &HashSet::new()),
            Statement::Explain { statement, .. } => self.rewrite_statement(statement),
            other => {
                let _ = visit_relations_mut(other, |name| {
                    if let Some((bolt_name, engine_ident)) =
                        resolve_table_from_map(name, self.bolt_to_engine)
                    {
                        self.record_ref(bolt_name);
                        if !is_exact_single_ident(name, &engine_ident) {
                            name.0 = vec![make_sql_ident(&engine_ident)];
                            self.rewritten = true;
                        }
                    }
                    ControlFlow::<()>::Continue(())
                });
            }
        }
    }

    fn rewrite_query(&mut self, query: &mut Query, outer_ctes: &HashSet<String>) {
        let mut ctes = outer_ctes.clone();
        if let Some(with) = &mut query.with {
            if with.recursive {
                for cte in &with.cte_tables {
                    ctes.insert(sql_ident_to_name(&cte.alias.name));
                }
                for cte in &mut with.cte_tables {
                    self.rewrite_query(&mut cte.query, &ctes);
                }
            } else {
                for cte in &mut with.cte_tables {
                    self.rewrite_query(&mut cte.query, &ctes);
                    ctes.insert(sql_ident_to_name(&cte.alias.name));
                }
            }
        }

        let body_quals = self.rewrite_set_expr(&mut query.body, &ctes);

        let empty_quals: Vec<(Vec<String>, String)> = Vec::new();
        if let Some(order_by) = &mut query.order_by {
            self.rewrite_node_exprs(order_by, &ctes, &body_quals);
        }
        if let Some(limit) = &mut query.limit {
            self.rewrite_node_exprs(limit, &ctes, &empty_quals);
        }
        if !query.limit_by.is_empty() {
            self.rewrite_node_exprs(&mut query.limit_by, &ctes, &body_quals);
        }
        if let Some(offset) = &mut query.offset {
            self.rewrite_node_exprs(offset, &ctes, &empty_quals);
        }
        if let Some(fetch) = &mut query.fetch {
            self.rewrite_node_exprs(fetch, &ctes, &empty_quals);
        }
    }

    fn rewrite_set_expr(
        &mut self,
        set_expr: &mut SetExpr,
        ctes: &HashSet<String>,
    ) -> Vec<(Vec<String>, String)> {
        match set_expr {
            SetExpr::Select(select) => self.rewrite_select(select, ctes),
            SetExpr::Query(inner) => {
                self.rewrite_query(inner, ctes);
                Vec::new()
            }
            SetExpr::SetOperation { left, right, .. } => {
                let _ = self.rewrite_set_expr(left, ctes);
                let _ = self.rewrite_set_expr(right, ctes);
                Vec::new()
            }
            SetExpr::Values(values) => {
                let empty_quals: Vec<(Vec<String>, String)> = Vec::new();
                self.rewrite_node_exprs(values, ctes, &empty_quals);
                Vec::new()
            }
            SetExpr::Table(table) => {
                if let Some(tname) = table.table_name.as_deref() {
                    let mut idents = Vec::new();
                    if let Some(schema) = table.schema_name.as_deref() {
                        idents.push(Ident::with_quote('"', schema));
                    }
                    idents.push(Ident::with_quote('"', tname));
                    let obj = ObjectName(idents);
                    if obj.0.len() == 1 && ctes.contains(&sql_ident_to_name(&obj.0[0])) {
                        return Vec::new();
                    }
                    if let Some((bolt_name, engine_ident)) =
                        resolve_table_from_map(&obj, self.bolt_to_engine)
                    {
                        self.record_ref(bolt_name);
                        if table.schema_name.is_some() || tname != engine_ident {
                            table.schema_name = None;
                            table.table_name = Some(engine_ident);
                            self.rewritten = true;
                        }
                    }
                }
                Vec::new()
            }
            SetExpr::Insert(_) | SetExpr::Update(_) => Vec::new(),
        }
    }

    fn count_table_tails(
        &self,
        tf: &TableFactor,
        ctes: &HashSet<String>,
        counts: &mut HashMap<String, usize>,
    ) {
        match tf {
            TableFactor::Table {
                name,
                alias: None,
                args: None,
                ..
            } => {
                if name.0.len() == 1 && ctes.contains(&sql_ident_to_name(&name.0[0])) {
                    return;
                }
                if resolve_table_from_map(name, self.bolt_to_engine).is_some() {
                    if let Some(tail) = name.0.last() {
                        *counts.entry(sql_ident_to_name(tail)).or_insert(0) += 1;
                    }
                }
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.count_twj_tails(table_with_joins, ctes, counts);
            }
            TableFactor::Pivot { table, .. }
            | TableFactor::Unpivot { table, .. }
            | TableFactor::MatchRecognize { table, .. } => {
                self.count_table_tails(table, ctes, counts);
            }
            _ => {}
        }
    }

    fn count_twj_tails(
        &self,
        twj: &TableWithJoins,
        ctes: &HashSet<String>,
        counts: &mut HashMap<String, usize>,
    ) {
        self.count_table_tails(&twj.relation, ctes, counts);
        for join in &twj.joins {
            self.count_table_tails(&join.relation, ctes, counts);
        }
    }

    fn rewrite_table_factor(
        &mut self,
        tf: &mut TableFactor,
        ctes: &HashSet<String>,
        tail_counts: &HashMap<String, usize>,
        qual_rewrites: &mut Vec<(Vec<String>, String)>,
    ) {
        match tf {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                if name.0.len() == 1 && ctes.contains(&sql_ident_to_name(&name.0[0])) {
                    return;
                }
                if let Some((bolt_name, engine_ident)) =
                    resolve_table_from_map(name, self.bolt_to_engine)
                {
                    self.record_ref(bolt_name);
                    let orig_segs: Vec<String> = name.0.iter().map(sql_ident_to_name).collect();
                    let orig_tail = orig_segs.last().cloned().unwrap_or_default();
                    let dup_tail = tail_counts.get(&orig_tail).copied().unwrap_or(0) > 1;

                    if !is_exact_single_ident(name, &engine_ident) {
                        name.0 = vec![make_sql_ident(&engine_ident)];
                        self.rewritten = true;
                    }

                    if alias.is_none() {
                        let desired_qual = if dup_tail {
                            engine_ident.clone()
                        } else {
                            orig_tail.clone()
                        };
                        if desired_qual != engine_ident {
                            *alias = Some(TableAlias {
                                name: make_sql_ident(&desired_qual),
                                columns: Vec::new(),
                            });
                            self.rewritten = true;
                        }
                        if orig_segs.len() >= 2 {
                            qual_rewrites.push((orig_segs, desired_qual));
                        }
                    }
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.rewrite_query(subquery, ctes);
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.rewrite_twj(table_with_joins, ctes, tail_counts, qual_rewrites);
            }
            TableFactor::Pivot {
                table,
                aggregate_functions,
                default_on_null,
                ..
            } => {
                self.rewrite_table_factor(table, ctes, tail_counts, qual_rewrites);
                self.rewrite_node_exprs(aggregate_functions, ctes, qual_rewrites);
                if let Some(expr) = default_on_null {
                    self.rewrite_node_exprs(expr, ctes, qual_rewrites);
                }
            }
            TableFactor::Unpivot { table, .. } => {
                self.rewrite_table_factor(table, ctes, tail_counts, qual_rewrites);
            }
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                symbols,
                ..
            } => {
                self.rewrite_table_factor(table, ctes, tail_counts, qual_rewrites);
                self.rewrite_node_exprs(partition_by, ctes, qual_rewrites);
                self.rewrite_node_exprs(order_by, ctes, qual_rewrites);
                self.rewrite_node_exprs(measures, ctes, qual_rewrites);
                self.rewrite_node_exprs(symbols, ctes, qual_rewrites);
            }
            other => {
                self.rewrite_node_exprs(other, ctes, qual_rewrites);
            }
        }
    }

    fn rewrite_twj(
        &mut self,
        twj: &mut TableWithJoins,
        ctes: &HashSet<String>,
        tail_counts: &HashMap<String, usize>,
        qual_rewrites: &mut Vec<(Vec<String>, String)>,
    ) {
        self.rewrite_table_factor(&mut twj.relation, ctes, tail_counts, qual_rewrites);
        for join in &mut twj.joins {
            self.rewrite_table_factor(&mut join.relation, ctes, tail_counts, qual_rewrites);
        }
        for join in &mut twj.joins {
            match &mut join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr))
                | JoinOperator::LeftOuter(JoinConstraint::On(expr))
                | JoinOperator::RightOuter(JoinConstraint::On(expr))
                | JoinOperator::FullOuter(JoinConstraint::On(expr))
                | JoinOperator::LeftSemi(JoinConstraint::On(expr))
                | JoinOperator::RightSemi(JoinConstraint::On(expr))
                | JoinOperator::LeftAnti(JoinConstraint::On(expr))
                | JoinOperator::RightAnti(JoinConstraint::On(expr)) => {
                    self.rewrite_node_exprs(expr, ctes, qual_rewrites);
                }
                JoinOperator::AsOf {
                    match_condition,
                    constraint,
                } => {
                    self.rewrite_node_exprs(match_condition, ctes, qual_rewrites);
                    if let JoinConstraint::On(expr) = constraint {
                        self.rewrite_node_exprs(expr, ctes, qual_rewrites);
                    }
                }
                _ => {}
            }
        }
    }

    fn rewrite_select(
        &mut self,
        select: &mut Select,
        ctes: &HashSet<String>,
    ) -> Vec<(Vec<String>, String)> {
        let mut tail_counts: HashMap<String, usize> = HashMap::new();
        for twj in &select.from {
            self.count_twj_tails(twj, ctes, &mut tail_counts);
        }

        let mut qual_rewrites: Vec<(Vec<String>, String)> = Vec::new();
        for twj in &mut select.from {
            self.rewrite_twj(twj, ctes, &tail_counts, &mut qual_rewrites);
        }

        for item in &mut select.projection {
            if let sqlparser::ast::SelectItem::QualifiedWildcard(obj_name, _) = item {
                if obj_name.0.len() >= 2 {
                    let prefix: Vec<String> = obj_name.0.iter().map(sql_ident_to_name).collect();
                    if let Some(new_qual) = lookup_qual_rewrite(&prefix, &qual_rewrites) {
                        obj_name.0 = vec![make_sql_ident(new_qual)];
                        self.rewritten = true;
                    }
                }
            }
        }

        self.rewrite_node_exprs(&mut select.projection, ctes, &qual_rewrites);
        self.rewrite_node_exprs(&mut select.lateral_views, ctes, &qual_rewrites);
        if let Some(prewhere) = &mut select.prewhere {
            self.rewrite_node_exprs(prewhere, ctes, &qual_rewrites);
        }
        if let Some(selection) = &mut select.selection {
            self.rewrite_node_exprs(selection, ctes, &qual_rewrites);
        }
        self.rewrite_node_exprs(&mut select.group_by, ctes, &qual_rewrites);
        self.rewrite_node_exprs(&mut select.cluster_by, ctes, &qual_rewrites);
        self.rewrite_node_exprs(&mut select.distribute_by, ctes, &qual_rewrites);
        self.rewrite_node_exprs(&mut select.sort_by, ctes, &qual_rewrites);
        if let Some(having) = &mut select.having {
            self.rewrite_node_exprs(having, ctes, &qual_rewrites);
        }
        self.rewrite_node_exprs(&mut select.named_window, ctes, &qual_rewrites);
        if let Some(qualify) = &mut select.qualify {
            self.rewrite_node_exprs(qualify, ctes, &qual_rewrites);
        }
        qual_rewrites
    }

    fn rewrite_node_exprs<N: VisitMut>(
        &mut self,
        node: &mut N,
        ctes: &HashSet<String>,
        qual_rewrites: &[(Vec<String>, String)],
    ) {
        let mut visitor = SelectExprVisitor {
            ctx: self,
            ctes,
            qual_rewrites,
            deferred_subqueries: Vec::new(),
        };
        let _ = node.visit(&mut visitor);
    }
}

fn lookup_qual_rewrite<'a>(
    prefix: &[String],
    qual_rewrites: &'a [(Vec<String>, String)],
) -> Option<&'a str> {
    let exact: Vec<&str> = qual_rewrites
        .iter()
        .filter(|(segs, _)| segs.as_slice() == prefix)
        .map(|(_, q)| q.as_str())
        .collect();
    if exact.len() == 1 {
        return Some(exact[0]);
    }
    let ci: Vec<&str> = qual_rewrites
        .iter()
        .filter(|(segs, _)| {
            segs.len() == prefix.len()
                && segs
                    .iter()
                    .zip(prefix)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
        })
        .map(|(_, q)| q.as_str())
        .collect();
    if ci.len() == 1 {
        Some(ci[0])
    } else {
        None
    }
}

struct SelectExprVisitor<'a, 'b> {
    ctx: &'a mut SqlRewriteContext<'b>,
    ctes: &'a HashSet<String>,
    qual_rewrites: &'a [(Vec<String>, String)],
    deferred_subqueries: Vec<(String, Expr)>,
}

impl<'a, 'b> VisitorMut for SelectExprVisitor<'a, 'b> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                let sentinel = self.ctx.alloc_sentinel();
                let mut owned =
                    std::mem::replace(expr, Expr::Value(Value::Placeholder(sentinel.clone())));
                match &mut owned {
                    Expr::Subquery(q) => self.ctx.rewrite_query(q, self.ctes),
                    Expr::Exists { subquery: q, .. } => self.ctx.rewrite_query(q, self.ctes),
                    Expr::InSubquery {
                        expr: lhs,
                        subquery: q,
                        ..
                    } => {
                        self.ctx
                            .rewrite_node_exprs(lhs.as_mut(), self.ctes, self.qual_rewrites);
                        self.ctx.rewrite_query(q, self.ctes);
                    }
                    _ => {}
                }
                self.deferred_subqueries.push((sentinel, owned));
            }
            Expr::CompoundIdentifier(parts)
                if parts.len() >= 3 && !self.qual_rewrites.is_empty() =>
            {
                let prefix: Vec<String> = parts[..parts.len() - 1]
                    .iter()
                    .map(sql_ident_to_name)
                    .collect();
                if let Some(new_qual) = lookup_qual_rewrite(&prefix, self.qual_rewrites) {
                    if let Some(col) = parts.last().cloned() {
                        *parts = vec![make_sql_ident(new_qual), col];
                        self.ctx.rewritten = true;
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(Value::Placeholder(tag)) = expr {
            if let Some((expected_tag, _)) = self.deferred_subqueries.last() {
                if tag == expected_tag {
                    let (_, owned) = self.deferred_subqueries.pop().unwrap();
                    *expr = owned;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

/// Rewrites SQL table references using `bolt_to_engine` and collects matched `bolt_name`s.
pub(crate) fn rewrite_sql_for_tables(
    sql: &str,
    bolt_to_engine: &HashMap<String, String>,
) -> (String, Option<Vec<String>>) {
    if bolt_to_engine.is_empty() {
        return (sql.to_string(), Some(Vec::new()));
    }
    let dialect = PostgreSqlDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return (sql.to_string(), None);
    };
    let mut ctx = SqlRewriteContext::new(bolt_to_engine);
    for stmt in &mut statements {
        ctx.rewrite_statement(stmt);
    }
    let rewritten_sql = if ctx.rewritten {
        statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    } else {
        sql.to_string()
    };
    (rewritten_sql, Some(ctx.refs))
}

/// Process-bound Bolt runtime hosting cached Arrow/GPU tables and executing analytical SQL.
pub struct BoltSessionRuntime {
    #[cfg(not(feature = "cuda-stub"))]
    engine: Engine,
    #[cfg(feature = "cuda-stub")]
    engine: Option<Engine>,
    #[cfg(feature = "cuda-stub")]
    stub_tables: HashMap<String, Vec<RecordBatch>>,
    plan_provider: StandardTableProvider,
    cached_tables: HashMap<String, CachedTableMeta>,
    table_schemas: HashMap<String, SchemaRef>,
    engine_idents: HashMap<String, String>,
    reverse_engine_idents: HashMap<String, String>,
    next_engine_ident_seq: u64,
    boot_epoch: Instant,
    owner_pid: u32,
    access_seq: u64,
    queries_executed: u64,
    eviction_count: u64,
    max_vram_bytes: Option<usize>,
    follow_guc_budget: bool,
    tx_stack: Vec<SessionTxFrame>,
    table_epochs: HashMap<String, u64>,
    table_snapshots: HashMap<String, (u32, u32)>,
    #[cfg(test)]
    fail_next_engine_op: Option<String>,
}

#[cfg(feature = "pgrx")]
#[inline]
pub(crate) fn pg_cancel_pending() -> bool {
    // SAFETY: Reading PostgreSQL backend-local interrupt flags atomically with volatile loads.
    unsafe {
        std::ptr::read_volatile(&raw const pgrx::pg_sys::QueryCancelPending) != Default::default()
            || std::ptr::read_volatile(&raw const pgrx::pg_sys::ProcDiePending)
                != Default::default()
    }
}

#[inline]
pub(crate) fn check_pg_cancel_pending() -> PgBoltResult<()> {
    #[cfg(feature = "pgrx")]
    if pg_cancel_pending() {
        return Err(PgBoltError::Cancelled(
            "PostgreSQL query cancellation pending".into(),
        ));
    }
    Ok(())
}

#[inline]
pub(crate) fn effective_timeout_with_pg(explicit_ms: u64) -> u64 {
    #[cfg(feature = "pgrx")]
    let pg_timeout_ms = {
        // SAFETY: Reading PostgreSQL backend-local `StatementTimeout` global integer.
        let raw = unsafe { std::ptr::read_volatile(&raw const pgrx::pg_sys::StatementTimeout) };
        u64::try_from(raw).unwrap_or(0)
    };
    #[cfg(not(feature = "pgrx"))]
    let pg_timeout_ms = 0u64;

    match (explicit_ms, pg_timeout_ms) {
        (0, pg) => pg,
        (explicit, 0) => explicit,
        (explicit, pg) => explicit.min(pg),
    }
}

impl BoltSessionRuntime {
    /// Initializes a new runtime using the current `BoltConfig`.
    ///
    /// Must only be called *after* PostgreSQL `fork()` in a backend or CUDA background worker.
    pub fn new() -> PgBoltResult<Self> {
        Self::new_with_config_inner(&current_config(), true)
    }

    /// Initializes a new runtime using an explicit `BoltConfig`.
    pub fn new_with_config(cfg: &BoltConfig) -> PgBoltResult<Self> {
        Self::new_with_config_inner(cfg, false)
    }

    fn new_with_config_inner(cfg: &BoltConfig, follow_guc_budget: bool) -> PgBoltResult<Self> {
        let max_vram_bytes = cfg.max_vram_bytes();
        let owner_pid = std::process::id();

        #[cfg(not(feature = "cuda-stub"))]
        let engine = {
            let mut builder = Engine::builder().sql_semantics(SqlSemantics::Standard);
            if let Some(budget) = max_vram_bytes {
                builder = builder.memory_budget(budget);
            }
            builder.build()?
        };

        #[cfg(feature = "cuda-stub")]
        let engine: Option<Engine> = None;

        Ok(Self {
            engine,
            #[cfg(feature = "cuda-stub")]
            stub_tables: HashMap::new(),
            plan_provider: StandardTableProvider::new(),
            cached_tables: HashMap::new(),
            table_schemas: HashMap::new(),
            engine_idents: HashMap::new(),
            reverse_engine_idents: HashMap::new(),
            next_engine_ident_seq: 0,
            boot_epoch: Instant::now(),
            owner_pid,
            access_seq: 0,
            queries_executed: 0,
            eviction_count: 0,
            max_vram_bytes,
            follow_guc_budget,
            tx_stack: Vec::new(),
            table_epochs: HashMap::new(),
            table_snapshots: HashMap::new(),
            #[cfg(test)]
            fail_next_engine_op: None,
        })
    }

    /// PID of the OS process that initialized this runtime.
    pub const fn owner_pid(&self) -> u32 {
        self.owner_pid
    }

    /// Returns `true` when backed by a live CUDA `Engine` (always `false` under `cuda-stub`).
    pub const fn has_engine(&self) -> bool {
        #[cfg(not(feature = "cuda-stub"))]
        {
            true
        }
        #[cfg(feature = "cuda-stub")]
        {
            self.engine.is_some()
        }
    }

    /// Verifies that the calling OS process matches the process that initialized this runtime.
    pub fn check_fork_safety(&self) -> PgBoltResult<()> {
        let current_pid = std::process::id();
        if current_pid != self.owner_pid {
            return Err(PgBoltError::ForkHazard {
                expected_pid: self.owner_pid,
                current_pid,
            });
        }
        Ok(())
    }

    fn effective_max_vram_bytes(&self) -> Option<usize> {
        if self.follow_guc_budget {
            current_config().max_vram_bytes()
        } else {
            self.max_vram_bytes
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.access_seq = self.access_seq.saturating_add(1);
        self.access_seq
    }

    fn total_cached_bytes_excluding(&self, excluded: Option<&str>) -> usize {
        self.cached_tables
            .iter()
            .filter(|(name, _)| excluded.is_none_or(|ex| name.as_str() != ex))
            .map(|(_, meta)| meta.approx_bytes)
            .fold(0usize, usize::saturating_add)
    }

    fn plan_vram_evictions(
        &self,
        target_name: &str,
        projected_target_bytes: usize,
    ) -> PgBoltResult<Vec<String>> {
        self.plan_vram_evictions_with_budget(
            self.effective_max_vram_bytes(),
            target_name,
            projected_target_bytes,
        )
    }

    /// Computes LRU victim tables to evict before mutating any state so failed registrations
    /// never leave healthy cached tables evicted or `plan_provider` out of sync.
    fn plan_vram_evictions_with_budget(
        &self,
        budget: Option<usize>,
        target_name: &str,
        projected_target_bytes: usize,
    ) -> PgBoltResult<Vec<String>> {
        let Some(budget) = budget else {
            return Ok(Vec::new());
        };

        if projected_target_bytes > budget {
            return Err(PgBoltError::VramExhausted(format!(
                "table `{target_name}` requires {projected_target_bytes} bytes, exceeding max VRAM budget of {budget} bytes"
            )));
        }

        let mut remaining_bytes = self.total_cached_bytes_excluding(Some(target_name));
        if remaining_bytes.saturating_add(projected_target_bytes) <= budget {
            return Ok(Vec::new());
        }

        let mut candidates: Vec<&CachedTableMeta> = self
            .cached_tables
            .values()
            .filter(|m| m.bolt_name != target_name)
            .collect();
        candidates.sort_by(|a, b| {
            (a.last_accessed_seq, a.loaded_at_ms, &a.bolt_name).cmp(&(
                b.last_accessed_seq,
                b.loaded_at_ms,
                &b.bolt_name,
            ))
        });

        let mut victims = Vec::new();
        for cand in candidates {
            if remaining_bytes.saturating_add(projected_target_bytes) <= budget {
                break;
            }
            remaining_bytes = remaining_bytes.saturating_sub(cand.approx_bytes);
            victims.push(cand.bolt_name.clone());
        }

        if remaining_bytes.saturating_add(projected_target_bytes) > budget {
            return Err(PgBoltError::VramExhausted(format!(
                "unable to free enough VRAM within {budget}-byte budget for `{target_name}`"
            )));
        }

        Ok(victims)
    }

    /// Applies updated GUC settings (such as a tightened `max_vram_mb` budget after `SIGHUP`),
    /// evicting oldest LRU cached tables if total usage exceeds the new limit.
    pub fn reconfigure(&mut self, cfg: &BoltConfig) -> PgBoltResult<()> {
        self.check_fork_safety()?;
        let new_budget = cfg.max_vram_bytes();
        let victims = self.plan_vram_evictions_with_budget(new_budget, "", 0)?;
        self.commit_evictions(&victims)?;
        self.max_vram_bytes = new_budget;
        self.follow_guc_budget = false;
        Ok(())
    }

    fn preview_engine_ident(
        &self,
        bolt_name: &str,
        _pending_evictions: &[String],
    ) -> (String, bool) {
        if let Some(existing) = self.engine_idents.get(bolt_name) {
            return (existing.clone(), false);
        }

        let is_claimed =
            |candidate: &str| -> bool { self.reverse_engine_idents.contains_key(candidate) };

        let exact_segs = exact_bolt_segments(bolt_name);
        if exact_segs.len() == 1 {
            let exact_single = &exact_segs[0];
            if !exact_single.is_empty() && !is_claimed(exact_single) {
                return (exact_single.clone(), true);
            }
        }

        let c_segs = canonical_bolt_segments(bolt_name);
        if let Some(tail) = c_segs.last() {
            if is_simple_ascii_ident(tail) && !is_claimed(tail) {
                return (tail.clone(), true);
            }
        }

        let base_tail = c_segs
            .last()
            .map(|s| sanitize_engine_ident_part(s))
            .unwrap_or_else(|| "tbl".to_string());
        let mut seq = self.next_engine_ident_seq.saturating_add(1);
        loop {
            let candidate = format!("pgb_t{seq}_{base_tail}");
            if !is_claimed(&candidate) {
                return (candidate, true);
            }
            seq = seq.saturating_add(1);
        }
    }

    fn commit_engine_ident(&mut self, bolt_name: &str, engine_ident: &str) {
        if !self.engine_idents.contains_key(bolt_name) {
            self.next_engine_ident_seq = self.next_engine_ident_seq.saturating_add(1);
        }
        self.engine_idents
            .insert(bolt_name.to_string(), engine_ident.to_string());
        self.reverse_engine_idents
            .insert(engine_ident.to_string(), bolt_name.to_string());
    }

    fn engine_ident_for(&self, bolt_name: &str) -> String {
        self.engine_idents
            .get(bolt_name)
            .cloned()
            .unwrap_or_else(|| bolt_name.to_string())
    }

    fn sync_plan_provider_table(
        &mut self,
        engine_ident: &str,
        bolt_schema: BoltSchema,
        null_flags: &[(String, bool)],
    ) {
        self.plan_provider
            .inner_mut()
            .register(engine_ident, bolt_schema);
        for (col_name, has_nulls) in null_flags {
            self.plan_provider.inner_mut().set_column_nullability(
                engine_ident,
                col_name.as_str(),
                *has_nulls,
            );
        }
    }

    fn commit_evictions(&mut self, victims: &[String]) -> PgBoltResult<()> {
        for victim in victims {
            self.evict_table(victim)?;
        }
        Ok(())
    }

    /// Registers or atomically replaces a full table snapshot in Bolt.
    pub fn register_batch(
        &mut self,
        pg_relation: &str,
        bolt_name: &str,
        batch: RecordBatch,
    ) -> PgBoltResult<CachedTableMeta> {
        self.check_fork_safety()?;
        if bolt_name.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "bolt table name must not be empty".into(),
            ));
        }

        let schema = batch.schema();
        let bolt_schema = arrow_schema_to_bolt(&schema)?;
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let approx_bytes = batch.get_array_memory_size();
        let null_flags: Vec<(String, bool)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|&(idx, _)| idx < batch.num_columns())
            .map(|(idx, field)| (field.name().clone(), batch.column(idx).null_count() > 0))
            .collect();

        // Compute VRAM budget eviction victims and target identifier BEFORE mutating state.
        let victims = self.plan_vram_evictions(bolt_name, approx_bytes)?;
        let replacing = self.cached_tables.contains_key(bolt_name);
        let (engine_ident, is_new_ident) = if replacing {
            (self.engine_ident_for(bolt_name), false)
        } else {
            self.preview_engine_ident(bolt_name, &victims)
        };

        #[cfg(test)]
        if let Some(msg) = self.fail_next_engine_op.take() {
            return Err(PgBoltError::Bolt(msg));
        }

        #[cfg(not(feature = "cuda-stub"))]
        {
            if replacing {
                self.engine.replace_table(engine_ident.as_str(), batch)?;
            } else {
                self.engine.register_table(engine_ident.as_str(), batch)?;
            }
        }

        #[cfg(feature = "cuda-stub")]
        {
            self.stub_tables.insert(bolt_name.to_string(), vec![batch]);
        }

        // Engine registration succeeded: now apply LRU evictions and update metadata/planner.
        self.commit_evictions(&victims)?;
        if is_new_ident {
            self.commit_engine_ident(bolt_name, &engine_ident);
        }
        self.sync_plan_provider_table(&engine_ident, bolt_schema, &null_flags);
        self.table_schemas
            .insert(bolt_name.to_string(), schema.clone());

        let seq = self.next_seq();
        let meta = CachedTableMeta {
            pg_relation: pg_relation.to_string(),
            bolt_name: bolt_name.to_string(),
            num_rows,
            num_cols,
            approx_bytes,
            loaded_at_ms: self.boot_epoch.elapsed().as_millis(),
            last_accessed_seq: seq,
            dirty: false,
        };
        self.cached_tables
            .insert(bolt_name.to_string(), meta.clone());
        let cur_epoch = current_catalog_epoch();
        self.table_epochs.insert(bolt_name.to_string(), cur_epoch);
        if let Some(frame) = self.tx_stack.last_mut() {
            if !frame.added_tables.contains(&meta.bolt_name) {
                frame.added_tables.push(meta.bolt_name.clone());
            }
            frame.pending_evict_on_commit.remove(&meta.bolt_name);
        }
        #[cfg(feature = "pgrx")]
        crate::planner::register_planner_table(meta.clone(), schema);
        Ok(meta)
    }

    /// Appends an incremental MVCC delta `RecordBatch` to an already-cached table.
    pub fn append_delta_batch(
        &mut self,
        bolt_name: &str,
        delta: RecordBatch,
    ) -> PgBoltResult<CachedTableMeta> {
        self.check_fork_safety()?;

        let existing = self.cached_tables.get(bolt_name).cloned().ok_or_else(|| {
            PgBoltError::Mvcc(format!(
                "cannot append delta: table `{bolt_name}` is not currently cached"
            ))
        })?;

        if let Some(expected_schema) = self.table_schemas.get(bolt_name) {
            if delta.schema() != *expected_schema {
                return Err(PgBoltError::Bridge(format!(
                    "delta batch schema mismatch for table `{bolt_name}`"
                )));
            }
        }

        let delta_rows = delta.num_rows();
        let delta_bytes = delta.get_array_memory_size();
        let projected_bytes = existing.approx_bytes.saturating_add(delta_bytes);
        let null_cols: Vec<String> = delta
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|&(idx, _)| idx < delta.num_columns() && delta.column(idx).null_count() > 0)
            .map(|(_, field)| field.name().clone())
            .collect();

        // Compute VRAM budget eviction victims BEFORE mutating healthy tables or `plan_provider`.
        let victims = self.plan_vram_evictions(bolt_name, projected_bytes)?;
        let engine_ident = self.engine_ident_for(bolt_name);

        #[cfg(test)]
        if let Some(msg) = self.fail_next_engine_op.take() {
            return Err(PgBoltError::Bolt(msg));
        }

        #[cfg(not(feature = "cuda-stub"))]
        {
            self.engine.register_batch(&engine_ident, delta)?;
        }

        #[cfg(feature = "cuda-stub")]
        {
            self.stub_tables
                .entry(bolt_name.to_string())
                .or_default()
                .push(delta);
        }

        // Engine append succeeded: commit evictions and metadata updates atomically.
        self.commit_evictions(&victims)?;
        for col_name in null_cols {
            self.plan_provider.inner_mut().set_column_nullability(
                engine_ident.as_str(),
                col_name,
                true,
            );
        }

        let seq = self.next_seq();
        let entry = self
            .cached_tables
            .get_mut(bolt_name)
            .ok_or_else(|| PgBoltError::Mvcc(format!("missing metadata for `{bolt_name}`")))?;
        entry.num_rows = entry.num_rows.saturating_add(delta_rows);
        entry.approx_bytes = projected_bytes;
        entry.last_accessed_seq = seq;
        entry.dirty = false;
        let cur_epoch = current_catalog_epoch();
        self.table_epochs.insert(bolt_name.to_string(), cur_epoch);
        let updated = entry.clone();
        #[cfg(feature = "pgrx")]
        if let Some(schema) = self.table_schemas.get(bolt_name).cloned() {
            crate::planner::register_planner_table(updated.clone(), schema);
        }
        Ok(updated)
    }

    /// Evicts a table from Bolt VRAM, host batch caches, and planner schema registry.
    pub fn evict_table(&mut self, bolt_name: &str) -> PgBoltResult<bool> {
        self.check_fork_safety()?;

        self.table_epochs.remove(bolt_name);
        self.table_snapshots.remove(bolt_name);
        for frame in &mut self.tx_stack {
            frame.added_tables.retain(|n| n != bolt_name);
            frame.dirtied_tables.retain(|(n, _)| n != bolt_name);
            frame.pending_evict_on_commit.remove(bolt_name);
        }

        let Some(engine_ident) = self.engine_idents.get(bolt_name).cloned() else {
            let existed = self.cached_tables.remove(bolt_name).is_some();
            self.table_schemas.remove(bolt_name);
            self.plan_provider.inner_mut().unregister_table(bolt_name);
            #[cfg(feature = "cuda-stub")]
            self.stub_tables.remove(bolt_name);
            #[cfg(feature = "pgrx")]
            if existed {
                crate::planner::unregister_planner_table(bolt_name);
            }
            if existed {
                self.eviction_count = self.eviction_count.saturating_add(1);
            }
            return Ok(existed);
        };

        #[cfg(not(feature = "cuda-stub"))]
        {
            self.engine.deregister_table(&engine_ident)?;
        }

        let existed = self.cached_tables.remove(bolt_name).is_some();
        self.table_schemas.remove(bolt_name);
        self.engine_idents.remove(bolt_name);
        self.reverse_engine_idents.remove(&engine_ident);
        self.plan_provider
            .inner_mut()
            .unregister_table(&engine_ident);
        #[cfg(feature = "pgrx")]
        if existed {
            crate::planner::unregister_planner_table(bolt_name);
        }

        #[cfg(feature = "cuda-stub")]
        {
            self.stub_tables.remove(bolt_name);
        }

        if existed {
            self.eviction_count = self.eviction_count.saturating_add(1);
        }
        Ok(existed)
    }

    fn set_dirty(&mut self, bolt_name: &str, dirty: bool) -> bool {
        let Some(meta) = self.cached_tables.get_mut(bolt_name) else {
            return false;
        };
        meta.dirty = dirty;
        #[cfg(feature = "pgrx")]
        if let Some(schema) = self.table_schemas.get(bolt_name).cloned() {
            crate::planner::register_planner_table(meta.clone(), schema);
        }
        true
    }

    /// Marks a cached table as having unapplied PostgreSQL heap changes.
    pub fn mark_dirty(&mut self, bolt_name: &str) -> bool {
        let prev_dirty = self.cached_tables.get(bolt_name).is_some_and(|m| m.dirty);
        let updated = self.set_dirty(bolt_name, true);
        if updated {
            let _ = bump_catalog_epoch();
            if let Some(frame) = self.tx_stack.last_mut() {
                if !frame.dirtied_tables.iter().any(|(n, _)| n == bolt_name) {
                    frame
                        .dirtied_tables
                        .push((bolt_name.to_string(), prev_dirty));
                }
            }
        }
        updated
    }

    /// Clears a cached table's dirty flag after refresh/reconciliation.
    pub fn clear_dirty(&mut self, bolt_name: &str) -> bool {
        let updated = self.set_dirty(bolt_name, false);
        if updated {
            let cur_epoch = current_catalog_epoch();
            self.table_epochs.insert(bolt_name.to_string(), cur_epoch);
        }
        updated
    }

    /// Returns `true` if `bolt_name` is currently cached in this runtime.
    pub fn is_table_cached(&self, bolt_name: &str) -> bool {
        self.cached_tables.contains_key(bolt_name)
    }

    /// Returns cached metadata for a single table if present.
    pub fn get_table_meta(&self, bolt_name: &str) -> Option<&CachedTableMeta> {
        self.cached_tables.get(bolt_name)
    }

    /// Returns sorted names of all cached tables.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.cached_tables.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns Arrow schemas of all cached tables for planner eligibility checks.
    pub fn table_schemas(&self) -> HashMap<String, SchemaRef> {
        self.table_schemas.clone()
    }

    /// Returns metadata snapshots for all cached tables ordered deterministically by name.
    pub fn list_tables(&self) -> Vec<CachedTableMeta> {
        let mut list: Vec<CachedTableMeta> = self.cached_tables.values().cloned().collect();
        list.sort_by(|a, b| a.bolt_name.cmp(&b.bolt_name));
        list
    }

    #[cfg(feature = "cuda-stub")]
    /// Returns stored batches for a cached table in `cuda-stub` mode.
    pub fn stub_batches(&self, bolt_name: &str) -> Option<&[RecordBatch]> {
        self.stub_tables.get(bolt_name).map(Vec::as_slice)
    }

    fn fallback_tokenize_refs(&self, sql: &str) -> Vec<String> {
        if self.cached_tables.is_empty() {
            return Vec::new();
        }
        let dialect = PostgreSqlDialect {};
        let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
            return Vec::new();
        };
        let sig: Vec<&Token> = tokens
            .iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect();

        let mut candidates: Vec<String> = Vec::new();
        let mut i = 0;
        while i < sig.len() {
            if let Token::Word(first) = sig[i] {
                let mut chain = first.value.clone();
                candidates.push(chain.clone());
                let mut j = i + 1;
                while j + 1 < sig.len() && matches!(sig[j], Token::Period) {
                    if let Token::Word(next) = sig[j + 1] {
                        chain.push('.');
                        chain.push_str(&next.value);
                        candidates.push(chain.clone());
                        candidates.push(next.value.clone());
                        j += 2;
                    } else {
                        break;
                    }
                }
                i = j;
            } else {
                i += 1;
            }
        }

        self.cached_tables
            .keys()
            .filter(|k| {
                candidates
                    .iter()
                    .any(|cand| cand == *k || cand.eq_ignore_ascii_case(k))
            })
            .cloned()
            .collect()
    }

    fn rewrite_sql_for_engine(&self, sql: &str) -> (String, Vec<String>) {
        let (rewritten_sql, maybe_refs) = rewrite_sql_for_tables(sql, &self.engine_idents);
        let refs = maybe_refs.unwrap_or_else(|| self.fallback_tokenize_refs(sql));
        (rewritten_sql, refs)
    }

    fn touch_table_names(&mut self, matched: Vec<String>) {
        for name in matched {
            let seq = self.next_seq();
            if let Some(meta) = self.cached_tables.get_mut(&name) {
                meta.last_accessed_seq = seq;
            }
        }
    }

    #[cfg(all(test, feature = "cuda-stub"))]
    fn touch_referenced_tables(&mut self, sql: &str) {
        if self.cached_tables.is_empty() {
            return;
        }
        let (_rewritten, matched) = self.rewrite_sql_for_engine(sql);
        self.touch_table_names(matched);
    }

    /// Executes analytical SQL on the active Bolt CUDA engine with an explicit timeout (`0` = unbounded)
    /// and cooperative cancellation support.
    pub fn execute_sql_with_timeout(
        &mut self,
        sql: &str,
        timeout_ms: u64,
    ) -> PgBoltResult<BoltExecutionResult> {
        self.check_fork_safety()?;
        if sql.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "SQL query must not be empty".into(),
            ));
        }

        let (rewritten_sql, matched_refs) = self.rewrite_sql_for_engine(sql);
        for name in &matched_refs {
            if let Some(meta) = self.cached_tables.get(name) {
                if meta.dirty {
                    return Err(PgBoltError::Mvcc(format!(
                        "cached table `{name}` is dirty and awaiting MVCC sync"
                    )));
                }
            }
        }
        self.touch_table_names(matched_refs);

        check_pg_cancel_pending()?;
        let effective_timeout_ms = effective_timeout_with_pg(timeout_ms);

        #[cfg(feature = "cuda-stub")]
        {
            let _ = (rewritten_sql, effective_timeout_ms);
            debug_assert!(self.engine.is_none());
            Err(PgBoltError::Bolt(
                "cuda-stub mode: GPU kernel execution disabled".into(),
            ))
        }

        #[cfg(not(feature = "cuda-stub"))]
        {
            let start = Instant::now();
            let handle = if effective_timeout_ms > 0 {
                let token = CancellationToken::new()
                    .with_timeout(Duration::from_millis(effective_timeout_ms));
                self.run_engine_sql_with_pg_cancel(&rewritten_sql, Some(&token))?
            } else {
                self.run_engine_sql_with_pg_cancel(&rewritten_sql, None)?
            };
            let tier = handle.planned_execution_tier();
            let batch = handle.into_record_batch();
            let elapsed_us = start.elapsed().as_micros();
            self.queries_executed = self.queries_executed.saturating_add(1);
            Ok(BoltExecutionResult {
                batch,
                elapsed_us,
                tier,
            })
        }
    }

    #[cfg(not(feature = "cuda-stub"))]
    fn run_engine_sql_with_pg_cancel(
        &self,
        rewritten_sql: &str,
        explicit_token: Option<&CancellationToken>,
    ) -> PgBoltResult<craton_bolt::QueryHandle> {
        #[cfg(feature = "pgrx")]
        {
            struct WatcherDoneGuard<'a>(&'a (StdMutex<bool>, Condvar));
            impl Drop for WatcherDoneGuard<'_> {
                fn drop(&mut self) {
                    let (lock, cvar) = self.0;
                    let mut finished = lock.lock().unwrap_or_else(|e| e.into_inner());
                    *finished = true;
                    cvar.notify_all();
                }
            }

            let token = explicit_token
                .cloned()
                .unwrap_or_else(CancellationToken::new);
            let done_pair = (StdMutex::new(false), Condvar::new());
            let watcher_pair = &done_pair;
            let watcher_token = token.clone();
            let result = std::thread::scope(|scope| {
                scope.spawn(move || {
                    let (lock, cvar) = watcher_pair;
                    let mut finished = lock.lock().unwrap_or_else(|e| e.into_inner());
                    while !*finished && !watcher_token.is_cancelled() {
                        if pg_cancel_pending() {
                            watcher_token.cancel();
                            break;
                        }
                        let wait = cvar
                            .wait_timeout(finished, Duration::from_millis(10))
                            .unwrap_or_else(|e| e.into_inner());
                        finished = wait.0;
                    }
                });
                let _done_guard = WatcherDoneGuard(&done_pair);
                self.engine.sql_with_cancel(rewritten_sql, &token)
            });
            Ok(result?)
        }
        #[cfg(not(feature = "pgrx"))]
        {
            let handle = if let Some(token) = explicit_token {
                self.engine.sql_with_cancel(rewritten_sql, token)?
            } else {
                self.engine.sql(rewritten_sql)?
            };
            Ok(handle)
        }
    }

    /// Executes analytical SQL on the active Bolt CUDA engine and returns batch, latency, and tier.
    pub fn execute_sql(&mut self, sql: &str) -> PgBoltResult<BoltExecutionResult> {
        let timeout_ms = current_config().statement_timeout_ms;
        self.execute_sql_with_timeout(sql, timeout_ms)
    }

    fn plan_physical(&self, sql: &str) -> PgBoltResult<(craton_bolt::LogicalPlan, PhysicalPlan)> {
        self.check_fork_safety()?;
        if sql.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "SQL query must not be empty".into(),
            ));
        }
        let (rewritten_sql, _) = self.rewrite_sql_for_engine(sql);
        let logical = parse_sql(&rewritten_sql, &self.plan_provider)?;
        let passes = default_passes();
        let optimized = run_to_fixpoint(&passes, logical)?;
        let physical = lower_physical(&optimized)?;
        Ok((optimized, physical))
    }

    /// Determines the `ExecutionTier` (`Gpu`, `Hybrid`, or `Host`) that Bolt would use for `sql`.
    pub fn explain_tier(&self, sql: &str) -> PgBoltResult<ExecutionTier> {
        let (_logical, physical) = self.plan_physical(sql)?;
        Ok(physical.planned_execution_tier())
    }

    /// Returns a human-readable logical and physical execution plan explanation for `sql`.
    pub fn explain_sql(&self, sql: &str) -> PgBoltResult<String> {
        self.check_fork_safety()?;
        if sql.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "SQL query must not be empty".into(),
            ));
        }

        #[cfg(not(feature = "cuda-stub"))]
        {
            let (rewritten_sql, _) = self.rewrite_sql_for_engine(sql);
            Ok(self.engine.explain_sql(&rewritten_sql)?)
        }

        #[cfg(feature = "cuda-stub")]
        {
            let (logical, physical) = self.plan_physical(sql)?;
            Ok(format!(
                "Logical plan:\n{}\n\nPhysical plan:\n{}",
                format_logical(&logical),
                format_physical(&physical)
            ))
        }
    }

    /// Returns a snapshot of runtime status, VRAM usage, PTX cache counters, and pool stats.
    pub fn status(&self) -> BoltRuntimeStatus {
        let total_cached_bytes = self.total_cached_bytes_excluding(None);
        let (ptx_cache_hits, ptx_cache_misses, ptx_cache_evictions) = ptx_cache_stats();
        BoltRuntimeStatus {
            cuda_stub: cfg!(feature = "cuda-stub"),
            owner_pid: self.owner_pid,
            cached_tables: self.cached_tables.len(),
            total_cached_bytes,
            max_vram_bytes: self.effective_max_vram_bytes(),
            ptx_cache_hits,
            ptx_cache_misses,
            ptx_cache_evictions,
            pool_stats: pool_stats(),
            queries_executed: self.queries_executed,
            eviction_count: self.eviction_count,
        }
    }

    /// Returns `true` if an explicit transaction or subtransaction block is active.
    pub fn is_in_transaction(&self) -> bool {
        !self.tx_stack.is_empty()
    }

    /// Returns the current nesting depth of transaction / subtransaction frames.
    pub fn transaction_depth(&self) -> usize {
        self.tx_stack.len()
    }

    /// Begins a top-level transaction or subtransaction frame identified by `subid`.
    pub fn begin_subtransaction(&mut self, subid: u64) {
        self.tx_stack.push(SessionTxFrame {
            subid,
            ..Default::default()
        });
    }

    /// Commits the active subtransaction frame `subid`, merging tracked mutations into the parent frame.
    pub fn commit_subtransaction(&mut self, subid: u64) -> bool {
        let Some(pos) = self.tx_stack.iter().rposition(|f| f.subid == subid) else {
            return false;
        };
        let child = self.tx_stack.remove(pos);
        if pos > 0 {
            let parent = &mut self.tx_stack[pos - 1];
            for added in child.added_tables {
                if !parent.added_tables.contains(&added) {
                    parent.added_tables.push(added);
                }
            }
            for (name, prev_dirty) in child.dirtied_tables {
                if !parent.dirtied_tables.iter().any(|(n, _)| n == &name) {
                    parent.dirtied_tables.push((name, prev_dirty));
                }
            }
            parent
                .pending_evict_on_commit
                .extend(child.pending_evict_on_commit);
        }
        true
    }

    /// Aborts the subtransaction frame `subid` and all nested child frames,
    /// evicting uncommitted newly added tables and restoring clean dirty flags.
    pub fn abort_subtransaction(&mut self, subid: u64) -> PgBoltResult<bool> {
        let Some(pos) = self.tx_stack.iter().rposition(|f| f.subid == subid) else {
            return Ok(false);
        };
        let aborted_frames: Vec<SessionTxFrame> = self.tx_stack.split_off(pos);
        self.apply_session_abort_frames(aborted_frames)?;
        Ok(true)
    }

    /// Begins a top-level transaction block.
    pub fn begin_transaction(&mut self, tx_id: u64) {
        self.tx_stack.clear();
        self.tx_stack.push(SessionTxFrame {
            subid: tx_id,
            ..Default::default()
        });
    }

    /// Commits the top-level transaction block, applying any pending evictions.
    pub fn commit_transaction(&mut self, _tx_id: u64) -> PgBoltResult<()> {
        let frames = std::mem::take(&mut self.tx_stack);
        for frame in frames {
            for bolt_name in frame.pending_evict_on_commit {
                let _ = self.evict_table(&bolt_name)?;
            }
        }
        Ok(())
    }

    /// Aborts the top-level transaction block, rolling back all uncommitted mutations.
    pub fn abort_transaction(&mut self, _tx_id: u64) -> PgBoltResult<()> {
        let frames = std::mem::take(&mut self.tx_stack);
        self.apply_session_abort_frames(frames)
    }

    fn apply_session_abort_frames(&mut self, frames: Vec<SessionTxFrame>) -> PgBoltResult<()> {
        if frames.is_empty() {
            return Ok(());
        }
        let mut to_evict = Vec::new();
        let mut to_restore_clean = Vec::new();

        for frame in frames.into_iter().rev() {
            for bolt_name in frame.added_tables {
                if !to_evict.contains(&bolt_name) {
                    to_evict.push(bolt_name);
                }
            }
            for (bolt_name, prev_dirty) in frame.dirtied_tables {
                if !prev_dirty && !to_restore_clean.contains(&bolt_name) {
                    to_restore_clean.push(bolt_name);
                }
            }
        }

        to_restore_clean.retain(|name| !to_evict.contains(name));

        for bolt_name in to_evict {
            let _ = self.evict_table(&bolt_name)?;
        }
        for bolt_name in to_restore_clean {
            if let Some(entry) = self.cached_tables.get_mut(&bolt_name) {
                entry.dirty = false;
            }
            #[cfg(feature = "pgrx")]
            if let Some(schema) = self.table_schemas.get(&bolt_name).cloned() {
                if let Some(meta) = self.cached_tables.get(&bolt_name).cloned() {
                    crate::planner::register_planner_table(meta, schema);
                }
            }
        }
        Ok(())
    }

    /// Returns the epoch at which `bolt_name` was registered or refreshed.
    pub fn table_epoch(&self, bolt_name: &str) -> Option<u64> {
        self.table_epochs.get(bolt_name).copied()
    }

    /// Sets the catalog epoch for `bolt_name`.
    pub fn set_table_epoch(&mut self, bolt_name: &str, epoch: u64) {
        self.table_epochs.insert(bolt_name.to_string(), epoch);
    }

    /// Records the PostgreSQL MVCC snapshot bounds (`xmin`, `xmax`) associated with a cached table.
    pub fn set_table_snapshot(&mut self, bolt_name: &str, xmin: u32, xmax: u32) {
        self.table_snapshots
            .insert(bolt_name.to_string(), (xmin, xmax));
    }

    /// Returns the recorded PostgreSQL MVCC snapshot bounds (`(xmin, xmax)`) for a cached table.
    pub fn table_snapshot(&self, bolt_name: &str) -> Option<(u32, u32)> {
        self.table_snapshots.get(bolt_name).copied()
    }

    /// Checks whether `bolt_name` was registered at an epoch strictly older than `target_epoch`.
    pub fn is_table_epoch_stale(&self, bolt_name: &str, target_epoch: u64) -> bool {
        self.table_epochs
            .get(bolt_name)
            .is_some_and(|&e| e < target_epoch)
    }

    /// Evaluates whether the cached snapshot for `bolt_name` is stale relative to `active_xmin` and `active_xmax`.
    pub fn is_snapshot_stale(&self, bolt_name: &str, active_xmin: u32, active_xmax: u32) -> bool {
        if let Some(&(xmin, xmax)) = self.table_snapshots.get(bolt_name) {
            if active_xmin != 0 && xmax != 0 && crate::mvcc::xid_precedes(xmax, active_xmin) {
                return true;
            }
            if active_xmax != 0 && xmax != 0 && crate::mvcc::xid_precedes(xmax, active_xmax) {
                return true;
            }
            if active_xmax != 0 && xmin != 0 && crate::mvcc::xid_precedes(active_xmax, xmin) {
                return true;
            }
            false
        } else {
            false
        }
    }
}

thread_local! {
    static BOLT_RT: RefCell<Option<BoltSessionRuntime>> = const { RefCell::new(None) };
}

/// Executes `f` with the calling thread's lazily-initialized `BoltSessionRuntime`.
pub fn with_bolt_runtime<F, R>(f: F) -> PgBoltResult<R>
where
    F: FnOnce(&mut BoltSessionRuntime) -> PgBoltResult<R>,
{
    BOLT_RT.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            *borrow = Some(BoltSessionRuntime::new()?);
        }
        let rt = borrow
            .as_mut()
            .ok_or_else(|| PgBoltError::Bolt("runtime unavailable".into()))?;
        rt.check_fork_safety()?;
        f(rt)
    })
}

/// Drops the calling thread's `BoltSessionRuntime`, releasing cached tables and CUDA resources.
pub fn reset_bolt_runtime() {
    BOLT_RT.with(|cell| {
        *cell.borrow_mut() = None;
    });
    #[cfg(feature = "pgrx")]
    crate::planner::clear_planner_tables();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_qualified_identifier_accepts_valid_and_escapes_injection() {
        assert_eq!(quote_qualified_identifier("sales").unwrap(), "\"sales\"");
        assert_eq!(
            quote_qualified_identifier("public.sales").unwrap(),
            "\"public\".\"sales\""
        );
        assert_eq!(
            quote_qualified_identifier("mydb.public.sales").unwrap(),
            "\"mydb\".\"public\".\"sales\""
        );
        assert_eq!(
            quote_qualified_identifier("  public.sales  ").unwrap(),
            "\"public\".\"sales\""
        );
        assert_eq!(
            quote_qualified_identifier("evil\"table; DROP TABLE users;--").unwrap(),
            "\"evil\"\"table; DROP TABLE users;--\""
        );
        assert_eq!(
            quote_qualified_identifier(r#""my.schema"."my.tbl""#).unwrap(),
            "\"my.schema\".\"my.tbl\""
        );
        assert_eq!(
            quote_qualified_identifier(r#"public."sales.2026""#).unwrap(),
            "\"public\".\"sales.2026\""
        );
        assert_eq!(
            quote_qualified_identifier(r#""a""b".tbl"#).unwrap(),
            "\"a\"\"b\".\"tbl\""
        );
        assert_eq!(
            quote_qualified_identifier(r#""MySchema"."Sales""#).unwrap(),
            "\"MySchema\".\"Sales\""
        );
        assert_eq!(
            quote_qualified_identifier("my\"schema.my\"tbl").unwrap(),
            "\"my\"\"schema\".\"my\"\"tbl\""
        );
    }

    #[test]
    fn quote_qualified_identifier_rejects_malformed_inputs() {
        for bad in [
            "",
            "   ",
            "sales\0x",
            ".sales",
            "sales.",
            "public..sales",
            "public .sales",
            "public. sales",
            "a.b.c.d",
            "sales\nDROP TABLE x",
            "\"unclosed",
            "\"a\"b",
            r#""""#,
            "\"a\" .b",
            "a.\"\"",
        ] {
            let err = quote_qualified_identifier(bad).unwrap_err();
            assert!(
                matches!(err, PgBoltError::Validation(_)),
                "expected Validation error for {bad:?}, got {err:?}"
            );
        }
    }

    #[cfg(feature = "cuda-stub")]
    mod stub_tests {
        use std::sync::Arc;

        use arrow_array::{Float64Array, Int32Array};
        use arrow_schema::{DataType, Field, Schema};

        use super::*;
        use crate::guc::with_test_config;

        fn sample_batch(rows: usize) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("val", DataType::Float64, false),
            ]));
            let ids: Vec<i32> = (0..rows as i32).collect();
            let vals: Vec<f64> = (0..rows).map(|i| i as f64 * 1.5).collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(Float64Array::from(vals)),
                ],
            )
            .unwrap()
        }

        #[test]
        fn stub_lifecycle_register_delta_explain_evict_and_fork_guard() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();
            assert!(!rt.has_engine());
            assert_eq!(rt.owner_pid(), std::process::id());
            assert!(rt.status().cuda_stub);

            let batch = sample_batch(64);
            let meta = rt.register_batch("public.sales", "sales", batch).unwrap();
            assert_eq!(meta.num_rows, 64);
            assert_eq!(meta.num_cols, 2);
            assert!(rt.is_table_cached("sales"));
            assert_eq!(rt.stub_batches("sales").unwrap().len(), 1);
            assert!(rt.table_schemas().contains_key("sales"));

            let delta = sample_batch(16);
            let updated = rt.append_delta_batch("sales", delta).unwrap();
            assert_eq!(updated.num_rows, 80);
            assert_eq!(rt.stub_batches("sales").unwrap().len(), 2);

            let gpu_tier = rt
                .explain_tier("SELECT id, val * 2.0 AS doubled FROM sales")
                .unwrap();
            assert_eq!(gpu_tier, ExecutionTier::Gpu);

            let tier = rt
                .explain_tier("SELECT id, SUM(val) FROM sales GROUP BY id")
                .unwrap();
            assert_eq!(tier, ExecutionTier::Hybrid);

            let plan_str = rt
                .explain_sql("SELECT id, SUM(val) FROM sales GROUP BY id")
                .unwrap();
            assert!(plan_str.contains("Logical plan:"));
            assert!(plan_str.contains("Physical plan:"));

            let exec_err = rt.execute_sql("SELECT COUNT(*) FROM sales").unwrap_err();
            assert!(matches!(exec_err, PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")));
            let timeout_err = rt
                .execute_sql_with_timeout("SELECT COUNT(*) FROM sales", 250)
                .unwrap_err();
            assert!(matches!(timeout_err, PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")));

            assert!(rt.mark_dirty("sales"));
            assert!(rt.get_table_meta("sales").unwrap().dirty);
            assert!(rt.clear_dirty("sales"));
            assert!(!rt.get_table_meta("sales").unwrap().dirty);

            assert!(rt.evict_table("sales").unwrap());
            assert!(!rt.evict_table("sales").unwrap());
            assert!(!rt.is_table_cached("sales"));
            assert_eq!(rt.status().eviction_count, 1);

            // Simulate post-fork PID mismatch.
            rt.owner_pid = rt.owner_pid.wrapping_add(1);
            let fork_err = rt.check_fork_safety().unwrap_err();
            assert!(matches!(fork_err, PgBoltError::ForkHazard { .. }));
        }

        #[test]
        fn lru_eviction_enforces_max_vram_budget() {
            let _lock = test_process_lock();
            // 1 MiB cap; each (i32 + f64) row is 12 B, so 30k (~360 KiB) + 35k (~420 KiB) fits,
            // and adding 30k (~360 KiB) exceeds 1 MiB cleanly after keeping t1 hot over t2.
            let cfg = BoltConfig {
                max_vram_mb: 1,
                ..BoltConfig::default()
            };
            with_test_config(cfg, || {
                let mut rt = BoltSessionRuntime::new().unwrap();
                let b_t1 = sample_batch(30_000); // ~360 KB
                let b_t2 = sample_batch(35_000); // ~420 KB
                let b_t3 = sample_batch(30_000); // ~360 KB -> forces eviction of LRU (t2)

                rt.register_batch("public.t1", "t1", b_t1).unwrap();
                rt.register_batch("public.t2", "t2", b_t2).unwrap();

                // Touch t1 so t2 becomes LRU victim.
                rt.touch_referenced_tables("SELECT SUM(val) FROM t1");
                rt.register_batch("public.t3", "t3", b_t3).unwrap();

                assert!(rt.is_table_cached("t1"), "hot table t1 must remain cached");
                assert!(!rt.is_table_cached("t2"), "cold table t2 must be evicted");
                assert!(rt.is_table_cached("t3"), "new table t3 must be cached");
                assert!(rt.status().eviction_count >= 1);

                // Single table exceeding the entire 1 MiB budget must fail without wiping cache.
                let huge = sample_batch(150_000); // ~1.8 MB > 1 MiB
                let err = rt.register_batch("public.huge", "huge", huge).unwrap_err();
                assert!(matches!(err, PgBoltError::VramExhausted(_)));
                assert!(rt.is_table_cached("t1"));
                assert!(rt.is_table_cached("t3"));
            });
        }

        #[test]
        fn failed_register_and_delta_preserve_cache_and_rollback_evictions() {
            let _lock = test_process_lock();
            let cfg = BoltConfig {
                max_vram_mb: 1,
                ..BoltConfig::default()
            };
            with_test_config(cfg, || {
                let mut rt = BoltSessionRuntime::new().unwrap();
                rt.register_batch("public.t1", "t1", sample_batch(30_000))
                    .unwrap();
                rt.register_batch("public.t2", "t2", sample_batch(35_000))
                    .unwrap();
                rt.touch_referenced_tables("SELECT SUM(val) FROM t1");

                // Inject an engine failure on a register that would otherwise evict t2.
                rt.fail_next_engine_op = Some("simulated engine register failure".into());
                let reg_err = rt
                    .register_batch("public.t3", "t3", sample_batch(30_000))
                    .unwrap_err();
                assert!(matches!(reg_err, PgBoltError::Bolt(ref msg) if msg.contains("simulated")));
                assert!(rt.is_table_cached("t1"), "t1 must stay cached on rollback");
                assert!(
                    rt.is_table_cached("t2"),
                    "LRU victim t2 must NOT be evicted on failure"
                );
                assert!(
                    !rt.is_table_cached("t3"),
                    "failed t3 must not appear in cache"
                );
                assert!(!rt.table_schemas().contains_key("t3"));
                assert_eq!(rt.status().eviction_count, 0);
                assert!(rt.explain_tier("SELECT id FROM t3").is_err());

                // Inject an engine failure on an append_delta that would otherwise evict t2.
                let prev_t1 = rt.get_table_meta("t1").unwrap().clone();
                rt.fail_next_engine_op = Some("simulated engine append failure".into());
                let delta_err = rt
                    .append_delta_batch("t1", sample_batch(30_000))
                    .unwrap_err();
                assert!(
                    matches!(delta_err, PgBoltError::Bolt(ref msg) if msg.contains("simulated"))
                );
                assert!(
                    rt.is_table_cached("t2"),
                    "t2 must remain cached after failed delta"
                );
                assert_eq!(rt.stub_batches("t1").unwrap().len(), 1);
                let after_t1 = rt.get_table_meta("t1").unwrap();
                assert_eq!(after_t1.num_rows, prev_t1.num_rows);
                assert_eq!(after_t1.approx_bytes, prev_t1.approx_bytes);
                assert_eq!(rt.status().eviction_count, 0);
            });
        }

        #[test]
        fn schema_qualified_tables_rewrite_and_resolve_cleanly() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();
            let meta_pub = rt
                .register_batch("public.sales", "public.sales", sample_batch(32))
                .unwrap();
            assert_eq!(meta_pub.bolt_name, "public.sales");

            let meta_ana = rt
                .register_batch("analytics.sales", "analytics.sales", sample_batch(16))
                .unwrap();
            assert_eq!(meta_ana.bolt_name, "analytics.sales");

            assert_eq!(
                rt.table_names(),
                vec!["analytics.sales".to_string(), "public.sales".to_string()]
            );

            assert_eq!(
                rt.explain_tier("SELECT id, val * 2.0 AS d FROM public.sales")
                    .unwrap(),
                ExecutionTier::Gpu
            );
            assert_eq!(
                rt.explain_tier("SELECT id, SUM(val) FROM analytics.sales GROUP BY id")
                    .unwrap(),
                ExecutionTier::Hybrid
            );

            let joined_plan = rt
                .explain_sql(
                    "SELECT public.sales.id, analytics.sales.val \
                     FROM public.sales JOIN analytics.sales ON public.sales.id = analytics.sales.id",
                )
                .unwrap();
            assert!(joined_plan.contains("Logical plan:"));

            let cte_plan = rt
                .explain_sql(
                    "WITH sales AS (SELECT id, val FROM public.sales) \
                     SELECT id, val * 2.0 AS d FROM sales",
                )
                .unwrap();
            assert!(cte_plan.contains("Physical plan:"));

            let prev_seq = rt
                .get_table_meta("analytics.sales")
                .unwrap()
                .last_accessed_seq;
            rt.touch_referenced_tables("SELECT val FROM analytics.sales");
            assert!(
                rt.get_table_meta("analytics.sales")
                    .unwrap()
                    .last_accessed_seq
                    > prev_seq
            );
        }

        #[test]
        fn thread_local_with_bolt_runtime_and_reset_work() {
            let _lock = test_process_lock();
            reset_bolt_runtime();
            let count = with_bolt_runtime(|rt| {
                rt.register_batch("public.s", "s", sample_batch(8))?;
                Ok(rt.list_tables().len())
            })
            .unwrap();
            assert_eq!(count, 1);

            reset_bolt_runtime();
            let count_after = with_bolt_runtime(|rt| Ok(rt.list_tables().len())).unwrap();
            assert_eq!(count_after, 0);
            reset_bolt_runtime();
        }

        #[test]
        fn reconfigure_enforces_budget_evictions_and_uncaps_cleanly() {
            let _lock = test_process_lock();
            let cfg_capped = BoltConfig {
                max_vram_mb: 1,
                ..BoltConfig::default()
            };
            with_test_config(cfg_capped.clone(), || {
                let mut rt = BoltSessionRuntime::new_with_config(&BoltConfig::default()).unwrap();
                rt.register_batch("public.t1", "t1", sample_batch(55_000))
                    .unwrap();
                rt.register_batch("public.t2", "t2", sample_batch(55_000))
                    .unwrap();
                rt.touch_referenced_tables("SELECT SUM(val) FROM t1");

                rt.reconfigure(&cfg_capped).unwrap();
                assert!(rt.is_table_cached("t1"));
                assert!(!rt.is_table_cached("t2"));
                assert_eq!(rt.status().max_vram_bytes, Some(1024 * 1024));
                assert!(rt.status().eviction_count >= 1);

                // Even while outer GUC still carries 1 MiB, explicit reconfigure to unlimited (0)
                // uncaps the runtime without falling back to the outer GUC cap.
                rt.reconfigure(&BoltConfig::default()).unwrap();
                assert_eq!(rt.status().max_vram_bytes, None);
                rt.register_batch("public.t3", "t3", sample_batch(55_000))
                    .unwrap();
                assert!(rt.is_table_cached("t1"));
                assert!(rt.is_table_cached("t3"));
            });
        }

        #[test]
        fn sql_rewrite_collapses_order_by_qualifiers_and_preserves_mixed_case_aliases() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();
            rt.register_batch("public.sales", "public.sales", sample_batch(8))
                .unwrap();
            rt.register_batch("public.events", "MyAlias", sample_batch(8))
                .unwrap();

            let (rewritten, refs) = rt.rewrite_sql_for_engine(
                "SELECT public.sales.id FROM public.sales ORDER BY public.sales.id",
            );
            assert!(!rewritten.contains("public.sales"));
            assert!(
                rewritten.contains("ORDER BY \"sales\".id"),
                "unexpected rewritten SQL: {rewritten}"
            );
            assert_eq!(refs, vec!["public.sales".to_string()]);

            let (mixed_sql, _) = rt.rewrite_sql_for_engine("SELECT id FROM MyAlias");
            assert!(
                mixed_sql.contains("\"MyAlias\""),
                "unquoted mixed-case table must be quoted cleanly: {mixed_sql}"
            );
            assert!(rt.explain_sql("SELECT id FROM MyAlias").is_ok());
        }

        #[test]
        fn transaction_abort_evicts_new_tables_and_restores_dirty_flags() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();

            // Pre-existing table outside transaction
            rt.register_batch("public.clean_tbl", "clean_tbl", sample_batch(10))
                .unwrap();
            assert!(!rt.get_table_meta("clean_tbl").unwrap().dirty);

            // Begin transaction block
            rt.begin_transaction(101);
            assert!(rt.is_in_transaction());
            assert_eq!(rt.transaction_depth(), 1);

            // Mutate state within transaction: add a new table and mark pre-existing table dirty
            rt.register_batch("public.tx_tbl", "tx_tbl", sample_batch(20))
                .unwrap();
            assert!(rt.is_table_cached("tx_tbl"));
            rt.mark_dirty("clean_tbl");
            assert!(rt.get_table_meta("clean_tbl").unwrap().dirty);

            // Abort transaction: tx_tbl must be evicted and clean_tbl restored to clean
            rt.abort_transaction(101).unwrap();
            assert!(!rt.is_in_transaction());
            assert_eq!(rt.transaction_depth(), 0);
            assert!(
                !rt.is_table_cached("tx_tbl"),
                "uncommitted table must be evicted on abort"
            );
            assert!(
                rt.is_table_cached("clean_tbl"),
                "pre-existing table must remain"
            );
            assert!(
                !rt.get_table_meta("clean_tbl").unwrap().dirty,
                "dirty flag must be restored to clean"
            );

            // Successful commit preserves changes
            rt.begin_transaction(102);
            rt.register_batch("public.committed_tbl", "committed_tbl", sample_batch(15))
                .unwrap();
            rt.commit_transaction(102).unwrap();
            assert!(!rt.is_in_transaction());
            assert!(rt.is_table_cached("committed_tbl"));
        }

        #[test]
        fn subtransaction_savepoint_rollback_and_commit_hierarchy() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();

            rt.register_batch("public.root", "root", sample_batch(10))
                .unwrap();

            // Root transaction
            rt.begin_transaction(1);
            rt.register_batch("public.sp1_tbl", "sp1_tbl", sample_batch(10))
                .unwrap();
            rt.mark_dirty("root");

            // Savepoint 1 (subid 10)
            rt.begin_subtransaction(10);
            assert_eq!(rt.transaction_depth(), 2);
            rt.register_batch("public.sp2_tbl", "sp2_tbl", sample_batch(10))
                .unwrap();

            // Savepoint 2 (subid 20) nested under Savepoint 1
            rt.begin_subtransaction(20);
            assert_eq!(rt.transaction_depth(), 3);
            rt.register_batch("public.nested_tbl", "nested_tbl", sample_batch(5))
                .unwrap();
            assert!(rt.is_table_cached("nested_tbl"));

            // Abort Savepoint 2: only nested_tbl evicted, depth goes back to 2
            assert!(rt.abort_subtransaction(20).unwrap());
            assert_eq!(rt.transaction_depth(), 2);
            assert!(!rt.is_table_cached("nested_tbl"));
            assert!(rt.is_table_cached("sp2_tbl"));
            assert!(rt.is_table_cached("sp1_tbl"));

            // Commit Savepoint 1: merged into Root transaction
            assert!(rt.commit_subtransaction(10));
            assert_eq!(rt.transaction_depth(), 1);
            assert!(rt.is_table_cached("sp2_tbl"));

            // Abort Root transaction: sp1_tbl and sp2_tbl evicted, root table restored to clean
            rt.abort_transaction(1).unwrap();
            assert_eq!(rt.transaction_depth(), 0);
            assert!(!rt.is_table_cached("sp1_tbl"));
            assert!(!rt.is_table_cached("sp2_tbl"));
            assert!(rt.is_table_cached("root"));
            assert!(!rt.get_table_meta("root").unwrap().dirty);
        }

        #[test]
        fn catalog_epoch_and_staleness_tracking() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();

            let epoch_before = current_catalog_epoch();
            rt.register_batch("public.epoch_tbl", "epoch_tbl", sample_batch(10))
                .unwrap();
            let table_epoch = rt.table_epoch("epoch_tbl").unwrap();
            assert!(table_epoch >= epoch_before);

            // Table is not stale relative to its own registration epoch
            assert!(!rt.is_table_epoch_stale("epoch_tbl", table_epoch));

            // Advancing global catalog epoch marks table stale
            let next_epoch = bump_catalog_epoch();
            assert!(rt.is_table_epoch_stale("epoch_tbl", next_epoch));

            // Mark dirty advances catalog epoch
            let epoch_prior = current_catalog_epoch();
            rt.mark_dirty("epoch_tbl");
            assert!(current_catalog_epoch() > epoch_prior);

            // Clear dirty refreshes table epoch
            rt.clear_dirty("epoch_tbl");
            assert_eq!(
                rt.table_epoch("epoch_tbl").unwrap(),
                current_catalog_epoch()
            );
            assert!(!rt.is_table_epoch_stale("epoch_tbl", current_catalog_epoch()));
        }

        #[test]
        fn mvcc_snapshot_staleness_detection() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();

            rt.register_batch("public.snap_tbl", "snap_tbl", sample_batch(10))
                .unwrap();
            rt.set_table_snapshot("snap_tbl", 100, 200);
            assert_eq!(rt.table_snapshot("snap_tbl"), Some((100, 200)));

            // Query with active snapshot before snapshot xmax is NOT stale
            assert!(!rt.is_snapshot_stale("snap_tbl", 100, 190));

            // Query with active snapshot after snapshot xmax IS stale
            assert!(rt.is_snapshot_stale("snap_tbl", 100, 250));
            assert!(rt.is_snapshot_stale("snap_tbl", 210, 220));

            // Query with active snapshot strictly preceding table xmin IS stale (future cache)
            assert!(rt.is_snapshot_stale("snap_tbl", 50, 80));

            // Missing table is not reported stale
            assert!(!rt.is_snapshot_stale("nonexistent", 100, 300));
        }

        #[test]
        fn subtransaction_savepoint_commit_merges_to_root_and_aborts_cleanly() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();

            // Top-level transaction and nested subtransaction
            rt.begin_transaction(0);
            rt.begin_subtransaction(42);
            assert!(rt.is_in_transaction());
            rt.register_batch("public.sub_tbl", "sub_tbl", sample_batch(10))
                .unwrap();
            assert!(rt.is_table_cached("sub_tbl"));

            // Committing subtransaction merges into root frame
            assert!(rt.commit_subtransaction(42));
            assert!(rt.is_table_cached("sub_tbl"));

            // Aborting transaction must evict the merged table
            rt.abort_transaction(0).unwrap();
            assert!(!rt.is_table_cached("sub_tbl"));
            assert!(!rt.is_in_transaction());
        }

        #[test]
        fn execute_sql_rejects_dirty_tables() {
            let _lock = test_process_lock();
            let mut rt = BoltSessionRuntime::new().unwrap();

            rt.register_batch("public.dirty_guard", "dirty_guard", sample_batch(10))
                .unwrap();
            rt.mark_dirty("dirty_guard");

            let err = rt
                .execute_sql_with_timeout("SELECT COUNT(*) FROM dirty_guard", 1000)
                .unwrap_err();
            assert!(
                matches!(err, PgBoltError::Mvcc(ref msg) if msg.contains("dirty")),
                "expected Mvcc dirty error, got {err:?}"
            );

            // Once cleared, query execution proceeds past the dirty guard
            rt.clear_dirty("dirty_guard");
            let after_err = rt
                .execute_sql_with_timeout("SELECT COUNT(*) FROM dirty_guard", 1000)
                .unwrap_err();
            assert!(
                matches!(after_err, PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")),
                "expected cuda-stub execution error after clean, got {after_err:?}"
            );
        }

        #[test]
        fn standard_table_provider_and_pool_stats_serde() {
            let provider = StandardTableProvider::new();
            assert_eq!(provider.sql_semantics(), SqlSemantics::Standard);

            let stats = pool_stats();
            let json = serde_json::to_string(&BoltRuntimeStatus {
                cuda_stub: true,
                owner_pid: 1234,
                cached_tables: 0,
                total_cached_bytes: 0,
                max_vram_bytes: None,
                ptx_cache_hits: 0,
                ptx_cache_misses: 0,
                ptx_cache_evictions: 0,
                pool_stats: stats,
                queries_executed: 0,
                eviction_count: 0,
            })
            .unwrap();
            let deserialized: BoltRuntimeStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.owner_pid, 1234);
            assert!(deserialized.cuda_stub);
        }
    }
}
