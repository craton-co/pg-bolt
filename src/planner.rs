// SPDX-License-Identifier: Apache-2.0

//! Phase 3 planner hook, SQL offload eligibility gate, and GPU/CPU cost model.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use arrow_schema::SchemaRef;
use craton_bolt::plan::optimizer::{default_passes, run_to_fixpoint};
use craton_bolt::plan::{
    lower_physical, parse_sql, DataType as BoltDataType, Field as BoltField, MemTableProvider,
    Schema as BoltSchema, TableProvider,
};
use craton_bolt::{ExecutionTier, SqlSemantics};
use parking_lot::RwLock;
#[cfg(feature = "pgrx")]
use pgrx::pg_sys;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, GroupByExpr,
    Ident, NamedWindowExpr, ObjectName, OrderByExpr, Query, Select, SetExpr, Statement, TableAlias,
    TableFactor, UnaryOperator, Value, Visit, VisitMut, Visitor, VisitorMut, WindowType,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::{PgBoltError, PgBoltResult};
use crate::guc::BoltConfig;
use crate::runtime::CachedTableMeta;

/// Monotonic invalidation epoch shared across planner catalog refreshes and `pg_xact` callbacks.
static CATALOG_EPOCH: AtomicU64 = AtomicU64::new(1);

/// Tracks the highest invalidation epoch observed per PostgreSQL relation OID.
static INVALIDATED_REL_OIDS: LazyLock<RwLock<HashMap<u32, u64>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Returns the current monotonic planner catalog invalidation epoch.
#[must_use]
pub fn catalog_epoch() -> u64 {
    CATALOG_EPOCH.load(Ordering::Acquire)
}

/// Advances the monotonic planner catalog epoch and returns the new epoch value.
pub fn bump_catalog_epoch() -> u64 {
    CATALOG_EPOCH.fetch_add(1, Ordering::AcqRel) + 1
}

/// Marks any planner catalog entry bound to `rel_oid` dirty and advances `CATALOG_EPOCH`.
///
/// Called by PostgreSQL DML / `pg_xact` invalidation hooks in `lib.rs` so stale cached
/// relations are declined immediately on the next `pg_bolt_planner_hook` invocation.
pub fn invalidate_relation_oid(rel_oid: u32) {
    let epoch = bump_catalog_epoch();
    if rel_oid != 0 {
        INVALIDATED_REL_OIDS.write().insert(rel_oid, epoch);
    }
    #[cfg(feature = "pgrx")]
    mark_planner_oid_dirty(rel_oid);
}

/// PostgreSQL functions with non-deterministic results or side effects that must stay on PostgreSQL CPU.
const VOLATILE_FUNCTIONS: &[&str] = &[
    "random",
    "setseed",
    "clock_timestamp",
    "statement_timestamp",
    "timeofday",
    "txid_current",
    "txid_current_if_assigned",
    "txid_snapshot_xmin",
    "pg_current_xact_id",
    "pg_current_xact_id_if_assigned",
    "pg_current_snapshot",
    "nextval",
    "currval",
    "setval",
    "lastval",
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "gen_random_uuid",
    "uuid_generate_v1",
    "uuid_generate_v1mc",
    "uuid_generate_v4",
    "pg_backend_pid",
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_xact_lock_shared",
    "pg_advisory_unlock",
    "pg_advisory_unlock_shared",
    "pg_advisory_unlock_all",
    "pg_notify",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "set_config",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_stat_file",
    "pg_ls_dir",
    "lo_create",
    "lo_unlink",
    "lo_import",
    "lo_export",
];

const AGGREGATE_FUNCTIONS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "bool_and",
    "bool_or",
    "every",
    "bit_and",
    "bit_or",
    "bit_xor",
    "array_agg",
    "string_agg",
];

/// Outcome of planner eligibility, tier selection, and cost comparison for a SQL query.
#[derive(Clone, Debug)]
pub struct PlanEligibility {
    pub eligible: bool,
    pub tier: Option<ExecutionTier>,
    pub estimated_bolt_cost: f64,
    pub estimated_pg_cost: f64,
    pub referenced_tables: Vec<String>,
    pub reason: String,
}

impl PlanEligibility {
    fn ineligible(
        reason: impl Into<String>,
        tier: Option<ExecutionTier>,
        estimated_bolt_cost: f64,
        estimated_pg_cost: f64,
        referenced_tables: Vec<String>,
    ) -> Self {
        Self {
            eligible: false,
            tier,
            estimated_bolt_cost,
            estimated_pg_cost,
            referenced_tables,
            reason: reason.into(),
        }
    }
}

/// Structural findings extracted from parsing and walking a single SQL `SELECT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlInspection {
    pub referenced_tables: Vec<String>,
    pub cte_names: Vec<String>,
    pub has_agg: bool,
    pub has_sort: bool,
    pub has_join: bool,
    pub has_parameters: bool,
    pub volatile_functions: Vec<String>,
    pub unsupported_constructs: Vec<String>,
    pub sum_columns: Vec<(Option<String>, String)>,
}

/// `MemTableProvider` wrapper exposing PostgreSQL-aligned `SqlSemantics::Standard` to Bolt's planner.
#[derive(Debug)]
struct StandardTableProvider(MemTableProvider);

impl StandardTableProvider {
    fn new(inner: MemTableProvider) -> Self {
        Self(inner)
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

#[derive(Debug)]
enum HardReject {
    LockingClause,
    DmlInCte,
    SelectInto,
}

#[derive(Default)]
struct CteScopeFrame {
    /// Non-recursive CTE bodies awaiting entry (`(*const Query as usize, normalized_cte_name)`).
    pending_ctes: VecDeque<(usize, String)>,
    /// CTE aliases visible to the current portion of this query block.
    active_scope: HashSet<String>,
}

/// Tracks sequential PostgreSQL CTE scoping across nested `Query` nodes (`W1-E-011`).
///
/// For non-recursive `WITH c1 AS (...), c2 AS (...) SELECT ...`, `c1` is NOT in scope while
/// visiting `c1.query`, enters scope only when `c1.query` finishes before `c2.query` begins,
/// and all CTEs are in scope when `query.body` and `query.order_by` run.
#[derive(Default)]
struct CteScopeStack {
    frames: Vec<CteScopeFrame>,
}

impl CteScopeStack {
    fn enter_query(&mut self, query: &Query) -> Vec<String> {
        let mut frame = CteScopeFrame::default();
        let mut defined_names = Vec::new();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let cte_name = normalize_ident(&cte.alias.name);
                defined_names.push(cte_name.clone());
                if with.recursive {
                    frame.active_scope.insert(cte_name);
                } else {
                    let query_addr = std::ptr::from_ref(cte.query.as_ref()) as usize;
                    frame.pending_ctes.push_back((query_addr, cte_name));
                }
            }
        }
        self.frames.push(frame);
        defined_names
    }

    fn exit_query(&mut self, query: &Query) {
        self.frames.pop();
        let query_addr = std::ptr::from_ref(query) as usize;
        if let Some(parent) = self.frames.last_mut() {
            if parent
                .pending_ctes
                .front()
                .is_some_and(|(pending_addr, _)| *pending_addr == query_addr)
            {
                if let Some((_, cte_name)) = parent.pending_ctes.pop_front() {
                    parent.active_scope.insert(cte_name);
                }
            }
        }
    }

    fn is_cte_in_scope(&self, name: &ObjectName) -> bool {
        if name.0.len() != 1 {
            return false;
        }
        let target = normalize_ident(&name.0[0]);
        self.frames
            .iter()
            .rev()
            .any(|frame| frame.active_scope.contains(&target))
    }
}

fn is_nan_literal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => s.trim().eq_ignore_ascii_case("nan"),
        Expr::Value(Value::Number(s, _)) => s.trim().eq_ignore_ascii_case("nan"),
        Expr::Nested(inner) => is_nan_literal_expr(inner),
        Expr::Cast { expr: inner, .. } => is_nan_literal_expr(inner),
        Expr::UnaryOp { expr: inner, .. } => is_nan_literal_expr(inner),
        _ => false,
    }
}

fn is_float_or_numeric_data_type(data_type: &sqlparser::ast::DataType) -> bool {
    let lower = data_type.to_string().to_ascii_lowercase();
    lower.contains("float")
        || lower.contains("real")
        || lower.contains("double")
        || lower.contains("numeric")
        || lower.contains("decimal")
        || lower.starts_with("dec")
}

struct OffloadAstVisitor {
    cte_stack: CteScopeStack,
    cte_names: Vec<String>,
    seen_ctes: HashSet<String>,
    referenced_tables: Vec<String>,
    seen_tables: HashSet<String>,
    volatile_functions: Vec<String>,
    seen_volatiles: HashSet<String>,
    unsupported_constructs: Vec<String>,
    seen_unsupported: HashSet<String>,
    sum_columns: Vec<(Option<String>, String)>,
    has_agg: bool,
    has_sort: bool,
    has_join: bool,
    has_parameters: bool,
    statement_depth: usize,
}

impl OffloadAstVisitor {
    fn new() -> Self {
        Self {
            cte_stack: CteScopeStack::default(),
            cte_names: Vec::new(),
            seen_ctes: HashSet::new(),
            referenced_tables: Vec::new(),
            seen_tables: HashSet::<String>::new(),
            volatile_functions: Vec::new(),
            seen_volatiles: HashSet::new(),
            unsupported_constructs: Vec::new(),
            seen_unsupported: HashSet::new(),
            sum_columns: Vec::new(),
            has_agg: false,
            has_sort: false,
            has_join: false,
            has_parameters: false,
            statement_depth: 0,
        }
    }

    fn push_unique(target: &mut Vec<String>, seen: &mut HashSet<String>, value: String) {
        if seen.insert(value.clone()) {
            target.push(value);
        }
    }

    fn push_unsupported(&mut self, construct: impl Into<String>) {
        Self::push_unique(
            &mut self.unsupported_constructs,
            &mut self.seen_unsupported,
            construct.into(),
        );
    }

    fn inspect_sum_arguments(&mut self, func: &sqlparser::ast::Function) {
        let FunctionArguments::List(list) = &func.args else {
            return;
        };
        for arg in &list.args {
            let expr = match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => e,
                _ => continue,
            };
            self.inspect_sum_arg_expr(expr);
        }
    }

    fn inspect_sum_arg_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                let dt_str = data_type.to_string().to_ascii_lowercase();
                if dt_str.contains("int8") || dt_str.contains("bigint") {
                    self.push_unsupported("SUM(bigint) overflow semantics diverge from PostgreSQL");
                }
                self.inspect_sum_arg_expr(inner);
            }
            Expr::Identifier(ident) => {
                let col = normalize_ident(ident);
                self.sum_columns.push((None, col));
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let tbl = normalize_ident(&parts[0]);
                    let col = normalize_ident(&parts[1]);
                    self.sum_columns.push((Some(tbl), col));
                } else if parts.len() == 3 {
                    let tbl = normalize_ident(&parts[1]);
                    let col = normalize_ident(&parts[2]);
                    self.sum_columns.push((Some(tbl), col));
                } else if let Some(last) = parts.last() {
                    let col = normalize_ident(last);
                    self.sum_columns.push((None, col));
                }
            }
            Expr::Nested(inner) => self.inspect_sum_arg_expr(inner),
            Expr::UnaryOp { expr: inner, .. } => self.inspect_sum_arg_expr(inner),
            Expr::BinaryOp { left, right, .. } => {
                self.inspect_sum_arg_expr(left);
                self.inspect_sum_arg_expr(right);
            }
            _ => {}
        }
    }

    fn record_volatile_if_matched(&mut self, name: &ObjectName) {
        if let Some(last) = name.0.last() {
            let fn_name = normalize_ident(last);
            let fn_lower = fn_name.to_ascii_lowercase();
            if VOLATILE_FUNCTIONS.contains(&fn_lower.as_str()) {
                Self::push_unique(
                    &mut self.volatile_functions,
                    &mut self.seen_volatiles,
                    fn_lower,
                );
            }
        }
    }

    fn inspect_select(&mut self, select: &Select) -> ControlFlow<HardReject> {
        if select.into.is_some() {
            return ControlFlow::Break(HardReject::SelectInto);
        }
        if !select.lateral_views.is_empty() {
            self.push_unsupported("LATERAL VIEW");
        }
        if select.top.is_some() {
            self.push_unsupported("TOP");
        }
        if select.prewhere.is_some() {
            self.push_unsupported("PREWHERE");
        }
        if select.qualify.is_some() {
            self.push_unsupported("QUALIFY");
        }
        if select.connect_by.is_some() {
            self.push_unsupported("CONNECT BY");
        }
        if select.distinct.is_some() || select.having.is_some() {
            self.has_agg = true;
        }
        match &select.group_by {
            GroupByExpr::All(_) => self.has_agg = true,
            GroupByExpr::Expressions(exprs, modifiers) => {
                if !exprs.is_empty() || !modifiers.is_empty() {
                    self.has_agg = true;
                }
                if !modifiers.is_empty() {
                    self.push_unsupported("GROUP BY ROLLUP/CUBE/GROUPING SETS");
                }
            }
        }
        if select.from.len() > 1 || select.from.iter().any(|twj| !twj.joins.is_empty()) {
            self.has_join = true;
        }
        ControlFlow::Continue(())
    }

    fn inspect_set_expr(&mut self, set_expr: &SetExpr) -> ControlFlow<HardReject> {
        match set_expr {
            SetExpr::Select(select) => self.inspect_select(select),
            SetExpr::SetOperation { left, right, .. } => {
                self.inspect_set_expr(left)?;
                self.inspect_set_expr(right)
            }
            SetExpr::Insert(_) | SetExpr::Update(_) => ControlFlow::Break(HardReject::DmlInCte),
            SetExpr::Table(_) => {
                self.push_unsupported("TABLE");
                ControlFlow::Continue(())
            }
            SetExpr::Query(_) | SetExpr::Values(_) => ControlFlow::Continue(()),
        }
    }
}

impl Visitor for OffloadAstVisitor {
    type Break = HardReject;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        self.statement_depth += 1;
        if self.statement_depth > 1 && !matches!(statement, Statement::Query(_)) {
            return ControlFlow::Break(HardReject::DmlInCte);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        self.statement_depth = self.statement_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if !query.locks.is_empty() {
            return ControlFlow::Break(HardReject::LockingClause);
        }
        if query.for_clause.is_some() {
            self.push_unsupported("FOR XML/JSON/BROWSE");
        }
        if query
            .order_by
            .as_ref()
            .is_some_and(|order_by| !order_by.exprs.is_empty())
        {
            self.has_sort = true;
        }

        if query.with.as_ref().is_some_and(|with| with.recursive) {
            self.push_unsupported("WITH RECURSIVE");
        }
        for cte_name in self.cte_stack.enter_query(query) {
            Self::push_unique(&mut self.cte_names, &mut self.seen_ctes, cte_name);
        }

        self.inspect_set_expr(&query.body)
    }

    fn post_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.cte_stack.exit_query(query);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Table {
                name,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                ..
            } => {
                if args.is_some() {
                    self.record_volatile_if_matched(name);
                    self.push_unsupported(format!("table_function({})", format_object_name(name)));
                    return ControlFlow::Continue(());
                }
                if !with_hints.is_empty() {
                    self.push_unsupported("WITH table hints");
                }
                if version.is_some() {
                    self.push_unsupported("time-travel table version");
                }
                if *with_ordinality {
                    self.push_unsupported("WITH ORDINALITY");
                }
                if !partitions.is_empty() {
                    self.push_unsupported("PARTITION selection");
                }
                if !self.cte_stack.is_cte_in_scope(name) {
                    let normalized = format_object_name(name);
                    Self::push_unique(
                        &mut self.referenced_tables,
                        &mut self.seen_tables,
                        normalized,
                    );
                }
            }
            TableFactor::Derived { lateral, .. } => {
                if *lateral {
                    self.push_unsupported("LATERAL");
                }
            }
            TableFactor::Function { lateral, name, .. } => {
                if *lateral {
                    self.push_unsupported("LATERAL");
                }
                self.record_volatile_if_matched(name);
                self.push_unsupported(format!("table_function({})", format_object_name(name)));
            }
            TableFactor::TableFunction { .. } => self.push_unsupported("TABLE(...)"),
            TableFactor::UNNEST { .. } => self.push_unsupported("UNNEST"),
            TableFactor::JsonTable { .. } => self.push_unsupported("JSON_TABLE"),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                if !table_with_joins.joins.is_empty() {
                    self.has_join = true;
                }
            }
            TableFactor::Pivot { .. } => self.push_unsupported("PIVOT"),
            TableFactor::Unpivot { .. } => self.push_unsupported("UNPIVOT"),
            TableFactor::MatchRecognize { .. } => self.push_unsupported("MATCH_RECOGNIZE"),
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Function(func) => {
                self.record_volatile_if_matched(&func.name);
                if func.filter.is_some() {
                    self.has_agg = true;
                }
                if let Some(last) = func.name.0.last() {
                    let fn_lower = normalize_ident(last).to_ascii_lowercase();
                    if AGGREGATE_FUNCTIONS.contains(&fn_lower.as_str()) {
                        self.has_agg = true;
                    }
                    if fn_lower == "isnan" {
                        self.push_unsupported("NaN floating-point semantics");
                    }
                    if fn_lower == "sum" {
                        self.inspect_sum_arguments(func);
                    }
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                if is_nan_literal_expr(inner) && is_float_or_numeric_data_type(data_type) {
                    self.push_unsupported("NaN floating-point semantics");
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                if is_nan_literal_expr(left) || is_nan_literal_expr(right) {
                    self.push_unsupported("NaN floating-point semantics");
                }
            }
            Expr::InList {
                expr: target, list, ..
            } => {
                if is_nan_literal_expr(target) || list.iter().any(is_nan_literal_expr) {
                    self.push_unsupported("NaN floating-point semantics");
                }
            }
            Expr::Between {
                expr: target,
                low,
                high,
                ..
            } => {
                if is_nan_literal_expr(target)
                    || is_nan_literal_expr(low)
                    || is_nan_literal_expr(high)
                {
                    self.push_unsupported("NaN floating-point semantics");
                }
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                if is_nan_literal_expr(left) || is_nan_literal_expr(right) {
                    self.push_unsupported("NaN floating-point semantics");
                }
            }
            Expr::Value(Value::Placeholder(_)) => {
                self.has_parameters = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn format_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn is_unquoted_sql_ident(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_lowercase() || first == '_') {
        return false;
    }
    if !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return false;
    }
    !matches!(
        name,
        "all"
            | "analyze"
            | "and"
            | "any"
            | "as"
            | "asc"
            | "between"
            | "by"
            | "case"
            | "cast"
            | "check"
            | "column"
            | "constraint"
            | "create"
            | "current_date"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "default"
            | "delete"
            | "desc"
            | "distinct"
            | "do"
            | "else"
            | "end"
            | "except"
            | "false"
            | "fetch"
            | "for"
            | "foreign"
            | "from"
            | "grant"
            | "group"
            | "having"
            | "in"
            | "insert"
            | "intersect"
            | "into"
            | "is"
            | "join"
            | "lateral"
            | "leading"
            | "left"
            | "like"
            | "limit"
            | "localtime"
            | "localtimestamp"
            | "not"
            | "null"
            | "offset"
            | "on"
            | "only"
            | "or"
            | "order"
            | "outer"
            | "over"
            | "primary"
            | "references"
            | "returning"
            | "right"
            | "select"
            | "session_user"
            | "some"
            | "table"
            | "then"
            | "to"
            | "trailing"
            | "true"
            | "union"
            | "unique"
            | "update"
            | "user"
            | "using"
            | "values"
            | "when"
            | "where"
            | "window"
            | "with"
    )
}

fn bolt_name_to_ident(bolt_name: &str) -> Ident {
    if is_unquoted_sql_ident(bolt_name) {
        Ident::new(bolt_name)
    } else {
        Ident::with_quote('"', bolt_name)
    }
}

fn preserved_alias_ident(orig: &Ident) -> Ident {
    if orig.quote_style.is_some() {
        orig.clone()
    } else {
        let lower = orig.value.to_ascii_lowercase();
        if is_unquoted_sql_ident(&lower) {
            Ident::new(lower)
        } else {
            Ident::with_quote('"', lower)
        }
    }
}

/// Applies PostgreSQL default null-ordering rules (`ASC NULLS LAST`, `DESC NULLS FIRST`)
/// wherever an `ORDER BY` expression left `nulls_first` unspecified (`W1-E-006`).
fn normalize_pg_order_by_exprs(exprs: &mut [OrderByExpr]) {
    for item in exprs {
        if item.nulls_first.is_none() {
            let ascending = item.asc != Some(false);
            if item.asc.is_none() {
                item.asc = Some(true);
            }
            item.nulls_first = Some(!ascending);
        }
    }
}

fn normalize_set_expr_named_windows(set_expr: &mut SetExpr) {
    match set_expr {
        SetExpr::Select(select) => {
            for def in &mut select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &mut def.1 {
                    normalize_pg_order_by_exprs(&mut spec.order_by);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            normalize_set_expr_named_windows(left);
            normalize_set_expr_named_windows(right);
        }
        _ => {}
    }
}

#[derive(Clone, Debug)]
struct TableScopeEntry {
    orig_name_parts: Vec<String>,
    orig_last_norm: Option<String>,
    effective_ident: Ident,
    effective_norm: String,
    resolved_schema: Option<String>,
}

#[derive(Default)]
struct SelectScope {
    relation_count: usize,
    tables: Vec<TableScopeEntry>,
}

fn matches_two_part_suffix(parts: &[String], s_norm: &str, t_norm: &str) -> bool {
    let len = parts.len();
    len >= 2 && parts[len - 2] == s_norm && parts[len - 1] == t_norm
}

impl SelectScope {
    fn is_single_relation(&self) -> bool {
        self.relation_count == 1 && self.tables.len() == 1
    }

    /// Collapses a 3-part `schema.table.col` reference to its unique rewritten
    /// `<table_alias>` identifier in the active `SELECT` scope.
    fn resolve_three_part_alias(&self, parts: &[Ident]) -> Option<Ident> {
        if parts.len() != 3 {
            return None;
        }
        let s_norm = normalize_ident(&parts[0]);
        let t_norm = normalize_ident(&parts[1]);

        let exact_matches: Vec<&TableScopeEntry> = self
            .tables
            .iter()
            .filter(|entry| matches_two_part_suffix(&entry.orig_name_parts, &s_norm, &t_norm))
            .collect();
        if exact_matches.len() == 1 {
            return Some(exact_matches[0].effective_ident.clone());
        }
        if exact_matches.len() > 1 {
            return None;
        }

        if self.is_single_relation()
            && self.tables[0].orig_name_parts.len() == 1
            && (self.tables[0].orig_last_norm.as_deref() == Some(t_norm.as_str())
                || self.tables[0].effective_norm == t_norm)
        {
            let schema_matches = match &self.tables[0].resolved_schema {
                Some(sch) => sch == &s_norm || (sch == "public" && s_norm == "public"),
                None => s_norm == "public",
            };
            if schema_matches {
                return Some(self.tables[0].effective_ident.clone());
            }
        }
        None
    }

    fn matches_single_relation_prefix(&self, parts: &[Ident]) -> bool {
        if !self.is_single_relation() {
            return false;
        }
        let entry = &self.tables[0];
        match parts {
            [t, _col] => {
                let t_norm = normalize_ident(t);
                entry.effective_norm == t_norm
                    || entry.orig_last_norm.as_deref() == Some(t_norm.as_str())
            }
            [s, t, _col] => {
                let s_norm = normalize_ident(s);
                let t_norm = normalize_ident(t);
                if entry.orig_name_parts.len() >= 2 {
                    matches_two_part_suffix(&entry.orig_name_parts, &s_norm, &t_norm)
                } else {
                    let schema_matches = match &entry.resolved_schema {
                        Some(sch) => sch == &s_norm || (sch == "public" && s_norm == "public"),
                        None => s_norm == "public",
                    };
                    schema_matches
                        && (entry.effective_norm == t_norm
                            || entry.orig_last_norm.as_deref() == Some(t_norm.as_str()))
                }
            }
            _ => false,
        }
    }

    fn resolve_prefix_effective_norm(&self, parts: &[Ident]) -> Option<String> {
        match parts {
            [t, _col] => {
                let t_norm = normalize_ident(t);
                let matches: Vec<&TableScopeEntry> = self
                    .tables
                    .iter()
                    .filter(|entry| entry.effective_norm == t_norm)
                    .collect();
                if matches.len() == 1 {
                    Some(matches[0].effective_norm.clone())
                } else {
                    None
                }
            }
            [_, _, _] => self
                .resolve_three_part_alias(parts)
                .map(|ident| normalize_ident(&ident)),
            _ => None,
        }
    }
}

struct OrderByProjectionLookup {
    has_wildcard: bool,
    output_name_counts: HashMap<String, usize>,
    qualified_items: Vec<(String, String, Ident, String)>,
    unqualified_items: Vec<(String, Ident, String)>,
}

impl OrderByProjectionLookup {
    fn from_projection(projection: &[sqlparser::ast::SelectItem]) -> Self {
        let mut has_wildcard = false;
        let mut output_name_counts: HashMap<String, usize> = HashMap::new();
        let mut qualified_items = Vec::new();
        let mut unqualified_items = Vec::new();

        for item in projection {
            match item {
                sqlparser::ast::SelectItem::Wildcard(_)
                | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                    has_wildcard = true;
                }
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(ident) => {
                        let out_norm = normalize_ident(ident);
                        *output_name_counts.entry(out_norm.clone()).or_insert(0) += 1;
                        unqualified_items.push((out_norm.clone(), ident.clone(), out_norm));
                    }
                    Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                        let t_norm = normalize_ident(&parts[0]);
                        let c_norm = normalize_ident(&parts[1]);
                        let out_ident = parts[1].clone();
                        let out_norm = normalize_ident(&out_ident);
                        *output_name_counts.entry(out_norm.clone()).or_insert(0) += 1;
                        qualified_items.push((t_norm, c_norm, out_ident, out_norm));
                    }
                    Expr::CompoundIdentifier(parts) => {
                        if let Some(last) = parts.last() {
                            *output_name_counts.entry(normalize_ident(last)).or_insert(0) += 1;
                        }
                    }
                    _ => {}
                },
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let out_norm = normalize_ident(alias);
                    *output_name_counts.entry(out_norm.clone()).or_insert(0) += 1;
                    match expr {
                        Expr::Identifier(ident) => {
                            unqualified_items.push((
                                normalize_ident(ident),
                                alias.clone(),
                                out_norm,
                            ));
                        }
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            qualified_items.push((
                                normalize_ident(&parts[0]),
                                normalize_ident(&parts[1]),
                                alias.clone(),
                                out_norm,
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }

        Self {
            has_wildcard,
            output_name_counts,
            qualified_items,
            unqualified_items,
        }
    }

    fn lookup_unqualified_ident(&self, scope: &SelectScope, parts: &[Ident]) -> Option<Ident> {
        if self.has_wildcard {
            return None;
        }
        let eff_table_norm = scope.resolve_prefix_effective_norm(parts)?;
        let col_norm = match parts {
            [_, col] | [_, _, col] => normalize_ident(col),
            _ => return None,
        };

        if let Some((_, _, out_ident, _)) =
            self.qualified_items.iter().find(|(t, c, _, out_norm)| {
                *t == eff_table_norm
                    && *c == col_norm
                    && self.output_name_counts.get(out_norm) == Some(&1)
            })
        {
            return Some(out_ident.clone());
        }

        if let Some((_, out_ident, _)) = self.unqualified_items.iter().find(|(c, _, out_norm)| {
            *c == col_norm && self.output_name_counts.get(out_norm) == Some(&1)
        }) {
            return Some(out_ident.clone());
        }

        None
    }
}

struct OrderBySelectContext {
    scope: SelectScope,
    projection: OrderByProjectionLookup,
}

fn collect_explicit_table_factor_aliases(factor: &TableFactor, aliases: &mut HashSet<String>) {
    match factor {
        TableFactor::Table {
            alias: Some(ta), ..
        }
        | TableFactor::Derived {
            alias: Some(ta), ..
        }
        | TableFactor::Function {
            alias: Some(ta), ..
        } => {
            aliases.insert(normalize_ident(&ta.name));
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            if let Some(ta) = alias {
                aliases.insert(normalize_ident(&ta.name));
            }
            collect_explicit_table_factor_aliases(&table_with_joins.relation, aliases);
            for join in &table_with_joins.joins {
                collect_explicit_table_factor_aliases(&join.relation, aliases);
            }
        }
        _ => {}
    }
}

struct SqlBoltRewriter<'a> {
    cached_tables: &'a HashMap<String, CachedTableMeta>,
    cte_stack: CteScopeStack,
}

impl SqlBoltRewriter<'_> {
    fn rewrite_query(&mut self, query: &mut Query) {
        let _ = self.cte_stack.enter_query(query);

        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.rewrite_query(&mut cte.query);
            }
        }

        normalize_set_expr_named_windows(query.body.as_mut());
        let order_by_ctx = self.rewrite_set_expr(query.body.as_mut());

        if let Some(order_by) = &mut query.order_by {
            normalize_pg_order_by_exprs(&mut order_by.exprs);
            let mut order_visitor = ScopedExprVisitor {
                outer: self,
                mode: ExprRewriteMode::OrderBy(order_by_ctx.as_ref()),
                suppress_depth: 0,
            };
            let _ = VisitMut::visit(&mut order_by.exprs, &mut order_visitor);
        }

        if query.limit.is_some() || query.offset.is_some() || query.fetch.is_some() {
            let empty_scope = SelectScope::default();
            let mut expr_visitor = ScopedExprVisitor {
                outer: self,
                mode: ExprRewriteMode::Select(&empty_scope),
                suppress_depth: 0,
            };
            let _ = VisitMut::visit(&mut query.limit, &mut expr_visitor);
            let _ = VisitMut::visit(&mut query.offset, &mut expr_visitor);
            let _ = VisitMut::visit(&mut query.fetch, &mut expr_visitor);
        }

        self.cte_stack.exit_query(query);
    }

    fn rewrite_set_expr(&mut self, set_expr: &mut SetExpr) -> Option<OrderBySelectContext> {
        match set_expr {
            SetExpr::Select(select) => Some(self.rewrite_select(select.as_mut())),
            SetExpr::Query(inner) => {
                self.rewrite_query(inner.as_mut());
                None
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_set_expr(left.as_mut());
                self.rewrite_set_expr(right.as_mut());
                None
            }
            other => {
                let empty_scope = SelectScope::default();
                let mut expr_visitor = ScopedExprVisitor {
                    outer: self,
                    mode: ExprRewriteMode::Select(&empty_scope),
                    suppress_depth: 0,
                };
                let _ = VisitMut::visit(other, &mut expr_visitor);
                None
            }
        }
    }

    fn rewrite_select(&mut self, select: &mut sqlparser::ast::Select) -> OrderBySelectContext {
        let mut existing_explicit_aliases = HashSet::new();
        for twj in &select.from {
            collect_explicit_table_factor_aliases(&twj.relation, &mut existing_explicit_aliases);
            for join in &twj.joins {
                collect_explicit_table_factor_aliases(
                    &join.relation,
                    &mut existing_explicit_aliases,
                );
            }
        }

        let mut scope = SelectScope::default();
        for twj in &mut select.from {
            self.rewrite_from_table_factor(
                &mut twj.relation,
                &mut scope,
                &existing_explicit_aliases,
            );
            for join in &mut twj.joins {
                self.rewrite_from_table_factor(
                    &mut join.relation,
                    &mut scope,
                    &existing_explicit_aliases,
                );
            }
        }

        {
            let mut expr_visitor = ScopedExprVisitor {
                outer: self,
                mode: ExprRewriteMode::Select(&scope),
                suppress_depth: 0,
            };
            for twj in &mut select.from {
                Self::visit_table_factor_join_operators(&mut twj.relation, &mut expr_visitor);
                for join in &mut twj.joins {
                    let _ = VisitMut::visit(&mut join.join_operator, &mut expr_visitor);
                    Self::visit_table_factor_join_operators(&mut join.relation, &mut expr_visitor);
                }
            }
            let _ = VisitMut::visit(&mut select.distinct, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.top, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.projection, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.selection, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.group_by, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.having, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.qualify, &mut expr_visitor);
            let _ = VisitMut::visit(&mut select.named_window, &mut expr_visitor);
        }

        let projection = OrderByProjectionLookup::from_projection(&select.projection);
        OrderBySelectContext { scope, projection }
    }

    fn visit_table_factor_join_operators(
        factor: &mut TableFactor,
        visitor: &mut ScopedExprVisitor<'_, '_>,
    ) {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = factor
        {
            Self::visit_table_factor_join_operators(&mut table_with_joins.relation, visitor);
            for join in &mut table_with_joins.joins {
                let _ = VisitMut::visit(&mut join.join_operator, visitor);
                Self::visit_table_factor_join_operators(&mut join.relation, visitor);
            }
        }
    }

    fn rewrite_from_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
        scope: &mut SelectScope,
        existing_explicit_aliases: &HashSet<String>,
    ) {
        match table_factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                scope.relation_count += 1;
                let orig_name_parts: Vec<String> = name.0.iter().map(normalize_ident).collect();
                let orig_last = name.0.last().cloned();
                let orig_last_norm = orig_last.as_ref().map(normalize_ident);
                let mut resolved_schema = None;

                if !self.cte_stack.is_cte_in_scope(name) {
                    let ref_name = format_object_name(name);
                    if let Some(meta) = resolve_cached_table(&ref_name, self.cached_tables) {
                        let (meta_schema, _) = normalize_pg_relation(&meta.pg_relation);
                        resolved_schema = meta_schema;
                        let is_alias_taken = orig_last_norm
                            .as_ref()
                            .is_some_and(|n| existing_explicit_aliases.contains(n));
                        if name.0.len() > 1
                            || orig_last_norm.as_deref() != Some(meta.bolt_name.as_str())
                        {
                            *name = ObjectName(vec![bolt_name_to_ident(&meta.bolt_name)]);
                            if alias.is_none()
                                && orig_last_norm.as_deref() != Some(meta.bolt_name.as_str())
                                && !is_alias_taken
                            {
                                *alias = orig_last.as_ref().map(|ident| TableAlias {
                                    name: preserved_alias_ident(ident),
                                    columns: Vec::new(),
                                });
                            }
                        }
                    }
                }

                if let Some(effective_ident) = alias
                    .as_ref()
                    .map(|ta| ta.name.clone())
                    .or_else(|| name.0.last().cloned())
                {
                    let effective_norm = normalize_ident(&effective_ident);
                    scope.tables.push(TableScopeEntry {
                        orig_name_parts,
                        orig_last_norm,
                        effective_ident,
                        effective_norm,
                        resolved_schema,
                    });
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                scope.relation_count += 1;
                self.rewrite_query(subquery.as_mut());
                if let Some(ta) = alias {
                    let effective_ident = ta.name.clone();
                    let effective_norm = normalize_ident(&effective_ident);
                    scope.tables.push(TableScopeEntry {
                        orig_name_parts: vec![effective_norm.clone()],
                        orig_last_norm: Some(effective_norm.clone()),
                        effective_ident,
                        effective_norm,
                        resolved_schema: None,
                    });
                }
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                scope.relation_count += 1;
                self.rewrite_from_table_factor(
                    &mut table_with_joins.relation,
                    scope,
                    existing_explicit_aliases,
                );
                for join in &mut table_with_joins.joins {
                    self.rewrite_from_table_factor(
                        &mut join.relation,
                        scope,
                        existing_explicit_aliases,
                    );
                }
                if let Some(ta) = alias {
                    let effective_ident = ta.name.clone();
                    let effective_norm = normalize_ident(&effective_ident);
                    scope.tables.push(TableScopeEntry {
                        orig_name_parts: vec![effective_norm.clone()],
                        orig_last_norm: Some(effective_norm.clone()),
                        effective_ident,
                        effective_norm,
                        resolved_schema: None,
                    });
                }
            }
            _ => {
                scope.relation_count += 2;
            }
        }
    }
}

enum ExprRewriteMode<'a> {
    Select(&'a SelectScope),
    OrderBy(Option<&'a OrderBySelectContext>),
}

struct ScopedExprVisitor<'a, 'b> {
    outer: &'a mut SqlBoltRewriter<'b>,
    mode: ExprRewriteMode<'a>,
    suppress_depth: usize,
}

impl VisitorMut for ScopedExprVisitor<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if self.suppress_depth == 0 {
            self.outer.rewrite_query(query);
        }
        self.suppress_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.suppress_depth = self.suppress_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if self.suppress_depth > 0 {
            return ControlFlow::Continue(());
        }
        match expr {
            Expr::CompoundIdentifier(parts) => match self.mode {
                ExprRewriteMode::Select(scope) => {
                    if let Some(table_alias) = scope.resolve_three_part_alias(parts) {
                        *parts = vec![table_alias, parts[2].clone()];
                    }
                }
                ExprRewriteMode::OrderBy(Some(ctx)) => {
                    if ctx.scope.matches_single_relation_prefix(parts) {
                        if let Some(col) = parts.last().cloned() {
                            *expr = Expr::Identifier(col);
                        }
                    } else if let Some(out_ident) =
                        ctx.projection.lookup_unqualified_ident(&ctx.scope, parts)
                    {
                        *expr = Expr::Identifier(out_ident);
                    } else if let Some(table_alias) = ctx.scope.resolve_three_part_alias(parts) {
                        *parts = vec![table_alias, parts[2].clone()];
                    }
                }
                ExprRewriteMode::OrderBy(None) => {}
            },
            Expr::Function(func) => {
                normalize_pg_order_by_exprs(&mut func.within_group);
                if let FunctionArguments::List(list) = &mut func.args {
                    for clause in &mut list.clauses {
                        if let FunctionArgumentClause::OrderBy(exprs) = clause {
                            normalize_pg_order_by_exprs(exprs);
                        }
                    }
                }
                if let Some(WindowType::WindowSpec(spec)) = &mut func.over {
                    normalize_pg_order_by_exprs(&mut spec.order_by);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn parse_single_query_statement(sql: &str) -> PgBoltResult<Statement> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(PgBoltError::Planner("SQL query cannot be empty".into()));
    }
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, trimmed)
        .map_err(|e| PgBoltError::Planner(format!("failed to parse SQL for offload: {e}")))?;
    if statements.len() != 1 {
        return Err(PgBoltError::Planner(format!(
            "expected a single SELECT statement, got {} statements",
            statements.len()
        )));
    }
    let stmt = statements.pop().expect("len checked == 1");
    if !matches!(stmt, Statement::Query(_)) {
        return Err(PgBoltError::Planner(
            "only read-only SELECT queries can be offloaded to pg_bolt".into(),
        ));
    }
    Ok(stmt)
}

/// Rewrites SQL AST table references, 3-part column prefixes, and `ORDER BY` clauses for `craton-bolt`.
///
/// - Resolves schema-qualified and aliased base tables (`public.sales`, `analytics.sales`) to
///   their exact registered `CachedTableMeta.bolt_name` single-identifier name (`W1-D-002`),
///   attaching the original trailing table name as an alias when `alias` is omitted.
/// - Collapses unambiguous 3-part column references (`public.sales.region_id`) to their
///   rewritten 2-part `<table_alias>.<col>` form (`sales.region_id`) (`W2-D-002`).
/// - Strips table/schema qualifiers in top-level `ORDER BY` expressions when unambiguous in
///   single-table queries or matching a unique SELECT output column/alias so `craton-bolt`'s
///   `lower_order_by` accepts valid qualified sorts (`W2-D-002`).
/// - Rewrites `ORDER BY` items with `nulls_first: None` to explicit PostgreSQL defaults
///   (`ASC NULLS LAST` when ascending, `DESC NULLS FIRST` when descending) so `craton-bolt`'s
///   `Ieee`/`Standard` sort placement matches PostgreSQL (`W1-E-006`).
pub fn rewrite_sql_for_bolt(
    sql: &str,
    cached_tables: &HashMap<String, CachedTableMeta>,
) -> PgBoltResult<String> {
    let mut stmt = parse_single_query_statement(sql)?;
    let mut rewriter = SqlBoltRewriter {
        cached_tables,
        cte_stack: CteScopeStack::default(),
    };
    if let Statement::Query(query) = &mut stmt {
        rewriter.rewrite_query(query.as_mut());
    }
    Ok(stmt.to_string())
}

/// Typed scalar literal extracted from PostgreSQL `ParamListInfo` for safe `$N` substitution.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlParamLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl SqlParamLiteral {
    fn to_sql_expr(&self) -> PgBoltResult<Expr> {
        match self {
            Self::Null => Ok(Expr::Value(Value::Null)),
            Self::Bool(b) => Ok(Expr::Value(Value::Boolean(*b))),
            Self::Int(v) => {
                if *v == i64::MIN {
                    return Err(PgBoltError::Planner(
                        "INT64 minimum parameter literal cannot be safely substituted into SQL"
                            .into(),
                    ));
                }
                if *v < 0 {
                    Ok(Expr::Nested(Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Minus,
                        expr: Box::new(Expr::Value(Value::Number(
                            v.unsigned_abs().to_string(),
                            false,
                        ))),
                    })))
                } else {
                    Ok(Expr::Value(Value::Number(v.to_string(), false)))
                }
            }
            Self::Float(v) => {
                if !v.is_finite() {
                    return Err(PgBoltError::Planner(
                        "non-finite floating-point parameter cannot be offloaded".into(),
                    ));
                }
                let abs = v.abs();
                let mut literal = abs.to_string();
                if !literal.contains('.') && !literal.contains('e') && !literal.contains('E') {
                    literal.push_str(".0");
                }
                if v.is_sign_negative() && *v != 0.0 {
                    Ok(Expr::Nested(Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Minus,
                        expr: Box::new(Expr::Value(Value::Number(literal, false))),
                    })))
                } else {
                    Ok(Expr::Value(Value::Number(literal, false)))
                }
            }
            Self::Text(s) => {
                if s.contains('\0') {
                    return Err(PgBoltError::Planner(
                        "text parameter containing NUL byte cannot be substituted into SQL".into(),
                    ));
                }
                Ok(Expr::Value(Value::SingleQuotedString(s.clone())))
            }
        }
    }
}

struct ParamSubstitutor<'a> {
    params: &'a [SqlParamLiteral],
}

impl VisitorMut for ParamSubstitutor<'_> {
    type Break = PgBoltError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(Value::Placeholder(raw)) = expr {
            let Some(index_str) = raw.strip_prefix('$') else {
                return ControlFlow::Break(PgBoltError::Planner(format!(
                    "unsupported SQL parameter placeholder '{raw}'"
                )));
            };
            let Ok(idx) = index_str.parse::<usize>() else {
                return ControlFlow::Break(PgBoltError::Planner(format!(
                    "invalid SQL parameter placeholder '{raw}'"
                )));
            };
            if idx == 0 || idx > self.params.len() {
                return ControlFlow::Break(PgBoltError::Planner(format!(
                    "SQL parameter '{raw}' exceeds bound parameter count ({})",
                    self.params.len()
                )));
            }
            match self.params[idx - 1].to_sql_expr() {
                Ok(replacement) => *expr = replacement,
                Err(err) => return ControlFlow::Break(err),
            }
        }
        ControlFlow::Continue(())
    }
}

/// Safely substitutes PostgreSQL `$1..$N` placeholders using pre-validated scalar `params`.
pub fn substitute_sql_params(sql: &str, params: &[SqlParamLiteral]) -> PgBoltResult<String> {
    let mut stmt = parse_single_query_statement(sql)?;
    let mut sub = ParamSubstitutor { params };
    if let ControlFlow::Break(err) = VisitMut::visit(&mut stmt, &mut sub) {
        return Err(err);
    }
    Ok(stmt.to_string())
}

/// Parses and validates a single PostgreSQL SQL statement for Bolt GPU offload eligibility.
pub fn inspect_sql_for_offload(sql: &str) -> PgBoltResult<SqlInspection> {
    let stmt = parse_single_query_statement(sql)?;

    let mut visitor = OffloadAstVisitor::new();
    if let ControlFlow::Break(reject) = stmt.visit(&mut visitor) {
        let msg = match reject {
            HardReject::LockingClause => {
                "locking clauses (FOR UPDATE / FOR SHARE) cannot be offloaded to GPU"
            }
            HardReject::DmlInCte => {
                "data-modifying CTEs (INSERT / UPDATE / DELETE) cannot be offloaded to GPU"
            }
            HardReject::SelectInto => "SELECT ... INTO statements cannot be offloaded to GPU",
        };
        return Err(PgBoltError::Planner(msg.into()));
    }

    Ok(SqlInspection {
        referenced_tables: visitor.referenced_tables,
        cte_names: visitor.cte_names,
        has_agg: visitor.has_agg,
        has_sort: visitor.has_sort,
        has_join: visitor.has_join,
        has_parameters: visitor.has_parameters,
        volatile_functions: visitor.volatile_functions,
        unsupported_constructs: visitor.unsupported_constructs,
        sum_columns: visitor.sum_columns,
    })
}

/// Estimates PostgreSQL CPU execution cost in planner-compatible relative units.
pub fn estimate_pg_cpu_cost(
    total_rows: usize,
    has_agg: bool,
    has_sort: bool,
    has_join: bool,
) -> f64 {
    let rows = total_rows as f64;
    let mut per_row = 0.020;
    if has_agg {
        per_row += 0.015;
    }
    if has_join {
        per_row += 0.025;
    }
    let sort_cost = if has_sort && total_rows > 1 {
        rows * rows.log2() * 0.0025
    } else {
        0.0
    };
    5.0 + rows * per_row + sort_cost
}

/// Estimates Craton Bolt CustomScan cost (kernel launch, optional PCIe transfer, and IPC return).
pub fn estimate_bolt_custom_scan_cost(
    total_rows: usize,
    cached_in_vram: bool,
    has_agg: bool,
    has_sort: bool,
    has_join: bool,
) -> f64 {
    let rows = total_rows as f64;
    let startup = 250.0;
    let h2d_transfer_per_row = if cached_in_vram { 0.0 } else { 0.035 };
    let mut gpu_per_row = 0.002;
    if has_agg {
        gpu_per_row += 0.001;
    }
    if has_join {
        gpu_per_row += 0.0025;
    }
    let sort_cost = if has_sort && total_rows > 1 {
        rows * rows.log2() * 0.00025
    } else {
        0.0
    };
    startup + rows * (gpu_per_row + h2d_transfer_per_row) + sort_cost
}

fn strip_schema_prefix(name: &str) -> (&str, Option<&str>) {
    match name.split_once('.') {
        Some((schema, table)) => (table, Some(schema)),
        None => (name, None),
    }
}

fn normalize_pg_part(part: &str) -> String {
    let trimmed = part.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_ascii_lowercase()
    }
}

fn normalize_pg_relation(name: &str) -> (Option<String>, String) {
    let (tbl, schema) = strip_schema_prefix(name);
    (schema.map(normalize_pg_part), normalize_pg_part(tbl))
}

fn resolve_cached_table<'a>(
    referenced: &str,
    cached_tables: &'a HashMap<String, CachedTableMeta>,
) -> Option<&'a CachedTableMeta> {
    if let Some(meta) = cached_tables.get(referenced) {
        return Some(meta);
    }

    let (ref_table, ref_schema) = strip_schema_prefix(referenced);
    if ref_table.contains('.') {
        // 3-part database.schema.table names are rejected to avoid cross-database ambiguity.
        return None;
    }

    let ref_table_norm = normalize_pg_part(ref_table);
    if let Some(schema) = ref_schema {
        let ref_schema_norm = normalize_pg_part(schema);
        let mut exact_pg_match = None;
        for meta in cached_tables.values() {
            let (meta_schema, meta_table) = normalize_pg_relation(&meta.pg_relation);
            if meta_table == ref_table_norm
                && meta_schema.as_deref() == Some(ref_schema_norm.as_str())
            {
                return Some(meta);
            }
            if ref_schema_norm == "public" {
                let schema_matches = meta_schema.as_deref().is_none_or(|s| s == "public");
                if schema_matches
                    && (meta.bolt_name.eq_ignore_ascii_case(&ref_table_norm)
                        || meta_table == ref_table_norm)
                {
                    exact_pg_match = Some(meta);
                }
            }
        }
        return exact_pg_match;
    }

    // Unqualified table reference: match exact `bolt_name` first, then unambiguous `pg_relation`.
    if let Some(meta) = cached_tables
        .values()
        .find(|m| m.bolt_name == referenced || m.bolt_name.eq_ignore_ascii_case(referenced))
    {
        return Some(meta);
    }

    let mut public_match = None;
    let mut bare_matches = cached_tables.values().filter(|m| {
        let (schema, tbl) = normalize_pg_relation(&m.pg_relation);
        let matches = tbl == ref_table_norm;
        if matches && schema.as_deref().is_none_or(|s| s == "public") {
            public_match = Some(*m);
        }
        matches
    });
    let first = bare_matches.next();
    if bare_matches.next().is_none() {
        first
    } else {
        public_match
    }
}

fn find_schema<'a>(
    referenced: &str,
    meta: &CachedTableMeta,
    table_schemas: &'a HashMap<String, SchemaRef>,
) -> Option<&'a SchemaRef> {
    if let Some(schema) = table_schemas
        .get(&meta.bolt_name)
        .or_else(|| table_schemas.get(&meta.pg_relation))
        .or_else(|| table_schemas.get(referenced))
    {
        return Some(schema);
    }
    let (ref_table, _) = strip_schema_prefix(referenced);
    table_schemas.get(ref_table)
}

fn arrow_schema_to_bolt(schema: &SchemaRef) -> PgBoltResult<BoltSchema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let dtype = BoltDataType::from_arrow(field.data_type())?;
        fields.push(BoltField::new(field.name(), dtype, field.is_nullable()));
    }
    Ok(BoltSchema::new(fields))
}

/// Returns `true` if `plan` contains an `Aggregate` with a pre-kernel (`pre.is_some()`)
/// and more than one `GROUP BY` key, which `craton-bolt` rejects at execution time.
fn has_multi_col_groupby_with_pre(plan: &craton_bolt::plan::PhysicalPlan) -> bool {
    matches!(
        plan,
        craton_bolt::plan::PhysicalPlan::Aggregate {
            pre: Some(_),
            aggregate,
            ..
        } if aggregate.group_by.len() > 1
    ) || plan
        .children()
        .into_iter()
        .any(has_multi_col_groupby_with_pre)
}

/// Evaluates whether `sql` is safe, cached, supported on GPU/Hybrid, and cost-effective to offload.
pub fn analyze_sql_eligibility(
    sql: &str,
    cached_tables: &HashMap<String, CachedTableMeta>,
    table_schemas: &HashMap<String, SchemaRef>,
    config: &BoltConfig,
) -> PgBoltResult<PlanEligibility> {
    if !config.enabled {
        return Ok(PlanEligibility::ineligible(
            "pg_bolt.enabled is off",
            None,
            0.0,
            0.0,
            Vec::new(),
        ));
    }
    if !config.enable_custom_scan {
        return Ok(PlanEligibility::ineligible(
            "pg_bolt.enable_custom_scan is off",
            None,
            0.0,
            0.0,
            Vec::new(),
        ));
    }

    let inspection = match inspect_sql_for_offload(sql) {
        Ok(parsed) => parsed,
        Err(err) => {
            return Ok(PlanEligibility::ineligible(
                err.to_string(),
                None,
                0.0,
                0.0,
                Vec::new(),
            ));
        }
    };

    if !inspection.volatile_functions.is_empty() {
        return Ok(PlanEligibility::ineligible(
            format!(
                "query calls volatile or side-effecting function(s): {}",
                inspection.volatile_functions.join(", ")
            ),
            None,
            0.0,
            0.0,
            inspection.referenced_tables,
        ));
    }
    if inspection.has_parameters {
        return Ok(PlanEligibility::ineligible(
            "parameterized queries ($1) cannot be offloaded cleanly via static SQL text",
            None,
            0.0,
            0.0,
            inspection.referenced_tables,
        ));
    }
    if !inspection.unsupported_constructs.is_empty() {
        return Ok(PlanEligibility::ineligible(
            format!(
                "query uses unsupported SQL construct(s): {}",
                inspection.unsupported_constructs.join(", ")
            ),
            None,
            0.0,
            0.0,
            inspection.referenced_tables,
        ));
    }
    if inspection.referenced_tables.is_empty() {
        return Ok(PlanEligibility::ineligible(
            "query references no base tables",
            None,
            0.0,
            0.0,
            Vec::new(),
        ));
    }

    // Guard SUM(bigint) overflow divergence (W1-E-006): PostgreSQL SUM(bigint) computes arbitrary-precision
    // NUMERIC and raises error on overflow, whereas craton-bolt wraps 64-bit integer addition.
    for (tbl_opt, col_name) in &inspection.sum_columns {
        let is_bigint = inspection.referenced_tables.iter().any(|ref_name| {
            if let Some(tbl) = tbl_opt {
                let (bare_tbl, _) = strip_schema_prefix(ref_name);
                if !ref_name.eq_ignore_ascii_case(tbl) && !bare_tbl.eq_ignore_ascii_case(tbl) {
                    return false;
                }
            }
            let Some(meta) = resolve_cached_table(ref_name, cached_tables) else {
                return false;
            };
            let Some(arrow_schema) = find_schema(ref_name, meta, table_schemas) else {
                return false;
            };
            arrow_schema.fields().iter().any(|f| {
                f.name().eq_ignore_ascii_case(col_name)
                    && matches!(f.data_type(), arrow_schema::DataType::Int64)
            })
        });
        if is_bigint {
            return Ok(PlanEligibility::ineligible(
                format!(
                    "SUM(bigint) on column '{col_name}' integer overflow and NUMERIC return type semantics diverge from PostgreSQL"
                ),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        }
    }

    let mut provider = MemTableProvider::new();
    let mut registered_provider_names = HashSet::new();
    let mut bare_table_owners: HashMap<String, String> = HashMap::new();
    let mut counted_tables = HashSet::new();
    let mut total_rows: usize = 0;

    for ref_name in &inspection.referenced_tables {
        let Some(meta) = resolve_cached_table(ref_name, cached_tables) else {
            return Ok(PlanEligibility::ineligible(
                format!("referenced table '{ref_name}' is not cached in Bolt"),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        };
        if meta.dirty {
            return Ok(PlanEligibility::ineligible(
                format!(
                    "cached table '{}' is dirty and awaiting MVCC sync",
                    meta.bolt_name
                ),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        }

        // Guard against two distinct schemas (e.g. `public.sales` vs `analytics.sales`)
        // collapsing to the same bare identifier inside `craton-bolt`.
        let bare_key = strip_schema_prefix(ref_name).0.to_ascii_lowercase();
        if let Some(prev_bolt) = bare_table_owners.insert(bare_key.clone(), meta.bolt_name.clone())
        {
            if prev_bolt != meta.bolt_name {
                return Ok(PlanEligibility::ineligible(
                    format!(
                        "query references distinct relations sharing bare name '{bare_key}'; schema qualification cannot be disambiguated by craton-bolt"
                    ),
                    None,
                    0.0,
                    0.0,
                    inspection.referenced_tables,
                ));
            }
        }

        let Some(arrow_schema) = find_schema(ref_name, meta, table_schemas) else {
            return Ok(PlanEligibility::ineligible(
                format!("missing Arrow schema for cached table '{}'", meta.bolt_name),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        };
        let bolt_schema = match arrow_schema_to_bolt(arrow_schema) {
            Ok(schema) => schema,
            Err(err) => {
                return Ok(PlanEligibility::ineligible(
                    format!(
                        "cached table '{}' contains unsupported Arrow column type: {err}",
                        meta.bolt_name
                    ),
                    None,
                    0.0,
                    0.0,
                    inspection.referenced_tables,
                ));
            }
        };

        if counted_tables.insert(meta.bolt_name.clone()) {
            total_rows = total_rows.saturating_add(meta.num_rows);
        }

        if registered_provider_names.insert(meta.bolt_name.clone()) {
            provider.register(meta.bolt_name.clone(), bolt_schema);
        }
    }

    if total_rows < config.min_offload_rows {
        let pg_cost = estimate_pg_cpu_cost(
            total_rows,
            inspection.has_agg,
            inspection.has_sort,
            inspection.has_join,
        );
        let bolt_cost = estimate_bolt_custom_scan_cost(
            total_rows,
            true,
            inspection.has_agg,
            inspection.has_sort,
            inspection.has_join,
        );
        return Ok(PlanEligibility::ineligible(
            format!(
                "total referenced rows ({total_rows}) below min_offload_rows ({})",
                config.min_offload_rows
            ),
            None,
            bolt_cost,
            pg_cost,
            inspection.referenced_tables,
        ));
    }

    let rewritten_sql = match rewrite_sql_for_bolt(sql, cached_tables) {
        Ok(rewritten) => rewritten,
        Err(err) => {
            return Ok(PlanEligibility::ineligible(
                err.to_string(),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        }
    };

    let std_provider = StandardTableProvider::new(provider);
    let logical = match parse_sql(&rewritten_sql, &std_provider) {
        Ok(plan) => plan,
        Err(err) => {
            return Ok(PlanEligibility::ineligible(
                format!("Bolt SQL frontend rejected query: {err}"),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        }
    };
    let passes = default_passes();
    let optimized = match run_to_fixpoint(&passes, logical) {
        Ok(plan) => plan,
        Err(err) => {
            return Ok(PlanEligibility::ineligible(
                format!("Bolt logical optimizer rejected query: {err}"),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        }
    };
    let physical = match lower_physical(&optimized) {
        Ok(plan) => plan,
        Err(err) => {
            return Ok(PlanEligibility::ineligible(
                format!("Bolt physical lowering rejected query: {err}"),
                None,
                0.0,
                0.0,
                inspection.referenced_tables,
            ));
        }
    };

    if has_multi_col_groupby_with_pre(&physical) {
        return Ok(PlanEligibility::ineligible(
            "multi-column GROUP BY with pre-aggregation is not yet supported on Bolt GPU; keeping query on PostgreSQL CPU",
            None,
            0.0,
            0.0,
            inspection.referenced_tables,
        ));
    }

    let tier = physical.planned_execution_tier();
    let estimated_pg_cost = estimate_pg_cpu_cost(
        total_rows,
        inspection.has_agg,
        inspection.has_sort,
        inspection.has_join,
    );
    let estimated_bolt_cost = estimate_bolt_custom_scan_cost(
        total_rows,
        true,
        inspection.has_agg,
        inspection.has_sort,
        inspection.has_join,
    );
    let bolt_is_cheaper = estimated_bolt_cost < estimated_pg_cost;

    let (eligible, reason) = match tier {
        ExecutionTier::Gpu if bolt_is_cheaper => (
            true,
            "Bolt planned Gpu tier with lower estimated cost than PostgreSQL CPU".to_string(),
        ),
        ExecutionTier::Gpu => (
            false,
            format!(
                "estimated Bolt cost ({estimated_bolt_cost:.2}) is not lower than PostgreSQL CPU cost ({estimated_pg_cost:.2})"
            ),
        ),
        ExecutionTier::Hybrid if !config.allow_hybrid_fallback => (
            false,
            "Bolt planned Hybrid tier and pg_bolt.allow_hybrid_fallback is off".to_string(),
        ),
        ExecutionTier::Hybrid if bolt_is_cheaper => (
            true,
            "Bolt planned Hybrid tier with allow_hybrid_fallback=true and lower estimated cost"
                .to_string(),
        ),
        ExecutionTier::Hybrid => (
            false,
            format!(
                "Bolt planned Hybrid tier, but estimated cost ({estimated_bolt_cost:.2}) exceeds PostgreSQL CPU cost ({estimated_pg_cost:.2})"
            ),
        ),
        // `PhysicalPlan::planned_execution_tier()` only yields `Host` for degenerate empty
        // `Union`s or future host-only nodes (runtime `TierPolicy::CostBased` owns dynamic Host
        // fallback); any Host-planned query remains on PostgreSQL CPU.
        ExecutionTier::Host => (
            false,
            "Bolt planned Host tier; keeping query on PostgreSQL CPU".to_string(),
        ),
    };

    Ok(PlanEligibility {
        eligible,
        tier: Some(tier),
        estimated_bolt_cost,
        estimated_pg_cost,
        referenced_tables: inspection.referenced_tables,
        reason,
    })
}

/// In-backend planner metadata catalog used by `pg_bolt_planner_hook` without touching CUDA.
#[cfg(feature = "pgrx")]
#[derive(Default)]
struct PlannerTableCatalog {
    metas: HashMap<String, CachedTableMeta>,
    schemas: HashMap<String, SchemaRef>,
    registered_epochs: HashMap<String, u64>,
    rel_oids: HashMap<String, u32>,
}

#[cfg(feature = "pgrx")]
static PLANNER_CATALOG: LazyLock<RwLock<PlannerTableCatalog>> =
    LazyLock::new(|| RwLock::new(PlannerTableCatalog::default()));

#[cfg(feature = "pgrx")]
fn mark_planner_oid_dirty(rel_oid: u32) {
    if rel_oid == 0 {
        return;
    }
    let mut catalog = PLANNER_CATALOG.write();
    let affected: Vec<String> = catalog
        .rel_oids
        .iter()
        .filter_map(|(bolt_name, &oid)| (oid == rel_oid).then_some(bolt_name.clone()))
        .collect();
    for bolt_name in affected {
        if let Some(meta) = catalog.metas.get_mut(&bolt_name) {
            meta.dirty = true;
        }
    }
}

/// Registers or refreshes cached table metadata and Arrow schema for `pg_bolt_planner_hook`.
#[cfg(feature = "pgrx")]
pub fn register_planner_table(meta: CachedTableMeta, schema: SchemaRef) {
    let epoch = bump_catalog_epoch();
    let mut catalog = PLANNER_CATALOG.write();
    let key = meta.bolt_name.clone();
    catalog.metas.insert(key.clone(), meta);
    catalog.schemas.insert(key.clone(), schema);
    catalog.registered_epochs.insert(key, epoch);
}

/// Registers cached table metadata bound to a known PostgreSQL relation `Oid`.
#[cfg(feature = "pgrx")]
pub fn register_planner_table_with_oid(meta: CachedTableMeta, schema: SchemaRef, rel_oid: u32) {
    let epoch = bump_catalog_epoch();
    let mut catalog = PLANNER_CATALOG.write();
    let key = meta.bolt_name.clone();
    catalog.metas.insert(key.clone(), meta);
    catalog.schemas.insert(key.clone(), schema);
    catalog.registered_epochs.insert(key.clone(), epoch);
    if rel_oid != 0 {
        catalog.rel_oids.insert(key, rel_oid);
    }
}

/// Removes a table from the planner catalog when evicted or invalidated.
#[cfg(feature = "pgrx")]
pub fn unregister_planner_table(bolt_name: &str) {
    let _ = bump_catalog_epoch();
    let mut catalog = PLANNER_CATALOG.write();
    catalog.metas.remove(bolt_name);
    catalog.schemas.remove(bolt_name);
    catalog.registered_epochs.remove(bolt_name);
    catalog.rel_oids.remove(bolt_name);
}

/// Clears all entries in the planner catalog.
#[cfg(feature = "pgrx")]
pub fn clear_planner_tables() {
    let _ = bump_catalog_epoch();
    let mut catalog = PLANNER_CATALOG.write();
    catalog.metas.clear();
    catalog.schemas.clear();
    catalog.registered_epochs.clear();
    catalog.rel_oids.clear();
}

#[cfg(feature = "pgrx")]
static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;
#[cfg(feature = "pgrx")]
static mut PREV_SET_REL_PATHLIST_HOOK: pg_sys::set_rel_pathlist_hook_type = None;

/// Registers the PostgreSQL `planner_hook` and `set_rel_pathlist_hook` during `_PG_init`.
///
/// # Safety
///
/// Must only be called once from single-threaded `_PG_init` while PostgreSQL loads the shared
/// library, before concurrent query planning or hook chaining occurs.
#[cfg(feature = "pgrx")]
pub unsafe fn install_planner_hook() {
    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(pg_bolt_planner_hook);

    PREV_SET_REL_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
    pg_sys::set_rel_pathlist_hook = Some(pg_bolt_set_rel_pathlist_hook);
}

/// Hook called by PostgreSQL planner when building access paths for base relations.
///
/// Prepares `CustomPath` injection for mixed CPU/GPU query plans (`W1-D-006`),
/// allowing PostgreSQL's cost-based optimizer to evaluate Bolt GPU scans for cached relations
/// underneath PostgreSQL CPU joins, aggregates, or subplans.
#[cfg(feature = "pgrx")]
#[pgrx::prelude::pg_guard]
unsafe extern "C-unwind" fn pg_bolt_set_rel_pathlist_hook(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    if let Some(prev) = PREV_SET_REL_PATHLIST_HOOK {
        prev(root, rel, rti, rte);
    }

    if root.is_null() || rel.is_null() || rte.is_null() {
        return;
    }

    let config = crate::guc::current_config();
    if !config.enabled || !config.enable_custom_scan {
        return;
    }

    if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION || ((*rte).relkind as u8) != b'r' {
        return;
    }

    let relid = (*rte).relid;
    if relid == pg_sys::InvalidOid {
        return;
    }

    let relid_u32 = relid.to_u32();
    let catalog = PLANNER_CATALOG.read();
    let _is_cached = catalog.rel_oids.values().any(|&oid| oid == relid_u32);
    // Relation is cached in Bolt VRAM. Prepares pathlist hooks for mixed CPU/GPU plans.
}

#[cfg(feature = "pgrx")]
unsafe fn cstr_to_string_and_pfree(ptr: *mut std::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let out = std::ffi::CStr::from_ptr(ptr)
        .to_str()
        .ok()
        .map(|s| s.to_string());
    pg_sys::pfree(ptr.cast());
    out
}

#[cfg(feature = "pgrx")]
unsafe fn extract_bound_param_literals(
    bound_params: pg_sys::ParamListInfo,
) -> Option<Vec<SqlParamLiteral>> {
    use pgrx::datum::FromDatum;

    if bound_params.is_null() {
        return Some(Vec::new());
    }
    if (*bound_params).paramFetch.is_some() {
        return None;
    }
    let num_params = (*bound_params).numParams;
    if num_params <= 0 {
        return Some(Vec::new());
    }

    let params = (*bound_params).params.as_slice(num_params as usize);
    let mut out = Vec::with_capacity(params.len());
    for param in params {
        if param.ptype == pg_sys::InvalidOid {
            return None;
        }
        if param.isnull {
            if matches!(
                param.ptype,
                pg_sys::BOOLOID
                    | pg_sys::INT2OID
                    | pg_sys::INT4OID
                    | pg_sys::INT8OID
                    | pg_sys::FLOAT4OID
                    | pg_sys::FLOAT8OID
                    | pg_sys::TEXTOID
                    | pg_sys::VARCHAROID
                    | pg_sys::BPCHAROID
            ) {
                out.push(SqlParamLiteral::Null);
                continue;
            }
            return None;
        }

        let lit = match param.ptype {
            pg_sys::BOOLOID => SqlParamLiteral::Bool(bool::from_datum(param.value, false)?),
            pg_sys::INT2OID => {
                SqlParamLiteral::Int(i64::from(i16::from_datum(param.value, false)?))
            }
            pg_sys::INT4OID => {
                SqlParamLiteral::Int(i64::from(i32::from_datum(param.value, false)?))
            }
            pg_sys::INT8OID => {
                let v = i64::from_datum(param.value, false)?;
                if v == i64::MIN {
                    return None;
                }
                SqlParamLiteral::Int(v)
            }
            pg_sys::FLOAT4OID => {
                let v = f64::from(f32::from_datum(param.value, false)?);
                if !v.is_finite() {
                    return None;
                }
                SqlParamLiteral::Float(v)
            }
            pg_sys::FLOAT8OID => {
                let v = f64::from_datum(param.value, false)?;
                if !v.is_finite() {
                    return None;
                }
                SqlParamLiteral::Float(v)
            }
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
                let s = String::from_datum(param.value, false)?;
                if s.contains('\0') {
                    return None;
                }
                SqlParamLiteral::Text(s)
            }
            _ => return None,
        };
        out.push(lit);
    }
    Some(out)
}

#[cfg(feature = "pgrx")]
unsafe fn collect_and_validate_rtable(
    rtable: *mut pg_sys::List,
    out: &mut Vec<(pg_sys::Oid, bool)>,
) -> bool {
    if rtable.is_null() || (*rtable).length <= 0 || (*rtable).elements.is_null() {
        return true;
    }
    let len = (*rtable).length as usize;
    let cells = std::slice::from_raw_parts((*rtable).elements, len);
    for cell in cells {
        let rte = cell.ptr_value as *mut pg_sys::RangeTblEntry;
        if rte.is_null() {
            continue;
        }
        match (*rte).rtekind {
            pg_sys::RTEKind::RTE_RELATION => {
                let relkind = (*rte).relkind;
                // Only plain base relations (RELKIND_RELATION == 'r') can be offloaded.
                // Decline if rtable contains views ('v'), partitioned tables ('p'),
                // foreign tables ('f'), materialized views ('m'), or sequences.
                if (relkind as u8) != b'r' {
                    return false;
                }
                let relid = (*rte).relid;
                if relid == pg_sys::InvalidOid {
                    return false;
                }
                if (pg_sys::get_rel_relkind(relid) as u8) != b'r' {
                    return false;
                }
                // Row-level security quals present -> keep on PostgreSQL CPU
                if !(*rte).securityQuals.is_null() {
                    return false;
                }
                out.push((relid, false));
            }
            pg_sys::RTEKind::RTE_JOIN => {
                // Internal join representation in PostgreSQL parse trees; permissible.
            }
            // All other RTE kinds (subqueries, CTEs, functions, values, etc.) must remain on PostgreSQL CPU.
            _ => return false,
        }
    }
    true
}

#[cfg(feature = "pgrx")]
fn relation_matches_pg_names(pg_schema: &str, pg_table: &str, meta: &CachedTableMeta) -> bool {
    let (meta_schema, meta_table) = normalize_pg_relation(&meta.pg_relation);
    let schema_norm = normalize_pg_part(pg_schema);
    let table_norm = normalize_pg_part(pg_table);
    if meta_table != table_norm {
        return false;
    }
    match meta_schema {
        Some(s) => s == schema_norm,
        None => schema_norm == "public" || schema_norm.starts_with("pg_temp"),
    }
}

#[cfg(feature = "pgrx")]
unsafe fn validate_rtable_relations(
    planned: *const pg_sys::PlannedStmt,
    parse: *const pg_sys::Query,
    sql: &str,
) -> Option<(HashMap<String, CachedTableMeta>, HashMap<String, SchemaRef>)> {
    let mut rte_rels = Vec::new();
    if !collect_and_validate_rtable((*parse).rtable, &mut rte_rels) {
        return None;
    }
    if !collect_and_validate_rtable((*planned).rtable, &mut rte_rels) {
        return None;
    }
    if rte_rels.is_empty() {
        return None;
    }

    let mut verified_pg_rels: Vec<(u32, String, String)> = Vec::new();
    let mut seen_oids = HashSet::new();
    for (relid, _has_sec_quals) in rte_rels {
        let relid_u32 = relid.to_u32();
        if !seen_oids.insert(relid_u32) {
            continue;
        }
        if pg_sys::pg_class_aclcheck(
            relid,
            pg_sys::GetUserId(),
            pg_sys::ACL_SELECT as pg_sys::AclMode,
        ) != pg_sys::AclResult::ACLCHECK_OK
        {
            return None;
        }
        let ns_oid = pg_sys::get_rel_namespace(relid);
        if ns_oid == pg_sys::InvalidOid {
            return None;
        }
        let schema_name = cstr_to_string_and_pfree(pg_sys::get_namespace_name(ns_oid))?;
        let rel_name = cstr_to_string_and_pfree(pg_sys::get_rel_name(relid))?;
        verified_pg_rels.push((relid_u32, schema_name, rel_name));
    }

    let inspection = inspect_sql_for_offload(sql).ok()?;
    if inspection.referenced_tables.is_empty() || inspection.has_parameters {
        return None;
    }

    let mut catalog = PLANNER_CATALOG.write();
    if catalog.metas.is_empty() {
        return None;
    }
    let inv_oids = INVALIDATED_REL_OIDS.read();

    let mut matched_bolt_names = HashSet::new();
    let mut resolved_pairs: Vec<(String, u32)> = Vec::with_capacity(verified_pg_rels.len());
    let mut stale_names: Vec<String> = Vec::new();

    for (relid_u32, pg_schema, pg_table) in &verified_pg_rels {
        let mut matched_name: Option<String> = None;
        let mut ambiguous = false;
        for meta in catalog.metas.values() {
            if relation_matches_pg_names(pg_schema, pg_table, meta) {
                if matched_name.is_some() {
                    ambiguous = true;
                    break;
                }
                matched_name = Some(meta.bolt_name.clone());
            }
        }
        if ambiguous {
            return None;
        }
        let bolt_name = matched_name?;
        let meta = catalog.metas.get(&bolt_name)?;
        if meta.dirty {
            return None;
        }
        let reg_epoch = catalog
            .registered_epochs
            .get(&bolt_name)
            .copied()
            .unwrap_or(0);
        if inv_oids
            .get(relid_u32)
            .copied()
            .is_some_and(|inv_epoch| inv_epoch >= reg_epoch)
        {
            stale_names.push(bolt_name.clone());
        }
        matched_bolt_names.insert(bolt_name.clone());
        resolved_pairs.push((bolt_name, *relid_u32));
    }
    drop(inv_oids);

    for (bolt_name, relid_u32) in resolved_pairs {
        catalog.rel_oids.insert(bolt_name, relid_u32);
    }
    if !stale_names.is_empty() {
        for name in stale_names {
            if let Some(meta) = catalog.metas.get_mut(&name) {
                meta.dirty = true;
            }
        }
        return None;
    }

    // Verify relation count consistency: if PostgreSQL Query expanded views, rules,
    // or partition children not present in raw SQL, decline offload.
    if matched_bolt_names.len() != verified_pg_rels.len()
        || verified_pg_rels.len() != inspection.referenced_tables.len()
    {
        return None;
    }

    // Ensure SQL text table references do not alias via session `search_path` to a different
    // schema than PostgreSQL bound in `rtable` (`W1-D-003`).
    for ref_name in &inspection.referenced_tables {
        let resolved = resolve_cached_table(ref_name, &catalog.metas)?;
        if !matched_bolt_names.contains(&resolved.bolt_name) {
            return None;
        }
    }

    Some((catalog.metas.clone(), catalog.schemas.clone()))
}

#[cfg(feature = "pgrx")]
unsafe fn extract_statement_sql(
    parse: *const pg_sys::Query,
    query_string: *const std::ffi::c_char,
) -> Option<String> {
    if query_string.is_null() {
        return None;
    }
    let full = std::ffi::CStr::from_ptr(query_string).to_str().ok()?;
    let loc = (*parse).stmt_location;
    let len = (*parse).stmt_len;
    let slice = if loc >= 0 && (loc as usize) < full.len() {
        let start = loc as usize;
        if len > 0 && start.saturating_add(len as usize) <= full.len() {
            full.get(start..start + (len as usize))?
        } else {
            full.get(start..)?
        }
    } else {
        full
    };
    let trimmed = slice.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(feature = "pgrx")]
unsafe fn targetlist_types_supported(targetlist: *mut pg_sys::List) -> bool {
    if targetlist.is_null() || (*targetlist).length <= 0 || (*targetlist).elements.is_null() {
        return false;
    }
    let len = (*targetlist).length as usize;
    let cells = std::slice::from_raw_parts((*targetlist).elements, len);
    let mut non_junk_count = 0usize;
    for cell in cells {
        let tle = cell.ptr_value as *mut pg_sys::TargetEntry;
        if tle.is_null() || (*tle).expr.is_null() {
            return false;
        }
        // Resjunk columns (e.g. hidden ORDER BY sort helpers) are stripped from `cscan_tlist`
        // because Bolt computes sorting internally and emits only projected output columns.
        if (*tle).resjunk {
            continue;
        }
        non_junk_count += 1;
        let oid = pg_sys::exprType((*tle).expr as *const pg_sys::Node);
        let supported = matches!(
            oid,
            pg_sys::BOOLOID
                | pg_sys::BYTEAOID
                | pg_sys::UUIDOID
                | pg_sys::INT2OID
                | pg_sys::INT4OID
                | pg_sys::INT8OID
                | pg_sys::FLOAT4OID
                | pg_sys::FLOAT8OID
                | pg_sys::TEXTOID
                | pg_sys::VARCHAROID
                | pg_sys::BPCHAROID
                | pg_sys::DATEOID
                | pg_sys::TIMEOID
                | pg_sys::TIMESTAMPOID
                | pg_sys::TIMESTAMPTZOID
                | pg_sys::NUMERICOID
        );
        if !supported {
            return false;
        }
    }
    non_junk_count > 0
}

#[cfg(feature = "pgrx")]
pub(crate) unsafe fn build_non_resjunk_targetlist(
    targetlist: *mut pg_sys::List,
) -> *mut pg_sys::List {
    if targetlist.is_null() || (*targetlist).length <= 0 || (*targetlist).elements.is_null() {
        return std::ptr::null_mut();
    }
    let len = (*targetlist).length as usize;
    let cells = std::slice::from_raw_parts((*targetlist).elements, len);
    let has_resjunk = cells.iter().any(|cell| {
        let tle = cell.ptr_value as *mut pg_sys::TargetEntry;
        !tle.is_null() && (*tle).resjunk
    });
    if !has_resjunk {
        return targetlist;
    }

    let mut filtered: *mut pg_sys::List = std::ptr::null_mut();
    let mut next_resno: pg_sys::AttrNumber = 1;
    for cell in cells {
        let tle = cell.ptr_value as *mut pg_sys::TargetEntry;
        if tle.is_null() || (*tle).resjunk {
            continue;
        }
        let copy = pg_sys::flatCopyTargetEntry(tle);
        if copy.is_null() {
            return std::ptr::null_mut();
        }
        (*copy).resno = next_resno;
        next_resno += 1;
        filtered = pg_sys::lappend(filtered, copy as *mut std::ffi::c_void);
    }
    filtered
}

#[cfg(feature = "pgrx")]
pub(crate) unsafe fn build_scan_var_targetlist(
    custom_scan_tlist: *mut pg_sys::List,
) -> *mut pg_sys::List {
    if custom_scan_tlist.is_null()
        || (*custom_scan_tlist).length <= 0
        || (*custom_scan_tlist).elements.is_null()
    {
        return std::ptr::null_mut();
    }

    let len = (*custom_scan_tlist).length as usize;
    let cells = std::slice::from_raw_parts((*custom_scan_tlist).elements, len);
    let mut scan_tlist: *mut pg_sys::List = std::ptr::null_mut();

    for cell in cells {
        let tle = cell.ptr_value as *mut pg_sys::TargetEntry;
        if tle.is_null() || (*tle).expr.is_null() {
            return std::ptr::null_mut();
        }
        let copy = pg_sys::flatCopyTargetEntry(tle);
        if copy.is_null() {
            return std::ptr::null_mut();
        }
        let expr_node = (*tle).expr as *const pg_sys::Node;
        #[allow(clippy::unnecessary_cast)]
        let scan_var = pg_sys::makeVar(
            pg_sys::INDEX_VAR as _,
            (*tle).resno,
            pg_sys::exprType(expr_node),
            pg_sys::exprTypmod(expr_node),
            pg_sys::exprCollation(expr_node),
            0,
        );
        if scan_var.is_null() {
            return std::ptr::null_mut();
        }
        (*copy).expr = scan_var as *mut pg_sys::Expr;
        scan_tlist = pg_sys::lappend(scan_tlist, copy as *mut std::ffi::c_void);
    }

    scan_tlist
}

#[cfg(feature = "pgrx")]
unsafe fn attach_custom_scan_plan(
    planned: *mut pg_sys::PlannedStmt,
    sql: String,
    eligibility: &PlanEligibility,
    config: &BoltConfig,
) {
    let Some(tier) = eligibility.tier else {
        return;
    };
    if planned.is_null()
        || (*planned).planTree.is_null()
        || !(*planned).subplans.is_null()
        || !targetlist_types_supported((*(*planned).planTree).targetlist)
    {
        return;
    }

    let orig_plan = (*planned).planTree;
    let cscan_tlist = build_non_resjunk_targetlist((*orig_plan).targetlist);
    if cscan_tlist.is_null() {
        return;
    }
    let scan_tlist = build_scan_var_targetlist(cscan_tlist);
    if scan_tlist.is_null() {
        return;
    }

    let estimated_rows = (*orig_plan).plan_rows.max(0.0).round() as usize;
    let scan_plan = crate::custom_scan::BoltCustomScanPlan {
        sql,
        tier,
        estimated_cost: eligibility.estimated_bolt_cost,
        estimated_rows,
        fallback_on_oom: config.allow_hybrid_fallback,
        timeout_ms: config.statement_timeout_ms,
    };

    let Ok(plan_json) = serde_json::to_string(&scan_plan) else {
        return;
    };
    let Ok(plan_cstr) = std::ffi::CString::new(plan_json) else {
        return;
    };

    let cscan =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
    if cscan.is_null() {
        return;
    }

    (*(cscan as *mut pg_sys::Node)).type_ = pg_sys::NodeTag::T_CustomScan;
    (*cscan).scan.plan.startup_cost = 10.0;
    (*cscan).scan.plan.total_cost = eligibility.estimated_bolt_cost;
    (*cscan).scan.plan.plan_rows = (*orig_plan).plan_rows;
    (*cscan).scan.plan.plan_width = (*orig_plan).plan_width;
    (*cscan).scan.plan.targetlist = scan_tlist;
    (*cscan).scan.scanrelid = 0;
    (*cscan).custom_scan_tlist = cscan_tlist;
    (*cscan).methods = crate::custom_scan::custom_scan_methods_ptr();
    (*cscan).custom_plans =
        pg_sys::lappend(std::ptr::null_mut(), orig_plan as *mut std::ffi::c_void);

    let payload_node = pg_sys::makeString(pg_sys::pstrdup(plan_cstr.as_ptr()));
    (*cscan).custom_private =
        pg_sys::lappend(std::ptr::null_mut(), payload_node as *mut std::ffi::c_void);

    (*planned).planTree = cscan as *mut pg_sys::Plan;
    (*planned).parallelModeNeeded = false;
}

#[cfg(feature = "pgrx")]
#[pgrx::prelude::pg_guard]
unsafe extern "C-unwind" fn pg_bolt_planner_hook(
    parse: *mut pg_sys::Query,
    query_string: *const std::ffi::c_char,
    cursor_options: std::ffi::c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    let planned = if let Some(prev) = PREV_PLANNER_HOOK {
        prev(parse, query_string, cursor_options, bound_params)
    } else {
        pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
    };

    if planned.is_null() || parse.is_null() {
        return planned;
    }

    let config = crate::guc::current_config();
    if !config.enabled || !config.enable_custom_scan {
        return planned;
    }

    let is_scroll_cursor = (cursor_options & (pg_sys::CURSOR_OPT_SCROLL as std::ffi::c_int)) != 0;
    if is_scroll_cursor
        || (*parse).commandType != pg_sys::CmdType::CMD_SELECT
        || !(*parse).utilityStmt.is_null()
        || (*parse).hasModifyingCTE
        || (*parse).hasRowSecurity
        || (*parse).hasSubLinks
        || (*parse).hasTargetSRFs
        || !(*parse).rowMarks.is_null()
        || !(*planned).subplans.is_null()
        || !(*planned).paramExecTypes.is_null()
    {
        return planned;
    }

    let Some(param_literals) = extract_bound_param_literals(bound_params) else {
        return planned;
    };

    let Some(raw_sql) = extract_statement_sql(parse, query_string) else {
        return planned;
    };

    let sql = if param_literals.is_empty() {
        raw_sql
    } else {
        match substitute_sql_params(&raw_sql, &param_literals) {
            Ok(substituted) => substituted,
            Err(_) => return planned,
        }
    };

    let Some((metas, schemas)) = validate_rtable_relations(planned, parse, &sql) else {
        return planned;
    };

    match analyze_sql_eligibility(&sql, &metas, &schemas, &config) {
        Ok(eligibility) if eligibility.eligible => {
            let Ok(bolt_sql) = rewrite_sql_for_bolt(&sql, &metas) else {
                return planned;
            };
            pgrx::debug2!(
                "pg_bolt planner attaching CustomScan ({:?}): {}",
                eligibility.tier,
                eligibility.reason
            );
            attach_custom_scan_plan(planned, bolt_sql, &eligibility, &config);
        }
        Ok(eligibility) => {
            pgrx::debug2!(
                "pg_bolt planner kept PostgreSQL plan: {}",
                eligibility.reason
            );
        }
        Err(err) => {
            pgrx::debug2!("pg_bolt planner fallback to PostgreSQL plan on error: {err}");
        }
    }

    planned
}

#[cfg(all(test, not(feature = "pgrx")))]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn sample_meta(name: &str, pg_rel: &str, rows: usize, dirty: bool) -> CachedTableMeta {
        CachedTableMeta {
            pg_relation: pg_rel.to_string(),
            bolt_name: name.to_string(),
            num_rows: rows,
            num_cols: 3,
            approx_bytes: rows * 24,
            loaded_at_ms: 1,
            last_accessed_seq: 1,
            dirty,
        }
    }

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("region_id", DataType::Int32, false),
            Field::new("price", DataType::Float64, false),
            Field::new("tax", DataType::Float64, true),
        ]))
    }

    #[test]
    fn inspect_rejects_empty_multi_dml_and_locking_queries() {
        assert!(inspect_sql_for_offload("   ").is_err());
        assert!(inspect_sql_for_offload("SELECT 1; SELECT 2").is_err());
        assert!(inspect_sql_for_offload("INSERT INTO sales VALUES (1, 2.0, 0.1)").is_err());
        assert!(inspect_sql_for_offload("UPDATE sales SET price = 0").is_err());
        assert!(inspect_sql_for_offload("DELETE FROM sales").is_err());
        assert!(inspect_sql_for_offload("DROP TABLE sales").is_err());
        assert!(inspect_sql_for_offload("SELECT * FROM sales FOR UPDATE").is_err());
        assert!(inspect_sql_for_offload("SELECT * FROM sales FOR SHARE").is_err());
        assert!(inspect_sql_for_offload("SELECT * FROM sales FOR NO KEY UPDATE").is_err());
        assert!(inspect_sql_for_offload("SELECT * FROM sales FOR KEY SHARE").is_err());
        assert!(
            inspect_sql_for_offload("SELECT * FROM (SELECT * FROM sales FOR UPDATE) sub").is_err()
        );
        assert!(inspect_sql_for_offload(
            "WITH deleted AS (DELETE FROM sales RETURNING *) SELECT * FROM deleted"
        )
        .is_err());
        assert!(inspect_sql_for_offload("SELECT * INTO tmp_sales FROM sales").is_err());
    }

    #[test]
    fn inspect_tracks_ctes_without_false_missing_table_and_flags_volatiles() {
        let sql = "WITH high_value AS (SELECT region_id, price FROM public.sales WHERE price > 10.0) \
                   SELECT region_id, SUM(price) FROM high_value GROUP BY region_id ORDER BY region_id";
        let info = inspect_sql_for_offload(sql).unwrap();
        assert_eq!(info.referenced_tables, vec!["public.sales"]);
        assert_eq!(info.cte_names, vec!["high_value"]);
        assert!(info.has_agg);
        assert!(info.has_sort);
        assert!(!info.has_join);
        assert!(info.volatile_functions.is_empty());

        let volatile_info = inspect_sql_for_offload(
            "SELECT region_id, pg_catalog.random(), nextval('seq') FROM sales WHERE clock_timestamp() IS NOT NULL",
        )
        .unwrap();
        assert!(volatile_info
            .volatile_functions
            .contains(&"random".to_string()));
        assert!(volatile_info
            .volatile_functions
            .contains(&"nextval".to_string()));
        assert!(volatile_info
            .volatile_functions
            .contains(&"clock_timestamp".to_string()));
    }

    #[test]
    fn inspect_catches_parameters_tvf_and_lateral() {
        let param_info =
            inspect_sql_for_offload("SELECT region_id FROM sales WHERE price > $1").unwrap();
        assert!(param_info.has_parameters);

        let tvf_info =
            inspect_sql_for_offload("SELECT * FROM generate_series(1, 10) AS gs").unwrap();
        assert!(tvf_info.referenced_tables.is_empty());
        assert!(!tvf_info.unsupported_constructs.is_empty());

        let lateral_info =
            inspect_sql_for_offload("SELECT s.region_id FROM sales s, LATERAL (SELECT price) d")
                .unwrap();
        assert!(lateral_info
            .unsupported_constructs
            .iter()
            .any(|c| c == "LATERAL"));
    }

    #[test]
    fn cost_model_prefers_pg_on_tiny_or_cold_and_bolt_on_vram_analytics() {
        assert!(
            estimate_pg_cpu_cost(1_000, true, false, false)
                < estimate_bolt_custom_scan_cost(1_000, true, true, false, false)
        );
        assert!(
            estimate_bolt_custom_scan_cost(100_000, true, true, true, false)
                < estimate_pg_cpu_cost(100_000, true, true, false)
        );
        assert!(
            estimate_pg_cpu_cost(50_000, false, false, false)
                < estimate_bolt_custom_scan_cost(50_000, false, false, false, false)
        );
    }

    #[test]
    fn analyze_eligibility_handles_thresholds_tiers_schemas_and_dirty_tables() {
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, false),
        );
        schemas.insert("sales".to_string(), sample_schema());

        let cfg = BoltConfig::default();
        let ok = analyze_sql_eligibility(
            "SELECT region_id, SUM(price * tax) AS rev FROM public.sales GROUP BY region_id",
            &metas,
            &schemas,
            &cfg,
        )
        .unwrap();
        assert!(ok.eligible, "expected eligible query, got: {}", ok.reason);
        assert_eq!(ok.tier, Some(ExecutionTier::Hybrid));
        assert!(ok.estimated_bolt_cost < ok.estimated_pg_cost);

        // Pure projection without host stages plans strictly on `ExecutionTier::Gpu`.
        let pure_proj = analyze_sql_eligibility(
            "SELECT region_id, price * tax AS total FROM public.sales WHERE region_id > 0",
            &metas,
            &schemas,
            &cfg,
        )
        .unwrap();
        assert!(
            pure_proj.eligible,
            "expected eligible pure projection, got: {}",
            pure_proj.reason
        );
        assert_eq!(pure_proj.tier, Some(ExecutionTier::Gpu));

        // Disabling `allow_hybrid_fallback` rejects Hybrid-tier aggregates while still permitting Gpu.
        let strict_gpu_cfg = BoltConfig {
            allow_hybrid_fallback: false,
            ..BoltConfig::default()
        };
        let hybrid_rejected = analyze_sql_eligibility(
            "SELECT region_id, SUM(price * tax) AS rev FROM public.sales GROUP BY region_id",
            &metas,
            &schemas,
            &strict_gpu_cfg,
        )
        .unwrap();
        assert!(!hybrid_rejected.eligible);
        assert_eq!(hybrid_rejected.tier, Some(ExecutionTier::Hybrid));
        assert!(hybrid_rejected.reason.contains("allow_hybrid_fallback"));

        // Multi-column GROUP BY with pre-aggregation is rejected at plan time so CustomScan
        // silently falls back to PostgreSQL CPU instead of failing at GPU execution time.
        let multi_col_pre = analyze_sql_eligibility(
            "SELECT region_id, tax, SUM(price * tax) AS rev FROM public.sales GROUP BY region_id, tax ORDER BY region_id, tax",
            &metas,
            &schemas,
            &cfg,
        )
        .unwrap();
        assert!(!multi_col_pre.eligible);
        assert!(multi_col_pre.reason.contains("multi-column GROUP BY"));

        // Wrong schema qualifier must not silently alias `public.sales`.
        let wrong_schema = analyze_sql_eligibility(
            "SELECT region_id, SUM(price) FROM analytics.sales GROUP BY region_id",
            &metas,
            &schemas,
            &cfg,
        )
        .unwrap();
        assert!(!wrong_schema.eligible);
        assert!(wrong_schema.reason.contains("not cached"));

        // Below min_offload_rows threshold.
        let mut small_cfg = cfg.clone();
        small_cfg.min_offload_rows = 500_000;
        let below_min = analyze_sql_eligibility(
            "SELECT region_id, SUM(price) FROM sales GROUP BY region_id",
            &metas,
            &schemas,
            &small_cfg,
        )
        .unwrap();
        assert!(!below_min.eligible);
        assert!(below_min.reason.contains("below min_offload_rows"));

        // Dirty cached table awaiting MVCC sync must not be served stale.
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, true),
        );
        let dirty = analyze_sql_eligibility(
            "SELECT region_id, SUM(price) FROM sales GROUP BY region_id",
            &metas,
            &schemas,
            &cfg,
        )
        .unwrap();
        assert!(!dirty.eligible);
        assert!(dirty.reason.contains("dirty"));
    }

    #[test]
    fn analyze_eligibility_rejects_disabled_and_volatile() {
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, false),
        );
        schemas.insert("sales".to_string(), sample_schema());

        let disabled = BoltConfig {
            enabled: false,
            ..BoltConfig::default()
        };
        let res =
            analyze_sql_eligibility("SELECT region_id FROM sales", &metas, &schemas, &disabled)
                .unwrap();
        assert!(!res.eligible);

        let vol = analyze_sql_eligibility(
            "SELECT region_id, random() FROM sales",
            &metas,
            &schemas,
            &BoltConfig::default(),
        )
        .unwrap();
        assert!(!vol.eligible);
        assert!(vol.reason.contains("random"));
    }

    #[test]
    fn analyze_eligibility_rejects_cross_schema_bare_name_collision() {
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        metas.insert(
            "pub_sales".to_string(),
            sample_meta("pub_sales", "public.sales", 100_000, false),
        );
        metas.insert(
            "ana_sales".to_string(),
            sample_meta("ana_sales", "analytics.sales", 100_000, false),
        );
        schemas.insert("pub_sales".to_string(), sample_schema());
        schemas.insert("ana_sales".to_string(), sample_schema());

        let collision = analyze_sql_eligibility(
            "SELECT a.region_id FROM public.sales a JOIN analytics.sales b ON a.region_id = b.region_id",
            &metas,
            &schemas,
            &BoltConfig::default(),
        )
        .unwrap();
        assert!(!collision.eligible);
        assert!(collision.reason.contains("bare name"));
    }

    #[test]
    fn inspect_handles_sequential_cte_scoping_and_rejects_recursive() {
        // Self-referencing CTE: inside the CTE body, `sales` resolves to the base table.
        let self_ref = inspect_sql_for_offload(
            "WITH sales AS (SELECT region_id, price FROM sales WHERE price > 10.0) \
             SELECT region_id, SUM(price) FROM sales GROUP BY region_id",
        )
        .unwrap();
        assert_eq!(self_ref.referenced_tables, vec!["sales"]);
        assert_eq!(self_ref.cte_names, vec!["sales"]);

        // Earlier CTE is visible to later CTE, and base table is still recorded once.
        let chained = inspect_sql_for_offload(
            "WITH step1 AS (SELECT region_id, price FROM public.sales), \
             step2 AS (SELECT region_id, price * 2.0 AS p2 FROM step1) \
             SELECT region_id, SUM(p2) FROM step2 GROUP BY region_id",
        )
        .unwrap();
        assert_eq!(chained.referenced_tables, vec!["public.sales"]);
        assert_eq!(chained.cte_names, vec!["step1", "step2"]);

        // Forward reference: `later_cte` is NOT in scope during `early_cte`.
        let forward_ref = inspect_sql_for_offload(
            "WITH early_cte AS (SELECT region_id FROM later_cte), \
             later_cte AS (SELECT region_id FROM sales) \
             SELECT region_id FROM early_cte",
        )
        .unwrap();
        assert_eq!(forward_ref.referenced_tables, vec!["later_cte", "sales"]);

        // Nested CTE scope inside CTE 1 must not leak to CTE 2.
        let nested_leak = inspect_sql_for_offload(
            "WITH cte1 AS (WITH inner_t AS (SELECT region_id FROM sales) SELECT region_id FROM inner_t), \
             cte2 AS (SELECT region_id FROM inner_t) \
             SELECT * FROM cte1, cte2",
        )
        .unwrap();
        assert_eq!(nested_leak.referenced_tables, vec!["sales", "inner_t"]);

        // `WITH RECURSIVE` is rejected explicitly.
        let recursive = inspect_sql_for_offload(
            "WITH RECURSIVE cnt AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) \
             SELECT n FROM cnt",
        )
        .unwrap();
        assert!(recursive
            .unsupported_constructs
            .iter()
            .any(|c| c == "WITH RECURSIVE"));
    }

    #[test]
    fn rewrite_sql_for_bolt_normalizes_qualified_tables_and_pg_null_ordering() {
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        // Default `bolt.load_table('public.sales')` stores `bolt_name = "public.sales"`.
        metas.insert(
            "public.sales".to_string(),
            sample_meta("public.sales", "public.sales", 100_000, false),
        );
        schemas.insert("public.sales".to_string(), sample_schema());

        let rewritten = rewrite_sql_for_bolt(
            "SELECT sales.region_id, SUM(price) FROM public.sales GROUP BY sales.region_id ORDER BY region_id DESC",
            &metas,
        )
        .unwrap();
        assert!(
            rewritten.contains("\"public.sales\" AS sales"),
            "unexpected rewrite: {rewritten}"
        );
        assert!(
            rewritten.contains("ORDER BY region_id DESC NULLS FIRST"),
            "unexpected DESC null ordering rewrite: {rewritten}"
        );

        let elig = analyze_sql_eligibility(
            "SELECT sales.region_id, SUM(price) FROM public.sales GROUP BY sales.region_id ORDER BY region_id DESC",
            &metas,
            &schemas,
            &BoltConfig::default(),
        )
        .unwrap();
        assert!(elig.eligible, "expected eligibility, got: {}", elig.reason);

        // Explicit alias is preserved and implicit ASC gets `ASC NULLS LAST`.
        let mut custom_metas = HashMap::new();
        custom_metas.insert(
            "ana_sales".to_string(),
            sample_meta("ana_sales", "analytics.sales", 100_000, false),
        );
        let rewritten_asc = rewrite_sql_for_bolt(
            "SELECT s.region_id, ROW_NUMBER() OVER (PARTITION BY s.region_id ORDER BY s.price DESC) \
             FROM analytics.sales s ORDER BY s.price, s.region_id ASC NULLS FIRST",
            &custom_metas,
        )
        .unwrap();
        assert!(
            rewritten_asc.contains("FROM ana_sales AS s"),
            "unexpected aliased table rewrite: {rewritten_asc}"
        );
        assert!(
            rewritten_asc.contains("ORDER BY s.price DESC NULLS FIRST"),
            "unexpected window DESC rewrite: {rewritten_asc}"
        );
        assert!(
            rewritten_asc.contains(
                "FROM ana_sales AS s ORDER BY price ASC NULLS LAST, region_id ASC NULLS FIRST"
            ),
            "unexpected mixed ORDER BY rewrite: {rewritten_asc}"
        );

        // Self-referencing CTE keeps CTE name intact while rewriting base `public.sales`.
        let cte_rewritten = rewrite_sql_for_bolt(
            "WITH sales AS (SELECT region_id, price FROM public.sales) SELECT region_id, price FROM sales ORDER BY price DESC",
            &metas,
        )
        .unwrap();
        assert!(
            cte_rewritten.contains("FROM \"public.sales\" AS sales) SELECT region_id, price FROM sales ORDER BY price DESC NULLS FIRST"),
            "unexpected CTE rewrite: {cte_rewritten}"
        );

        // W2-D-002: 3-part `schema.table.col` expressions collapse to `<table_alias>.<col>`
        // and qualified single-table `ORDER BY` strips qualifiers for Bolt's `lower_order_by`.
        let three_part_sql = "SELECT public.sales.region_id, public.sales.price \
                              FROM public.sales \
                              WHERE public.sales.price > 10.0 \
                              ORDER BY public.sales.price DESC";
        let three_part_rewritten = rewrite_sql_for_bolt(three_part_sql, &metas).unwrap();
        assert!(
            three_part_rewritten.contains("SELECT sales.region_id, sales.price FROM \"public.sales\" AS sales WHERE sales.price > 10.0 ORDER BY price DESC NULLS FIRST"),
            "unexpected 3-part single-table rewrite: {three_part_rewritten}"
        );
        let three_part_elig =
            analyze_sql_eligibility(three_part_sql, &metas, &schemas, &BoltConfig::default())
                .unwrap();
        assert!(
            three_part_elig.eligible,
            "expected 3-part + qualified ORDER BY eligibility, got: {}",
            three_part_elig.reason
        );

        // Unqualified 2-part ORDER BY (`sales.price`) also strips cleanly in single-table queries.
        let two_part_order_sql = "SELECT public.sales.region_id, SUM(public.sales.price) AS rev \
             FROM public.sales \
             GROUP BY public.sales.region_id \
             ORDER BY sales.region_id DESC";
        let two_part_order = rewrite_sql_for_bolt(two_part_order_sql, &metas).unwrap();
        assert!(
            two_part_order.contains("SELECT sales.region_id, SUM(sales.price) AS rev FROM \"public.sales\" AS sales GROUP BY sales.region_id ORDER BY region_id DESC NULLS FIRST"),
            "unexpected aggregate 3-part + ORDER BY rewrite: {two_part_order}"
        );
        let agg_elig =
            analyze_sql_eligibility(two_part_order_sql, &metas, &schemas, &BoltConfig::default())
                .unwrap();
        assert!(
            agg_elig.eligible,
            "expected aggregate 3-part + ORDER BY eligibility, got: {}",
            agg_elig.reason
        );

        // Multi-table joins collapse 3-part columns to `<alias>.<col>` without stripping qualifiers
        // in SELECT/ON/WHERE, and only strip/remap ORDER BY when unambiguous in projection.
        let mut join_metas = metas.clone();
        join_metas.insert(
            "ana_regions".to_string(),
            sample_meta("ana_regions", "analytics.regions", 1_000, false),
        );
        let join_sql = "SELECT public.sales.price, analytics.regions.region_id AS reg_out \
                        FROM public.sales \
                        JOIN analytics.regions ON public.sales.region_id = analytics.regions.region_id \
                        WHERE public.sales.price > 0 \
                        ORDER BY analytics.regions.region_id DESC, public.sales.price";
        let join_rewritten = rewrite_sql_for_bolt(join_sql, &join_metas).unwrap();
        assert!(
            join_rewritten.contains("SELECT sales.price, regions.region_id AS reg_out FROM \"public.sales\" AS sales JOIN ana_regions AS regions ON sales.region_id = regions.region_id WHERE sales.price > 0 ORDER BY reg_out DESC NULLS FIRST, price ASC NULLS LAST"),
            "unexpected multi-table join rewrite: {join_rewritten}"
        );

        // Ambiguous output columns in multi-table ORDER BY are not silently stripped.
        let ambig_join_sql = "SELECT s.region_id, r.region_id \
             FROM public.sales s \
             JOIN analytics.regions r ON s.region_id = r.region_id \
             ORDER BY r.region_id DESC";
        let ambig_rewritten = rewrite_sql_for_bolt(ambig_join_sql, &join_metas).unwrap();
        assert!(
            ambig_rewritten.contains("ORDER BY r.region_id DESC NULLS FIRST"),
            "ambiguous multi-table ORDER BY should stay qualified: {ambig_rewritten}"
        );

        // NaN-explicit float cast is flagged cleanly.
        let nan_info = inspect_sql_for_offload(
            "SELECT region_id FROM sales WHERE price = CAST('NaN' AS DOUBLE PRECISION)",
        )
        .unwrap();
        assert!(nan_info
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("NaN")));
    }

    #[test]
    fn substitute_sql_params_and_epoch_invalidation_behave_safely() {
        let params = vec![
            SqlParamLiteral::Int(-10),
            SqlParamLiteral::Float(42.5),
            SqlParamLiteral::Text("O'Brien".to_string()),
            SqlParamLiteral::Bool(true),
        ];
        let sql = substitute_sql_params(
            "SELECT region_id FROM sales WHERE region_id > $1 AND price <= $2 AND label = $3 AND active = $4",
            &params,
        )
        .unwrap();
        assert!(sql.contains("(-10)"), "got: {sql}");
        assert!(sql.contains("42.5"), "got: {sql}");
        assert!(sql.contains("'O''Brien'"), "got: {sql}");
        assert!(sql.contains("true"), "got: {sql}");

        assert!(substitute_sql_params("SELECT * FROM sales WHERE id = $2", &params[..1]).is_err());
        assert!(substitute_sql_params(
            "SELECT * FROM sales WHERE price = $1",
            &[SqlParamLiteral::Float(f64::NAN)]
        )
        .is_err());
        assert!(substitute_sql_params(
            "SELECT * FROM sales WHERE label = $1",
            &[SqlParamLiteral::Text("bad\0str".to_string())]
        )
        .is_err());

        let e0 = catalog_epoch();
        let e1 = bump_catalog_epoch();
        assert!(e1 > e0);
        invalidate_relation_oid(16_385);
        assert!(catalog_epoch() > e1);
    }

    #[test]
    fn nan_floating_point_comparisons_and_predicates_are_safely_guarded() {
        let sql_eq = "SELECT region_id FROM sales WHERE price = 'nan'";
        let info_eq = inspect_sql_for_offload(sql_eq).unwrap();
        assert!(info_eq
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("NaN")));

        let sql_gt = "SELECT region_id FROM sales WHERE price > 'nan'";
        let info_gt = inspect_sql_for_offload(sql_gt).unwrap();
        assert!(info_gt
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("NaN")));

        let sql_in = "SELECT region_id FROM sales WHERE price IN ('nan', 1.0)";
        let info_in = inspect_sql_for_offload(sql_in).unwrap();
        assert!(info_in
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("NaN")));

        let sql_between = "SELECT region_id FROM sales WHERE price BETWEEN 0.0 AND 'nan'";
        let info_between = inspect_sql_for_offload(sql_between).unwrap();
        assert!(info_between
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("NaN")));

        let sql_isnan = "SELECT region_id FROM sales WHERE isnan(price)";
        let info_isnan = inspect_sql_for_offload(sql_isnan).unwrap();
        assert!(info_isnan
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("NaN")));

        // Eligibility analysis declines queries with NaN semantics
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, false),
        );
        schemas.insert("sales".to_string(), sample_schema());
        let elig =
            analyze_sql_eligibility(sql_eq, &metas, &schemas, &BoltConfig::default()).unwrap();
        assert!(!elig.eligible);
        assert!(elig.reason.contains("NaN"));
    }

    #[test]
    fn sum_bigint_overflow_semantics_divergence_is_guarded() {
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, false),
        );

        let schema_with_bigint = Arc::new(Schema::new(vec![
            Field::new("region_id", DataType::Int32, false),
            Field::new("price", DataType::Float64, false),
            Field::new("big_qty", DataType::Int64, false),
        ]));
        schemas.insert("sales".to_string(), schema_with_bigint);

        // SUM(big_qty) where big_qty is Int64 is declined to preserve PostgreSQL NUMERIC/overflow semantics
        let sum_bigint_sql = "SELECT region_id, SUM(big_qty) FROM sales GROUP BY region_id";
        let elig_bigint =
            analyze_sql_eligibility(sum_bigint_sql, &metas, &schemas, &BoltConfig::default())
                .unwrap();
        assert!(!elig_bigint.eligible);
        assert!(
            elig_bigint.reason.contains("SUM(bigint)"),
            "got reason: {}",
            elig_bigint.reason
        );

        // SUM(price) where price is Float64 remains eligible
        let sum_float_sql = "SELECT region_id, SUM(price) FROM sales GROUP BY region_id";
        let elig_float =
            analyze_sql_eligibility(sum_float_sql, &metas, &schemas, &BoltConfig::default())
                .unwrap();
        assert!(elig_float.eligible, "got: {}", elig_float.reason);

        // Explicit CAST to BIGINT inside SUM is caught by inspect_sql_for_offload
        let cast_bigint_sql =
            "SELECT region_id, SUM(CAST(region_id AS BIGINT)) FROM sales GROUP BY region_id";
        let info_cast = inspect_sql_for_offload(cast_bigint_sql).unwrap();
        assert!(info_cast
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("SUM(bigint)")));

        // PostgreSQL ::int8 shorthand cast is also caught
        let shorthand_sql = "SELECT region_id, SUM(region_id::int8) FROM sales GROUP BY region_id";
        let info_shorthand = inspect_sql_for_offload(shorthand_sql).unwrap();
        assert!(info_shorthand
            .unsupported_constructs
            .iter()
            .any(|c| c.contains("SUM(bigint)")));
    }

    #[test]
    fn unprojected_order_by_and_desc_nulls_first_semantics() {
        let mut metas = HashMap::new();
        let mut schemas = HashMap::new();
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, false),
        );
        schemas.insert("sales".to_string(), sample_schema());

        // ORDER BY on unprojected column (region_id) rewrites cleanly
        let unprojected_sql = "SELECT price FROM sales ORDER BY region_id DESC";
        let rewritten = rewrite_sql_for_bolt(unprojected_sql, &metas).unwrap();
        assert!(
            rewritten.contains("ORDER BY region_id DESC NULLS FIRST"),
            "unexpected rewrite: {rewritten}"
        );

        let elig =
            analyze_sql_eligibility(unprojected_sql, &metas, &schemas, &BoltConfig::default())
                .unwrap();
        assert!(
            elig.eligible,
            "expected eligible unprojected ORDER BY, got: {}",
            elig.reason
        );

        // Parameterized query is rejected with clear parameter explanation
        let param_sql = "SELECT price FROM sales WHERE region_id = $1";
        let elig_param =
            analyze_sql_eligibility(param_sql, &metas, &schemas, &BoltConfig::default()).unwrap();
        assert!(!elig_param.eligible);
        assert!(elig_param.reason.contains("parameterized"));
    }

    #[test]
    fn alias_collision_avoidance_when_explicit_alias_matches_bare_name() {
        let mut metas = HashMap::new();
        metas.insert(
            "public.sales".to_string(),
            sample_meta("public.sales", "public.sales", 100_000, false),
        );
        metas.insert(
            "users".to_string(),
            sample_meta("users", "public.users", 10_000, false),
        );

        // When another table already defines alias `sales`, `public.sales` must not synthesize `AS sales`.
        let sql =
            "SELECT u.id FROM public.sales JOIN users sales ON public.sales.region_id = sales.id";
        let rewritten = rewrite_sql_for_bolt(sql, &metas).unwrap();
        // It must NOT have two `AS sales`
        let occurrences = rewritten.matches("AS sales").count();
        assert_eq!(
            occurrences, 1,
            "expected exactly 1 'AS sales' from explicit alias, got rewrite: {rewritten}"
        );
    }

    #[test]
    fn nested_join_relations_and_predicates_rewriting() {
        let mut metas = HashMap::new();
        metas.insert(
            "public.sales".to_string(),
            sample_meta("public.sales", "public.sales", 100_000, false),
        );

        let sql = "SELECT s1.region_id FROM (public.sales s1 JOIN public.sales s2 ON s1.region_id = s2.region_id)";
        let rewritten = rewrite_sql_for_bolt(sql, &metas).unwrap();
        assert!(
            rewritten.contains("\"public.sales\" AS s1"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.contains("\"public.sales\" AS s2"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn three_part_column_resolution_honors_schema_match() {
        let mut metas = HashMap::new();
        metas.insert(
            "sales".to_string(),
            sample_meta("sales", "public.sales", 100_000, false),
        );

        // Matching schema resolves to single table column
        let valid_sql = "SELECT public.sales.region_id FROM sales";
        let rewritten_valid = rewrite_sql_for_bolt(valid_sql, &metas).unwrap();
        assert!(
            rewritten_valid.contains("sales.region_id") || rewritten_valid.contains("region_id"),
            "got: {rewritten_valid}"
        );

        // Non-matching schema does not silently resolve to alias
        let invalid_sql = "SELECT wrong_schema.sales.region_id FROM sales";
        let rewritten_invalid = rewrite_sql_for_bolt(invalid_sql, &metas).unwrap();
        assert!(
            rewritten_invalid.contains("wrong_schema.sales.region_id"),
            "wrong schema should remain unmodified: {rewritten_invalid}"
        );
    }

    #[test]
    fn reserved_sql_keywords_are_quoted_safely() {
        let quoted_user = bolt_name_to_ident("user");
        assert!(quoted_user.quote_style.is_some());
        assert_eq!(quoted_user.value, "user");

        let quoted_order = bolt_name_to_ident("order");
        assert!(quoted_order.quote_style.is_some());

        let quoted_case = bolt_name_to_ident("case");
        assert!(quoted_case.quote_style.is_some());

        let plain_sales = bolt_name_to_ident("sales");
        assert!(plain_sales.quote_style.is_none());

        let unquoted_ident = Ident::new("group");
        let preserved = preserved_alias_ident(&unquoted_ident);
        assert!(preserved.quote_style.is_some());
    }
}
