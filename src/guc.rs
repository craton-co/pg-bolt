// SPDX-License-Identifier: Apache-2.0

//! GUC definitions and runtime configuration for `pg_bolt`.

use std::cell::RefCell;
#[cfg(feature = "pgrx")]
use std::ffi::CString;
#[cfg(not(feature = "pgrx"))]
use std::sync::LazyLock;

#[cfg(not(feature = "pgrx"))]
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[cfg(feature = "pgrx")]
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Fallback strategy for unconstrained PostgreSQL `NUMERIC` (`atttypmod == -1`) or columns
/// whose precision exceeds Arrow `Decimal128` (`precision > 38`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NumericUnconstrainedFallback {
    /// Return an explicit `PgBoltError::Bridge` when encountering unconstrained NUMERIC or precision > 38.
    Error,
    /// Promote unconstrained NUMERIC or precision 39..=76 to Arrow `Decimal256`.
    Decimal256,
    /// Fall back to native 64-bit floating point (`Float64`), safely handling NaN/Infinity and arbitrary scales.
    #[default]
    Float64,
}

impl std::str::FromStr for NumericUnconstrainedFallback {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "decimal256" => Ok(Self::Decimal256),
            "float64" | "f64" => Ok(Self::Float64),
            other => Err(format!(
                "invalid numeric unconstrained fallback `{other}`: expected 'error', 'decimal256', or 'float64'"
            )),
        }
    }
}

impl std::fmt::Display for NumericUnconstrainedFallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error => write!(f, "error"),
            Self::Decimal256 => write!(f, "decimal256"),
            Self::Float64 => write!(f, "float64"),
        }
    }
}

/// Runtime configuration snapshot derived from PostgreSQL GUCs (under `pgrx`)
/// or the in-process runtime/test store (in standalone Rust builds).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoltConfig {
    pub enabled: bool,
    pub min_offload_rows: usize,
    pub max_vram_mb: usize,
    pub enable_custom_scan: bool,
    pub allow_hybrid_fallback: bool,
    pub statement_timeout_ms: u64,
    pub use_bgworker: bool,
    pub ipc_ring_slots: usize,
    pub ipc_slot_bytes: usize,
    pub numeric_unconstrained_fallback: NumericUnconstrainedFallback,
}

impl Default for BoltConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_offload_rows: 50_000,
            max_vram_mb: 0,
            enable_custom_scan: true,
            allow_hybrid_fallback: true,
            statement_timeout_ms: 0,
            use_bgworker: true,
            ipc_ring_slots: 16,
            ipc_slot_bytes: 16 * 1024 * 1024,
            numeric_unconstrained_fallback: NumericUnconstrainedFallback::Float64,
        }
    }
}

impl BoltConfig {
    /// Returns the configured GPU VRAM budget in bytes, or `None` when unbounded (`max_vram_mb == 0`).
    pub const fn max_vram_bytes(&self) -> Option<usize> {
        if self.max_vram_mb == 0 {
            None
        } else {
            Some(self.max_vram_mb.saturating_mul(1024 * 1024))
        }
    }

    /// Returns the total byte capacity across all IPC ring slots, saturating on overflow.
    pub const fn total_ipc_ring_bytes(&self) -> usize {
        self.ipc_ring_slots.saturating_mul(self.ipc_slot_bytes)
    }
}

#[cfg(feature = "pgrx")]
pub static PG_BOLT_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_MIN_OFFLOAD_ROWS: GucSetting<i32> = GucSetting::<i32>::new(50_000);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_MAX_VRAM_MB: GucSetting<i32> = GucSetting::<i32>::new(0);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_ENABLE_CUSTOM_SCAN: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_ALLOW_HYBRID_FALLBACK: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_STATEMENT_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(0);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_USE_BGWORKER: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_IPC_RING_SLOTS: GucSetting<i32> = GucSetting::<i32>::new(16);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_IPC_SLOT_BYTES: GucSetting<i32> = GucSetting::<i32>::new(16 * 1024 * 1024);
#[cfg(feature = "pgrx")]
pub static PG_BOLT_NUMERIC_UNCONSTRAINED_FALLBACK: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

#[cfg(not(feature = "pgrx"))]
static GLOBAL_CONFIG: LazyLock<RwLock<BoltConfig>> =
    LazyLock::new(|| RwLock::new(BoltConfig::default()));

thread_local! {
    static TEST_CONFIG_OVERRIDE: RefCell<Option<BoltConfig>> = const { RefCell::new(None) };
}

struct TestConfigGuard {
    prev: Option<BoltConfig>,
}

impl Drop for TestConfigGuard {
    fn drop(&mut self) {
        TEST_CONFIG_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = self.prev.take();
        });
    }
}

/// Returns the currently active `BoltConfig`.
pub fn current_config() -> BoltConfig {
    #[cfg(not(feature = "pgrx"))]
    {
        if let Some(cfg) = TEST_CONFIG_OVERRIDE.with(|cell| cell.borrow().clone()) {
            return cfg;
        }
        GLOBAL_CONFIG.read().clone()
    }

    #[cfg(feature = "pgrx")]
    {
        if let Some(cfg) = TEST_CONFIG_OVERRIDE.with(|cell| cell.borrow().clone()) {
            return cfg;
        }
        BoltConfig {
            enabled: PG_BOLT_ENABLED.get(),
            min_offload_rows: PG_BOLT_MIN_OFFLOAD_ROWS.get().max(0) as usize,
            max_vram_mb: PG_BOLT_MAX_VRAM_MB.get().max(0) as usize,
            enable_custom_scan: PG_BOLT_ENABLE_CUSTOM_SCAN.get(),
            allow_hybrid_fallback: PG_BOLT_ALLOW_HYBRID_FALLBACK.get(),
            statement_timeout_ms: PG_BOLT_STATEMENT_TIMEOUT_MS.get().max(0) as u64,
            use_bgworker: PG_BOLT_USE_BGWORKER.get(),
            ipc_ring_slots: PG_BOLT_IPC_RING_SLOTS.get().max(2) as usize,
            ipc_slot_bytes: PG_BOLT_IPC_SLOT_BYTES.get().max(4096) as usize,
            numeric_unconstrained_fallback: PG_BOLT_NUMERIC_UNCONSTRAINED_FALLBACK
                .get()
                .as_ref()
                .and_then(|cs| cs.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(NumericUnconstrainedFallback::Float64),
        }
    }
}

/// Updates the process-wide fallback configuration (used in non-`pgrx` builds and tests).
pub fn set_runtime_config(#[cfg_attr(feature = "pgrx", allow(unused_variables))] cfg: BoltConfig) {
    #[cfg(not(feature = "pgrx"))]
    {
        *GLOBAL_CONFIG.write() = cfg;
    }
}

/// Temporarily overrides `current_config()` on the current thread for the duration of `f`.
pub fn with_test_config<R>(cfg: BoltConfig, f: impl FnOnce() -> R) -> R {
    let prev = TEST_CONFIG_OVERRIDE.with(|cell| cell.borrow_mut().replace(cfg));
    let _guard = TestConfigGuard { prev };
    f()
}

/// Registers all `pg_bolt.*` GUCs with PostgreSQL during `_PG_init`.
///
/// Uses `Suset`/`Userset` rather than `Postmaster` so `CREATE EXTENSION pg_bolt` works even when
/// dynamically loaded outside `shared_preload_libraries`.
#[cfg(feature = "pgrx")]
pub fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pg_bolt.enabled",
        c"Enable Craton Bolt runtime PTX GPU acceleration.",
        c"When enabled, bolt.* functions and planner hooks can offload analytical SQL to the GPU.",
        &PG_BOLT_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_bolt.min_offload_rows",
        c"Minimum estimated row count before CustomScan offloads a query to Craton Bolt.",
        c"Prevents tiny OLTP lookups from incurring GPU kernel launch and IPC transfer overhead.",
        &PG_BOLT_MIN_OFFLOAD_ROWS,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_bolt.max_vram_mb",
        c"Maximum GPU VRAM cache budget in MiB (0 = unlimited).",
        c"Triggers LRU eviction of cached Bolt tables when new loads would exceed this budget.",
        &PG_BOLT_MAX_VRAM_MB,
        0,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_bolt.enable_custom_scan",
        c"Enable transparent PostgreSQL CustomScan offload via planner_hook.",
        c"When false, only explicit bolt.query()/bolt.query_json() calls execute on Bolt.",
        &PG_BOLT_ENABLE_CUSTOM_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_bolt.allow_hybrid_fallback",
        c"Allow queries planned for Bolt Hybrid execution tier.",
        c"When false, planner offload rejects Hybrid-tier queries and keeps them on PostgreSQL.",
        &PG_BOLT_ALLOW_HYBRID_FALLBACK,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_bolt.statement_timeout_ms",
        c"Cancellation timeout in milliseconds for Bolt GPU queries (0 = disabled).",
        c"Cancels long-running Bolt kernels cooperatively via CancellationToken.",
        &PG_BOLT_STATEMENT_TIMEOUT_MS,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_bolt.use_bgworker",
        c"Route queries through the dedicated CUDA background worker over shared memory IPC.",
        c"Spawning the worker requires shared_preload_libraries = 'pg_bolt' at server startup.",
        &PG_BOLT_USE_BGWORKER,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_bolt.ipc_ring_slots",
        c"Number of shared-memory ring buffer slots for background worker IPC.",
        c"Controls concurrency depth between PostgreSQL backends and the CUDA background worker.",
        &PG_BOLT_IPC_RING_SLOTS,
        2,
        4096,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_bolt.ipc_slot_bytes",
        c"Maximum byte capacity per shared-memory IPC ring slot.",
        c"Bounds single Arrow IPC request/response buffer transfers over PostgreSQL shared memory.",
        &PG_BOLT_IPC_SLOT_BYTES,
        4096,
        512 * 1024 * 1024,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_bolt.numeric_unconstrained_fallback",
        c"Fallback strategy for unconstrained NUMERIC (atttypmod == -1) or precision > 38.",
        c"Valid values: 'error', 'decimal256', 'float64' (default).",
        &PG_BOLT_NUMERIC_UNCONSTRAINED_FALLBACK,
        GucContext::Userset,
        GucFlags::default(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_spec() {
        let cfg = BoltConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.min_offload_rows, 50_000);
        assert_eq!(cfg.max_vram_mb, 0);
        assert_eq!(cfg.max_vram_bytes(), None);
        assert!(cfg.enable_custom_scan);
        assert!(cfg.allow_hybrid_fallback);
        assert_eq!(cfg.statement_timeout_ms, 0);
        assert!(cfg.use_bgworker);
        assert_eq!(cfg.ipc_ring_slots, 16);
        assert_eq!(cfg.ipc_slot_bytes, 16 * 1024 * 1024);
        assert_eq!(
            cfg.numeric_unconstrained_fallback,
            NumericUnconstrainedFallback::Float64
        );
    }

    #[test]
    fn numeric_unconstrained_fallback_parsing_and_override() {
        assert_eq!(
            "error".parse::<NumericUnconstrainedFallback>().unwrap(),
            NumericUnconstrainedFallback::Error
        );
        assert_eq!(
            "decimal256"
                .parse::<NumericUnconstrainedFallback>()
                .unwrap(),
            NumericUnconstrainedFallback::Decimal256
        );
        assert_eq!(
            "float64".parse::<NumericUnconstrainedFallback>().unwrap(),
            NumericUnconstrainedFallback::Float64
        );
        assert_eq!(
            "f64".parse::<NumericUnconstrainedFallback>().unwrap(),
            NumericUnconstrainedFallback::Float64
        );
        assert_eq!(
            "  DECIMAL256  "
                .parse::<NumericUnconstrainedFallback>()
                .unwrap(),
            NumericUnconstrainedFallback::Decimal256
        );
        assert!("invalid".parse::<NumericUnconstrainedFallback>().is_err());

        assert_eq!(NumericUnconstrainedFallback::Error.to_string(), "error");
        assert_eq!(
            NumericUnconstrainedFallback::Decimal256.to_string(),
            "decimal256"
        );
        assert_eq!(NumericUnconstrainedFallback::Float64.to_string(), "float64");

        let custom = BoltConfig {
            numeric_unconstrained_fallback: NumericUnconstrainedFallback::Decimal256,
            ..BoltConfig::default()
        };
        with_test_config(custom, || {
            assert_eq!(
                current_config().numeric_unconstrained_fallback,
                NumericUnconstrainedFallback::Decimal256
            );
        });
    }

    #[test]
    fn with_test_config_is_thread_scoped_and_restores_on_panic() {
        let base = current_config();
        let custom = BoltConfig {
            enabled: false,
            min_offload_rows: 123,
            max_vram_mb: 64,
            ..BoltConfig::default()
        };
        assert_eq!(custom.max_vram_bytes(), Some(64 * 1024 * 1024));

        with_test_config(custom.clone(), || {
            assert_eq!(current_config(), custom);
            with_test_config(
                BoltConfig {
                    min_offload_rows: 999,
                    ..custom.clone()
                },
                || {
                    assert_eq!(current_config().min_offload_rows, 999);
                },
            );
            assert_eq!(current_config().min_offload_rows, 123);
        });
        assert_eq!(current_config(), base);

        let panicked = std::panic::catch_unwind(|| {
            with_test_config(custom, || -> () {
                panic!("boom");
            });
        });
        assert!(panicked.is_err());
        assert_eq!(current_config(), base);
    }

    #[test]
    fn max_vram_bytes_saturates_instead_of_overflowing() {
        let cfg = BoltConfig {
            max_vram_mb: usize::MAX,
            ..BoltConfig::default()
        };
        assert_eq!(cfg.max_vram_bytes(), Some(usize::MAX));
    }

    #[test]
    fn total_ipc_ring_bytes_and_saturation() {
        let cfg = BoltConfig::default();
        assert_eq!(cfg.total_ipc_ring_bytes(), 16 * 16 * 1024 * 1024);

        let max_cfg = BoltConfig {
            ipc_ring_slots: usize::MAX,
            ipc_slot_bytes: 2,
            ..BoltConfig::default()
        };
        assert_eq!(max_cfg.total_ipc_ring_bytes(), usize::MAX);
    }
}
