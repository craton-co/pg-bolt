// SPDX-License-Identifier: Apache-2.0

//! Error types and `craton_bolt::BoltError` conversions for `pg_bolt`.

use thiserror::Error;

/// Result alias used throughout `pg_bolt`.
pub type PgBoltResult<T> = Result<T, PgBoltError>;

/// Unified error enum for PostgreSQL bridge, IPC, planner, MVCC, and Bolt GPU execution.
#[derive(Debug, Error)]
pub enum PgBoltError {
    #[error("Craton Bolt GPU engine error: {0}")]
    Bolt(String),

    #[error("PostgreSQL/Arrow bridge error: {0}")]
    Bridge(String),

    #[error("IPC shared-memory error: {0}")]
    Ipc(String),

    #[error("Query planner error: {0}")]
    Planner(String),

    #[error("MVCC delta tracking error: {0}")]
    Mvcc(String),

    #[error("GPU VRAM capacity exhausted: {0}")]
    VramExhausted(String),

    #[error(
        "Fork safety violation: runtime bound to PID {expected_pid}, accessed from PID {current_pid}"
    )]
    ForkHazard { expected_pid: u32, current_pid: u32 },

    #[error("pg_bolt is disabled via GUC (SET pg_bolt.enabled = on)")]
    Disabled,

    #[error("Query cancelled: {0}")]
    Cancelled(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

impl From<craton_bolt::BoltError> for PgBoltError {
    fn from(err: craton_bolt::BoltError) -> Self {
        match err {
            craton_bolt::BoltError::Memory(msg) => Self::VramExhausted(msg),
            craton_bolt::BoltError::GpuCapacity(msg) => Self::VramExhausted(msg),
            craton_bolt::BoltError::CudaWithCode { code: 2, message } => {
                Self::VramExhausted(format!("CUDA_ERROR_OUT_OF_MEMORY (code 2): {message}"))
            }
            craton_bolt::BoltError::Cancelled(reason) => Self::Cancelled(reason.to_string()),
            other => Self::Bolt(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craton_bolt::BoltError;

    #[test]
    fn bolt_memory_and_oom_errors_map_to_vram_exhausted() {
        let mem: PgBoltError = BoltError::Memory("pool limit exceeded".into()).into();
        assert!(matches!(mem, PgBoltError::VramExhausted(ref s) if s.contains("pool limit")));

        let cap: PgBoltError = BoltError::GpuCapacity("not enough VRAM".into()).into();
        assert!(matches!(cap, PgBoltError::VramExhausted(ref s) if s.contains("not enough VRAM")));

        let cuda_oom: PgBoltError = BoltError::CudaWithCode {
            code: 2,
            message: "cuMemAlloc failed".into(),
        }
        .into();
        assert!(matches!(
            cuda_oom,
            PgBoltError::VramExhausted(ref s) if s.contains("CUDA_ERROR_OUT_OF_MEMORY")
        ));
    }

    #[test]
    fn non_oom_bolt_errors_map_to_bolt() {
        let cuda_other: PgBoltError = BoltError::CudaWithCode {
            code: 700,
            message: "illegal address".into(),
        }
        .into();
        assert!(matches!(cuda_other, PgBoltError::Bolt(ref s) if s.contains("illegal address")));

        let sql: PgBoltError = BoltError::Sql("syntax error".into()).into();
        assert!(matches!(sql, PgBoltError::Bolt(ref s) if s.contains("syntax error")));

        let unsupported: PgBoltError = BoltError::Unsupported("cuda-stub".into()).into();
        assert!(matches!(unsupported, PgBoltError::Bolt(ref s) if s.contains("cuda-stub")));
    }

    #[test]
    fn bolt_cancelled_error_maps_to_cancelled() {
        let cancelled: PgBoltError =
            BoltError::Cancelled(craton_bolt::CancelReason::Cancelled).into();
        assert!(matches!(cancelled, PgBoltError::Cancelled(ref s) if s.contains("cancelled")));

        let deadline: PgBoltError =
            BoltError::Cancelled(craton_bolt::CancelReason::DeadlineExceeded).into();
        assert!(matches!(deadline, PgBoltError::Cancelled(ref s) if s.contains("deadline")));
    }

    #[test]
    fn error_display_messages_are_descriptive() {
        assert_eq!(
            PgBoltError::Disabled.to_string(),
            "pg_bolt is disabled via GUC (SET pg_bolt.enabled = on)"
        );
        let fork = PgBoltError::ForkHazard {
            expected_pid: 100,
            current_pid: 200,
        };
        assert!(fork.to_string().contains("100"));
        assert!(fork.to_string().contains("200"));
        assert!(PgBoltError::Bridge("bad oid".into())
            .to_string()
            .contains("bad oid"));
        assert!(PgBoltError::Ipc("ring full".into())
            .to_string()
            .contains("ring full"));
        assert!(PgBoltError::Planner("bad hook".into())
            .to_string()
            .contains("bad hook"));
        assert!(PgBoltError::Mvcc("stale epoch".into())
            .to_string()
            .contains("stale epoch"));
        assert!(PgBoltError::Validation("invalid ident".into())
            .to_string()
            .contains("invalid ident"));
    }
}
