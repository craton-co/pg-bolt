//! Persistent GPU background worker and client facade (Phase 2).
//!
//! [`BoltBgWorker`] runs a single persistent worker thread (or PostgreSQL
//! dynamic background worker process under `#[cfg(feature = "pgrx")]`) that
//! owns the sole [`BoltSessionRuntime`] and CUDA context in the process.
//! Client sessions issue commands over [`SharedMemoryArena`] via
//! [`BgWorkerClient`], decoupling backend queries from CUDA initialization and
//! fork hazards.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use craton_bolt::ExecutionTier;
use parking_lot::Mutex;

use crate::error::{PgBoltError, PgBoltResult};
use crate::guc::{current_config, BoltConfig};
use crate::ipc::{
    ipc_stream_to_record_batch, record_batch_to_ipc_stream, ChunkReassembler,
    ChunkStreamCancelGuard, IpcChunkHeader, IpcCommand, IpcResponse, IpcRingConfig,
    ResponseChunkStash, SharedMemoryArena,
};
use crate::runtime::BoltSessionRuntime;

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Telemetry and tracking for initial engine OOC budget vs dynamically reconfigured GUC VRAM limit.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkerMemoryBudgetTracker {
    pub initial_engine_budget_bytes: Option<usize>,
    pub current_vram_budget_bytes: Option<usize>,
    pub reconfigure_count: u64,
}

impl WorkerMemoryBudgetTracker {
    pub fn new(initial_budget: Option<usize>) -> Self {
        Self {
            initial_engine_budget_bytes: initial_budget,
            current_vram_budget_bytes: initial_budget,
            reconfigure_count: 0,
        }
    }

    pub fn on_reconfigure(&mut self, new_budget: Option<usize>) {
        self.reconfigure_count = self.reconfigure_count.saturating_add(1);
        self.current_vram_budget_bytes = new_budget;
        if self.initial_engine_budget_bytes != new_budget {
            log::info!(
                "pg_bolt worker memory budget dynamically reconfigured from {:?} to {:?} bytes (evicting LRU tables; engine OOC operator budget retained until restart)",
                self.initial_engine_budget_bytes,
                new_budget
            );
        }
    }

    /// Returns the currently active VRAM budget in bytes (or 0 for unbounded).
    pub fn active_engine_budget_bytes(&self) -> u64 {
        self.current_vram_budget_bytes.unwrap_or(0) as u64
    }

    /// Records a GUC reconfigure event from `max_vram_mb` and returns a summary telemetry string.
    pub fn record_reconfigure(&mut self, max_vram_mb: usize) -> String {
        let new_bytes = if max_vram_mb == 0 {
            None
        } else {
            max_vram_mb.checked_mul(1024 * 1024)
        };
        self.on_reconfigure(new_bytes);
        format!(
            "initial_engine={:?}, current_vram={:?}, reconfigures={}",
            self.initial_engine_budget_bytes,
            self.current_vram_budget_bytes,
            self.reconfigure_count
        )
    }
}

/// Persistent in-process GPU worker owning the sole [`BoltSessionRuntime`].
pub struct BoltBgWorker {
    arena: Arc<SharedMemoryArena>,
    handle: Option<JoinHandle<()>>,
    default_timeout: Option<Duration>,
}

impl std::fmt::Debug for BoltBgWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltBgWorker")
            .field("stats", &self.arena.stats())
            .field("running", &self.handle.is_some())
            .field("default_timeout", &self.default_timeout)
            .finish()
    }
}

impl BoltBgWorker {
    /// Spawns the persistent GPU worker thread and waits until
    /// [`BoltSessionRuntime`] initialization finishes.
    ///
    /// If runtime startup fails, the worker thread exits cleanly and is joined
    /// before returning the error so no `ACTIVE_ENGINE` permit leaks.
    pub fn start(config: BoltConfig) -> PgBoltResult<Self> {
        let arena = Arc::new(SharedMemoryArena::new(IpcRingConfig {
            num_slots: config.ipc_ring_slots,
            slot_bytes: config.ipc_slot_bytes,
        })?);

        let default_timeout = default_client_timeout(config.statement_timeout_ms);
        let (tx, rx) = mpsc::sync_channel::<PgBoltResult<()>>(1);
        let worker_arena = Arc::clone(&arena);

        let handle = thread::Builder::new()
            .name("pg_bolt-gpu-worker".to_string())
            .spawn(move || {
                let mut runtime = match BoltSessionRuntime::new_with_config(&config) {
                    Ok(rt) => rt,
                    Err(err) => {
                        let _ = tx.send(Err(err));
                        return;
                    }
                };
                let worker_cfg = config.clone();
                if tx.send(Ok(())).is_err() {
                    return;
                }
                run_worker_loop(&worker_arena, &mut runtime, worker_cfg);
            })
            .map_err(|err| {
                PgBoltError::Ipc(format!("failed to spawn pg_bolt worker thread: {err}"))
            })?;

        match rx.recv() {
            Ok(Ok(())) => Ok(Self {
                arena,
                handle: Some(handle),
                default_timeout,
            }),
            Ok(Err(err)) => {
                arena.shutdown();
                let _ = handle.join();
                Err(err)
            }
            Err(_) => {
                arena.shutdown();
                let _ = handle.join();
                Err(PgBoltError::Ipc(
                    "pg_bolt worker thread exited unexpectedly during startup".to_string(),
                ))
            }
        }
    }

    /// Returns a cloneable client handle targeting this worker's IPC arena.
    #[must_use]
    pub fn client(&self) -> BgWorkerClient {
        BgWorkerClient::new(Arc::clone(&self.arena), self.default_timeout)
    }

    /// Returns a shared reference to the underlying [`SharedMemoryArena`].
    #[must_use]
    pub fn arena(&self) -> Arc<SharedMemoryArena> {
        Arc::clone(&self.arena)
    }

    /// Signals shutdown, unblocks any waiting IPC operations, and waits for the
    /// worker thread to drop [`BoltSessionRuntime`] and exit.
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        self.arena.shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for BoltBgWorker {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// Cloneable client handle sending commands over [`SharedMemoryArena`].
#[derive(Clone, Debug)]
pub struct BgWorkerClient {
    arena: Arc<SharedMemoryArena>,
    default_timeout: Option<Duration>,
    use_shmem_when_initialized: bool,
}

impl BgWorkerClient {
    /// Creates a client handle bound to an existing [`SharedMemoryArena`].
    #[must_use]
    pub fn new(arena: Arc<SharedMemoryArena>, default_timeout: Option<Duration>) -> Self {
        Self {
            arena,
            default_timeout,
            use_shmem_when_initialized: true,
        }
    }

    /// Overrides whether cross-process PostgreSQL shared memory is preferred when initialized.
    #[must_use]
    pub fn with_shmem_preference(mut self, prefer_shmem: bool) -> Self {
        self.use_shmem_when_initialized = prefer_shmem;
        self
    }

    /// Overrides the default client wait timeout on a derived client handle.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_timeout = timeout;
        self
    }

    #[inline]
    fn submit(&self, cmd: IpcCommand, timeout: Option<Duration>) -> PgBoltResult<IpcResponse> {
        #[cfg(feature = "pgrx")]
        if self.use_shmem_when_initialized && crate::ipc::is_shmem_initialized() {
            return crate::ipc::pg_shmem::shmem_submit_request(cmd, timeout);
        }
        self.arena.submit_request(cmd, timeout)
    }

    /// Requests cancellation for an in-flight request by `req_id`.
    /// First attempts preemptive out-of-band cancellation (without waiting for a free ring slot
    /// or for the busy worker to dequeue a command). If unhandled, falls back to in-band CancelRequest.
    pub fn request_cancel(&self, req_id: u64) -> PgBoltResult<bool> {
        let arena_cancelled = self.arena.request_cancel(req_id);
        #[cfg(feature = "pgrx")]
        let shmem_cancelled =
            if self.use_shmem_when_initialized && crate::ipc::is_shmem_initialized() {
                crate::ipc::pg_shmem::signal_slot_cancel(req_id)
            } else {
                false
            };
        #[cfg(not(feature = "pgrx"))]
        let shmem_cancelled = false;

        if arena_cancelled || shmem_cancelled {
            return Ok(true);
        }

        let resp = self.submit(
            IpcCommand::CancelRequest { req_id },
            Some(Duration::from_millis(500)),
        )?;
        match resp {
            IpcResponse::Ack { message } => Ok(message.starts_with("cancelled:")),
            IpcResponse::Error {
                message,
                vram_exhausted,
            } => Err(response_to_error(message, vram_exhausted)),
            other => Err(unexpected_response("CancelRequest", &other)),
        }
    }

    /// Executes a SQL query against cached tables inside the worker runtime.
    pub fn execute_sql(
        &self,
        sql: &str,
        timeout_ms: u64,
    ) -> PgBoltResult<(RecordBatch, u128, ExecutionTier)> {
        let wait_timeout = if timeout_ms > 0 {
            Some(Duration::from_millis(timeout_ms))
        } else {
            self.default_timeout
        };
        let deadline = wait_timeout.map(|d| Instant::now() + d);

        let resp = self.submit(
            IpcCommand::ExecuteSql {
                sql: sql.to_string(),
                timeout_ms,
            },
            wait_timeout,
        )?;

        match resp {
            IpcResponse::Batch {
                ipc_payload,
                elapsed_us,
                tier,
            } => {
                let batch = ipc_stream_to_record_batch(&ipc_payload)?;
                let parsed_tier = parse_tier(&tier)?;
                Ok((batch, elapsed_us, parsed_tier))
            }
            IpcResponse::BatchChunk {
                header,
                elapsed_us,
                tier,
                chunk_data,
            } => self.collect_chunked_batch_response(
                header,
                elapsed_us,
                &tier,
                chunk_data,
                wait_timeout,
                deadline,
            ),
            IpcResponse::Error {
                message,
                vram_exhausted,
            } => Err(response_to_error(message, vram_exhausted)),
            other => Err(unexpected_response("ExecuteSql", &other)),
        }
    }

    fn collect_chunked_batch_response(
        &self,
        first_header: IpcChunkHeader,
        elapsed_us: u128,
        tier: &str,
        first_chunk: Vec<u8>,
        wait_timeout: Option<Duration>,
        deadline: Option<Instant>,
    ) -> PgBoltResult<(RecordBatch, u128, ExecutionTier)> {
        if first_header.total_chunks == 0 || first_header.seq != 0 {
            return Err(PgBoltError::Ipc(format!(
                "invalid initial response chunk header seq={} total_chunks={}",
                first_header.seq, first_header.total_chunks
            )));
        }
        if first_chunk.len() != first_header.payload_len as usize {
            return Err(PgBoltError::Ipc(format!(
                "response chunk length mismatch: header={}, actual={}",
                first_header.payload_len,
                first_chunk.len()
            )));
        }
        let total_bytes = usize::try_from(first_header.total_bytes).map_err(|_| {
            PgBoltError::Ipc("chunked response total_bytes overflows platform usize".to_owned())
        })?;
        let max_bytes = self.arena.max_chunked_payload_bytes();
        if total_bytes > max_bytes {
            return Err(PgBoltError::Ipc(format!(
                "chunked IPC response ({total_bytes} bytes) exceeds max_chunked_payload_bytes ({max_bytes} bytes)"
            )));
        }

        let mut cancel_guard = ChunkStreamCancelGuard {
            arena: &self.arena,
            stream_id: first_header.stream_id,
            timeout: wait_timeout,
            armed: first_header.total_chunks > 1,
        };

        let mut assembled = Vec::with_capacity(total_bytes);
        assembled.extend_from_slice(&first_chunk);

        for chunk_idx in 1..first_header.total_chunks {
            let rem_timeout = deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
            if rem_timeout == Some(Duration::ZERO) {
                return Err(PgBoltError::Ipc(
                    "chunked ExecuteSql response timed out before all chunks arrived".to_owned(),
                ));
            }
            let resp = self.submit(
                IpcCommand::FetchResponseChunk {
                    stream_id: first_header.stream_id,
                    chunk_idx,
                },
                rem_timeout,
            )?;
            match resp {
                IpcResponse::BatchChunk {
                    header, chunk_data, ..
                } => {
                    if header.stream_id != first_header.stream_id
                        || header.seq != chunk_idx
                        || header.total_chunks != first_header.total_chunks
                        || header.total_bytes != first_header.total_bytes
                        || chunk_data.len() != header.payload_len as usize
                    {
                        return Err(PgBoltError::Ipc(format!(
                            "inconsistent response chunk metadata at seq={chunk_idx} for stream_id={}",
                            first_header.stream_id
                        )));
                    }
                    let next_len =
                        assembled
                            .len()
                            .checked_add(chunk_data.len())
                            .ok_or_else(|| {
                                PgBoltError::Ipc("chunked response byte count overflow".to_owned())
                            })?;
                    if next_len > total_bytes {
                        return Err(PgBoltError::Ipc(format!(
                            "chunked response exceeded declared total_bytes ({next_len} > {total_bytes})"
                        )));
                    }
                    assembled.extend_from_slice(&chunk_data);
                }
                IpcResponse::Error {
                    message,
                    vram_exhausted,
                } => return Err(response_to_error(message, vram_exhausted)),
                other => return Err(unexpected_response("FetchResponseChunk", &other)),
            }
        }

        cancel_guard.disarm();
        if assembled.len() != total_bytes {
            return Err(PgBoltError::Ipc(format!(
                "chunked response short read: got {} bytes, expected {total_bytes}",
                assembled.len()
            )));
        }
        let batch = ipc_stream_to_record_batch(&assembled)?;
        let parsed_tier = parse_tier(tier)?;
        Ok((batch, elapsed_us, parsed_tier))
    }

    /// Registers or replaces a cached table in the worker runtime.
    ///
    /// Preserves single-slot rejection when serialized payload exceeds `ipc_slot_bytes`.
    pub fn load_batch(
        &self,
        pg_relation: &str,
        bolt_name: &str,
        batch: &RecordBatch,
    ) -> PgBoltResult<String> {
        let ipc_payload = record_batch_to_ipc_stream(batch)?;
        let resp = self.submit(
            IpcCommand::LoadBatch {
                pg_relation: pg_relation.to_string(),
                bolt_name: bolt_name.to_string(),
                ipc_payload,
            },
            self.default_timeout,
        )?;
        expect_ack("LoadBatch", resp)
    }

    /// Registers or replaces a cached table in the worker runtime, automatically
    /// streaming oversized payloads across bounded `ipc_slot_bytes` chunks.
    pub fn load_batch_chunked(
        &self,
        pg_relation: &str,
        bolt_name: &str,
        batch: &RecordBatch,
    ) -> PgBoltResult<String> {
        let ipc_payload = record_batch_to_ipc_stream(batch)?;
        let resp = self.arena.submit_chunked_load_routed(
            pg_relation,
            bolt_name,
            &ipc_payload,
            self.default_timeout,
            self.use_shmem_when_initialized,
        )?;
        expect_ack("LoadBatchChunk", resp)
    }

    /// Appends a delta batch to an existing cached table in the worker runtime.
    pub fn append_delta(&self, bolt_name: &str, delta: &RecordBatch) -> PgBoltResult<String> {
        let ipc_payload = record_batch_to_ipc_stream(delta)?;
        let resp = self.submit(
            IpcCommand::AppendDelta {
                bolt_name: bolt_name.to_string(),
                ipc_payload,
            },
            self.default_timeout,
        )?;
        expect_ack("AppendDelta", resp)
    }

    /// Appends a delta batch over bounded `ipc_slot_bytes` chunks.
    pub fn append_delta_chunked(
        &self,
        bolt_name: &str,
        delta: &RecordBatch,
    ) -> PgBoltResult<String> {
        let ipc_payload = record_batch_to_ipc_stream(delta)?;
        let resp = self.arena.submit_chunked_append_routed(
            bolt_name,
            &ipc_payload,
            self.default_timeout,
            self.use_shmem_when_initialized,
        )?;
        expect_ack("AppendDeltaChunk", resp)
    }

    /// Evicts a cached table from the worker runtime, returning whether it existed.
    pub fn evict_table(&self, bolt_name: &str) -> PgBoltResult<bool> {
        let resp = self.submit(
            IpcCommand::EvictTable {
                bolt_name: bolt_name.to_string(),
            },
            self.default_timeout,
        )?;
        let message = expect_ack("EvictTable", resp)?;
        Ok(message.starts_with("evicted:"))
    }

    /// Plans a SQL query inside the worker runtime and returns its execution tier.
    pub fn explain_tier(&self, sql: &str) -> PgBoltResult<ExecutionTier> {
        let resp = self.submit(
            IpcCommand::ExplainTier {
                sql: sql.to_string(),
            },
            self.default_timeout,
        )?;

        match resp {
            IpcResponse::Tier { tier } => parse_tier(&tier),
            IpcResponse::Error {
                message,
                vram_exhausted,
            } => Err(response_to_error(message, vram_exhausted)),
            other => Err(unexpected_response("ExplainTier", &other)),
        }
    }

    /// Generates a formatted logical/physical plan explanation for `sql` in the worker runtime.
    pub fn explain_sql(&self, sql: &str) -> PgBoltResult<String> {
        let resp = self.submit(
            IpcCommand::ExplainSql {
                sql: sql.to_string(),
            },
            self.default_timeout,
        )?;

        match resp {
            IpcResponse::ExplainSql { plan } => Ok(plan),
            IpcResponse::Error {
                message,
                vram_exhausted,
            } => Err(response_to_error(message, vram_exhausted)),
            other => Err(unexpected_response("ExplainSql", &other)),
        }
    }

    /// Synchronizes cached table dirty state (`mark_dirty` / `clear_dirty`) in the worker runtime.
    pub fn set_table_dirty(&self, bolt_name: &str, dirty: bool) -> PgBoltResult<()> {
        let resp = self.submit(
            IpcCommand::SetTableDirty {
                bolt_name: bolt_name.to_string(),
                dirty,
            },
            self.default_timeout,
        )?;
        expect_ack("SetTableDirty", resp)?;
        Ok(())
    }

    /// Returns JSON-serialized [`crate::runtime::BoltRuntimeStatus`] from the worker.
    pub fn status_json(&self) -> PgBoltResult<String> {
        let resp = self.submit(IpcCommand::GetStatus, self.default_timeout)?;

        match resp {
            IpcResponse::StatusJson { json } => Ok(json),
            IpcResponse::Error {
                message,
                vram_exhausted,
            } => Err(response_to_error(message, vram_exhausted)),
            other => Err(unexpected_response("GetStatus", &other)),
        }
    }

    /// Returns JSON-serialized cached table metadata from the worker.
    pub fn tables_json(&self) -> PgBoltResult<String> {
        let resp = self.submit(IpcCommand::ListTables, self.default_timeout)?;

        match resp {
            IpcResponse::TablesJson { json } => Ok(json),
            IpcResponse::Error {
                message,
                vram_exhausted,
            } => Err(response_to_error(message, vram_exhausted)),
            other => Err(unexpected_response("ListTables", &other)),
        }
    }
}

fn run_worker_loop(
    arena: &SharedMemoryArena,
    runtime: &mut BoltSessionRuntime,
    worker_cfg: BoltConfig,
) {
    let mut reassembler = ChunkReassembler::new(arena.max_chunked_payload_bytes());
    let mut response_stash = ResponseChunkStash::new(arena.max_chunked_payload_bytes());
    while !arena.stats().is_shutdown {
        let Some((req_id, cmd, cancel_flag)) = arena.recv_request_with_cancel(WORKER_POLL_INTERVAL)
        else {
            reassembler.prune_stale_streams();
            response_stash.prune_stale_streams();
            continue;
        };
        if step_command(
            arena,
            runtime,
            &mut reassembler,
            &mut response_stash,
            &worker_cfg,
            req_id,
            cmd,
            Some(cancel_flag),
        ) {
            arena.shutdown();
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn step_command(
    arena: &SharedMemoryArena,
    runtime: &mut BoltSessionRuntime,
    reassembler: &mut ChunkReassembler,
    response_stash: &mut ResponseChunkStash,
    worker_cfg: &BoltConfig,
    req_id: u64,
    cmd: IpcCommand,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> bool {
    let cancel_flag_dispatch = cancel_flag.clone();
    match catch_unwind(AssertUnwindSafe(|| {
        dispatch_command(
            arena,
            runtime,
            reassembler,
            response_stash,
            worker_cfg,
            req_id,
            cmd,
            cancel_flag_dispatch,
        )
    })) {
        Ok((resp, should_stop)) => {
            complete_or_fallback(arena, req_id, resp);
            should_stop
        }
        Err(payload) => {
            let panic_msg = panic_payload_to_string(payload);
            log::error!("pg_bolt worker panicked handling req_id={req_id}: {panic_msg}");
            let _ = arena.complete_request(
                req_id,
                IpcResponse::Error {
                    message: format!("pg_bolt worker panicked: {panic_msg}"),
                    vram_exhausted: false,
                },
            );
            true
        }
    }
}

fn exec_register_payload(
    runtime: &mut BoltSessionRuntime,
    pg_relation: &str,
    bolt_name: &str,
    ipc_payload: &[u8],
) -> IpcResponse {
    match ipc_stream_to_record_batch(ipc_payload)
        .and_then(|batch| runtime.register_batch(pg_relation, bolt_name, batch))
    {
        Ok(meta) => IpcResponse::Ack {
            message: format!(
                "loaded {} rows ({} bytes) into '{}'",
                meta.num_rows, meta.approx_bytes, meta.bolt_name
            ),
        },
        Err(err) => error_response(err),
    }
}

fn exec_append_payload(
    runtime: &mut BoltSessionRuntime,
    bolt_name: &str,
    ipc_payload: &[u8],
) -> IpcResponse {
    match ipc_stream_to_record_batch(ipc_payload)
        .and_then(|delta| runtime.append_delta_batch(bolt_name, delta))
    {
        Ok(meta) => IpcResponse::Ack {
            message: format!(
                "appended delta into '{}' (total {} rows, {} bytes)",
                meta.bolt_name, meta.num_rows, meta.approx_bytes
            ),
        },
        Err(err) => error_response(err),
    }
}

#[inline]
fn resolve_effective_timeout_ms(worker_cfg: &BoltConfig, timeout_ms: u64) -> u64 {
    if timeout_ms > 0 {
        timeout_ms
    } else {
        worker_cfg.statement_timeout_ms
    }
}

fn execute_sql_with_effective_timeout(
    runtime: &mut BoltSessionRuntime,
    worker_cfg: &BoltConfig,
    sql: &str,
    timeout_ms: u64,
) -> PgBoltResult<crate::runtime::BoltExecutionResult> {
    let effective_timeout_ms = resolve_effective_timeout_ms(worker_cfg, timeout_ms);
    runtime.execute_sql_with_timeout(sql, effective_timeout_ms)
}

#[allow(clippy::too_many_arguments)]
fn dispatch_command(
    arena: &SharedMemoryArena,
    runtime: &mut BoltSessionRuntime,
    reassembler: &mut ChunkReassembler,
    response_stash: &mut ResponseChunkStash,
    worker_cfg: &BoltConfig,
    req_id: u64,
    cmd: IpcCommand,
    cancel_flag: Option<Arc<AtomicBool>>,
) -> (IpcResponse, bool) {
    match cmd {
        IpcCommand::CancelRequest {
            req_id: target_req_id,
        } => {
            let arena_cancelled = arena.request_cancel(target_req_id);
            #[cfg(feature = "pgrx")]
            let shmem_cancelled = crate::ipc::pg_shmem::signal_slot_cancel(target_req_id);
            #[cfg(not(feature = "pgrx"))]
            let shmem_cancelled = false;
            let cancelled = arena_cancelled || shmem_cancelled;
            let message = if cancelled {
                format!("cancelled:{target_req_id}")
            } else {
                format!("not_found:{target_req_id}")
            };
            (IpcResponse::Ack { message }, false)
        }
        IpcCommand::ExecuteSql { sql, timeout_ms } => {
            if let Some(ref flag) = cancel_flag {
                if flag.load(Ordering::Acquire) {
                    return (
                        error_response(PgBoltError::Cancelled(format!(
                            "request {req_id} was cancelled before execution started"
                        ))),
                        false,
                    );
                }
            }

            #[cfg(feature = "pgrx")]
            struct QueryCancelWatcherGuard {
                done: Arc<AtomicBool>,
                handle: Option<std::thread::JoinHandle<()>>,
            }

            #[cfg(feature = "pgrx")]
            impl Drop for QueryCancelWatcherGuard {
                fn drop(&mut self) {
                    self.done.store(true, Ordering::Release);
                    if let Some(h) = self.handle.take() {
                        let _ = h.join();
                    }
                    unsafe {
                        std::ptr::write_volatile(&raw mut pgrx::pg_sys::QueryCancelPending, 0);
                    }
                }
            }

            #[cfg(feature = "pgrx")]
            let _watcher_guard = cancel_flag.as_ref().map(|flag| {
                let flag = Arc::clone(flag);
                let done = Arc::new(AtomicBool::new(false));
                let done_clone = Arc::clone(&done);
                let handle = std::thread::spawn(move || {
                    while !done_clone.load(Ordering::Acquire) {
                        if flag.load(Ordering::Acquire) {
                            unsafe {
                                std::ptr::write_volatile(
                                    &raw mut pgrx::pg_sys::QueryCancelPending,
                                    1,
                                );
                            }
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                });
                QueryCancelWatcherGuard {
                    done,
                    handle: Some(handle),
                }
            });

            let slot_bytes = arena.slot_bytes();
            let exec_res =
                execute_sql_with_effective_timeout(runtime, worker_cfg, &sql, timeout_ms);

            #[cfg(feature = "pgrx")]
            drop(_watcher_guard);

            let resp = match exec_res.and_then(|res| {
                let ipc_payload = record_batch_to_ipc_stream(&res.batch)?;
                let stream_id = arena.next_stream_id();
                response_stash.prepare_batch_response(
                    stream_id,
                    slot_bytes,
                    ipc_payload,
                    res.elapsed_us,
                    tier_to_str(res.tier).to_string(),
                )
            }) {
                Ok(batch_resp) => batch_resp,
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::LoadBatch {
            pg_relation,
            bolt_name,
            ipc_payload,
        } => (
            exec_register_payload(runtime, &pg_relation, &bolt_name, &ipc_payload),
            false,
        ),
        IpcCommand::LoadBatchChunk {
            pg_relation,
            bolt_name,
            header,
            chunk_data,
        } => {
            let stream_key = format!("load:{pg_relation}:{bolt_name}");
            let resp = match reassembler.push(&stream_key, &header, &chunk_data) {
                Ok(None) => IpcResponse::Ack {
                    message: format!(
                        "buffered chunk {}/{} for '{}'",
                        header.seq + 1,
                        header.total_chunks,
                        bolt_name
                    ),
                },
                Ok(Some(full_payload)) => {
                    exec_register_payload(runtime, &pg_relation, &bolt_name, &full_payload)
                }
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::AppendDelta {
            bolt_name,
            ipc_payload,
        } => (
            exec_append_payload(runtime, &bolt_name, &ipc_payload),
            false,
        ),
        IpcCommand::AppendDeltaChunk {
            bolt_name,
            header,
            chunk_data,
        } => {
            let stream_key = format!("delta:{bolt_name}");
            let resp = match reassembler.push(&stream_key, &header, &chunk_data) {
                Ok(None) => IpcResponse::Ack {
                    message: format!(
                        "buffered delta chunk {}/{} for '{}'",
                        header.seq + 1,
                        header.total_chunks,
                        bolt_name
                    ),
                },
                Ok(Some(full_payload)) => exec_append_payload(runtime, &bolt_name, &full_payload),
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::EvictTable { bolt_name } => {
            let resp = match runtime.evict_table(&bolt_name) {
                Ok(true) => IpcResponse::Ack {
                    message: format!("evicted:{bolt_name}"),
                },
                Ok(false) => IpcResponse::Ack {
                    message: format!("not_found:{bolt_name}"),
                },
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::ExplainTier { sql } => {
            let resp = match runtime.explain_tier(&sql) {
                Ok(tier) => IpcResponse::Tier {
                    tier: tier_to_str(tier).to_string(),
                },
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::ExplainSql { sql } => {
            let resp = match runtime.explain_sql(&sql) {
                Ok(plan) => IpcResponse::ExplainSql { plan },
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::SetTableDirty { bolt_name, dirty } => {
            let updated = if dirty {
                runtime.mark_dirty(&bolt_name)
            } else {
                runtime.clear_dirty(&bolt_name)
            };
            let prefix = if !updated {
                "not_found"
            } else if dirty {
                "dirty"
            } else {
                "clean"
            };
            (
                IpcResponse::Ack {
                    message: format!("{prefix}:{bolt_name}"),
                },
                false,
            )
        }
        IpcCommand::CancelChunkStream { stream_id } => {
            reassembler.discard_stream(stream_id);
            response_stash.discard_stream(stream_id);
            (
                IpcResponse::Ack {
                    message: format!("cancelled:{stream_id}"),
                },
                false,
            )
        }
        IpcCommand::FetchResponseChunk {
            stream_id,
            chunk_idx,
        } => {
            let resp = match response_stash.take_chunk(stream_id, chunk_idx) {
                Ok(chunk) => chunk,
                Err(err) => error_response(err),
            };
            (resp, false)
        }
        IpcCommand::GetStatus => {
            let resp = match serde_json::to_string(&runtime.status()) {
                Ok(json) => IpcResponse::StatusJson { json },
                Err(err) => error_response(PgBoltError::Ipc(format!(
                    "failed to serialize runtime status: {err}"
                ))),
            };
            (resp, false)
        }
        IpcCommand::ListTables => {
            let resp = match serde_json::to_string(&runtime.list_tables()) {
                Ok(json) => IpcResponse::TablesJson { json },
                Err(err) => error_response(PgBoltError::Ipc(format!(
                    "failed to serialize cached tables: {err}"
                ))),
            };
            (resp, false)
        }
        IpcCommand::Shutdown => (
            IpcResponse::Ack {
                message: "pg_bolt worker shutting down".to_string(),
            },
            true,
        ),
    }
}

fn complete_or_fallback(arena: &SharedMemoryArena, req_id: u64, resp: IpcResponse) {
    if let Err(err) = arena.complete_request(req_id, resp) {
        log::warn!("pg_bolt worker failed to send IPC response for req_id={req_id}: {err}");
        let _ = arena.complete_request(
            req_id,
            IpcResponse::Error {
                message: format!("response exceeded IPC limit or could not be delivered: {err}"),
                vram_exhausted: false,
            },
        );
    }
}

fn default_client_timeout(statement_timeout_ms: u64) -> Option<Duration> {
    if statement_timeout_ms > 0 {
        Some(Duration::from_millis(statement_timeout_ms))
    } else {
        None
    }
}

fn error_response(err: PgBoltError) -> IpcResponse {
    IpcResponse::Error {
        vram_exhausted: matches!(err, PgBoltError::VramExhausted(_)),
        message: err.to_string(),
    }
}

fn response_to_error(message: String, vram_exhausted: bool) -> PgBoltError {
    if vram_exhausted {
        PgBoltError::VramExhausted(message)
    } else {
        PgBoltError::Bolt(message)
    }
}

fn expect_ack(op: &str, resp: IpcResponse) -> PgBoltResult<String> {
    match resp {
        IpcResponse::Ack { message } => Ok(message),
        IpcResponse::Error {
            message,
            vram_exhausted,
        } => Err(response_to_error(message, vram_exhausted)),
        other => Err(unexpected_response(op, &other)),
    }
}

fn unexpected_response(op: &str, resp: &IpcResponse) -> PgBoltError {
    PgBoltError::Ipc(format!("unexpected IPC response for {op}: {resp:?}"))
}

fn panic_payload_to_string(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn tier_to_str(tier: ExecutionTier) -> &'static str {
    match tier {
        ExecutionTier::Gpu => "gpu",
        ExecutionTier::Hybrid => "hybrid",
        ExecutionTier::Host => "host",
    }
}

fn parse_tier(raw: &str) -> PgBoltResult<ExecutionTier> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "gpu" => Ok(ExecutionTier::Gpu),
        "hybrid" => Ok(ExecutionTier::Hybrid),
        "host" => Ok(ExecutionTier::Host),
        other => Err(PgBoltError::Ipc(format!(
            "unrecognized ExecutionTier string '{other}'"
        ))),
    }
}

struct SharedBgWorkerState {
    pid: u32,
    worker: BoltBgWorker,
}

static SHARED_BGWORKER: OnceLock<Mutex<Option<SharedBgWorkerState>>> = OnceLock::new();

fn shared_worker_slot() -> &'static Mutex<Option<SharedBgWorkerState>> {
    SHARED_BGWORKER.get_or_init(|| Mutex::new(None))
}

/// Returns a client handle backed by the process's lazily initialized persistent
/// [`BoltBgWorker`], safely discarding stale pre-fork handles if a PID change occurred.
pub fn shared_bgworker_client() -> PgBoltResult<BgWorkerClient> {
    let cur_pid = std::process::id();
    let mut guard = shared_worker_slot().lock();
    if let Some(mut existing) = guard.take() {
        if existing.pid == cur_pid && !existing.worker.arena().stats().is_shutdown {
            let client = existing.worker.client();
            *guard = Some(existing);
            return Ok(client);
        }
        if existing.pid != cur_pid {
            // Post-fork child cannot join a parent thread that no longer exists in this OS process.
            existing.worker.arena().shutdown();
            let _ = existing.worker.handle.take();
        }
    }

    let cfg = current_config();
    let worker = BoltBgWorker::start(cfg)?;
    let client = worker.client();
    *guard = Some(SharedBgWorkerState {
        pid: cur_pid,
        worker,
    });
    Ok(client)
}

/// Shuts down the shared persistent [`BoltBgWorker`], if running in the current process.
pub fn shutdown_shared_bgworker() {
    let cur_pid = std::process::id();
    let existing = shared_worker_slot().lock().take();
    if let Some(mut state) = existing {
        if state.pid != cur_pid {
            state.worker.arena().shutdown();
            let _ = state.worker.handle.take();
        }
    }
}

/// Registers the persistent `pg_bolt GPU daemon` PostgreSQL background worker.
///
/// Must be called from `_PG_init` when `pg_bolt` is loaded via
/// `shared_preload_libraries`.
#[cfg(feature = "pgrx")]
pub fn register_bgworker() {
    use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};

    BackgroundWorkerBuilder::new("pg_bolt GPU daemon")
        .set_function("pg_bolt_bgworker_main")
        .set_library("pg_bolt")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .enable_shmem_access(None)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// PostgreSQL dynamic background worker entry point for `pg_bolt`.
#[cfg(feature = "pgrx")]
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_bolt_bgworker_main(_arg: pgrx::pg_sys::Datum) {
    use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    crate::ipc::pg_shmem::register_worker_latch();

    let mut worker_cfg = current_config();
    let mut budget_tracker = WorkerMemoryBudgetTracker::new(worker_cfg.max_vram_bytes());
    if crate::ipc::is_shmem_initialized() {
        crate::ipc::pg_shmem::update_active_budget(budget_tracker.active_engine_budget_bytes());
    }

    let arena = match SharedMemoryArena::new(IpcRingConfig {
        num_slots: worker_cfg.ipc_ring_slots,
        slot_bytes: worker_cfg.ipc_slot_bytes,
    }) {
        Ok(arena) => Arc::new(arena),
        Err(err) => {
            pgrx::warning!("pg_bolt GPU daemon failed to allocate IPC arena: {err}");
            return;
        }
    };

    let mut runtime = match BoltSessionRuntime::new_with_config(&worker_cfg) {
        Ok(rt) => rt,
        Err(err) => {
            pgrx::warning!("pg_bolt GPU daemon failed to initialize Bolt runtime: {err}");
            return;
        }
    };
    let mut reassembler = ChunkReassembler::new(arena.max_chunked_payload_bytes());
    let mut response_stash = ResponseChunkStash::new(arena.max_chunked_payload_bytes());

    pgrx::log!("pg_bolt GPU daemon started");

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(25))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        if BackgroundWorker::sighup_received() {
            unsafe {
                pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
            }
            worker_cfg = current_config();
            let note = budget_tracker.record_reconfigure(worker_cfg.max_vram_mb);
            if crate::ipc::is_shmem_initialized() {
                crate::ipc::pg_shmem::update_active_budget(
                    budget_tracker.active_engine_budget_bytes(),
                );
            }
            if let Err(err) = runtime.reconfigure(&worker_cfg) {
                pgrx::warning!("pg_bolt GPU daemon failed to reconfigure runtime on SIGHUP: {err}");
            }
            pgrx::log!(
                "pg_bolt GPU daemon reloaded GUCs on SIGHUP (statement_timeout_ms={}, max_vram_mb={}, allow_hybrid_fallback={}) [telemetry: {note}]",
                worker_cfg.statement_timeout_ms,
                worker_cfg.max_vram_mb,
                worker_cfg.allow_hybrid_fallback
            );
        }

        while let Some((req_id, cmd, cancel_flag)) =
            arena.recv_request_with_cancel(Duration::from_millis(1))
        {
            let should_stop = step_command(
                &arena,
                &mut runtime,
                &mut reassembler,
                &mut response_stash,
                &worker_cfg,
                req_id,
                cmd,
                Some(cancel_flag),
            );
            if should_stop || BackgroundWorker::sigterm_received() {
                arena.shutdown();
                pgrx::log!("pg_bolt GPU daemon exiting on stop request");
                return;
            }
        }

        if crate::ipc::is_shmem_initialized() {
            while let Some((slot_idx, req_id, cmd, dsm, cancel_flag)) =
                crate::ipc::pg_shmem::claim_pending_request()
            {
                let dispatch_res = catch_unwind(AssertUnwindSafe(|| {
                    dispatch_command(
                        &arena,
                        &mut runtime,
                        &mut reassembler,
                        &mut response_stash,
                        &worker_cfg,
                        req_id,
                        cmd,
                        Some(cancel_flag),
                    )
                }));
                let (resp, should_stop) = match dispatch_res {
                    Ok(pair) => pair,
                    Err(payload) => {
                        let panic_msg = panic_payload_to_string(payload);
                        pgrx::warning!(
                            "pg_bolt GPU daemon panicked on shmem req_id={req_id}: {panic_msg}"
                        );
                        (
                            IpcResponse::Error {
                                message: format!("pg_bolt GPU daemon panicked: {panic_msg}"),
                                vram_exhausted: false,
                            },
                            true,
                        )
                    }
                };
                crate::ipc::pg_shmem::complete_shmem_request(slot_idx, req_id, resp, dsm);
                if should_stop || BackgroundWorker::sigterm_received() {
                    arena.shutdown();
                    pgrx::log!("pg_bolt GPU daemon exiting on stop request");
                    return;
                }
            }
        }

        reassembler.prune_stale_streams();
        response_stash.prune_stale_streams();
    }

    arena.shutdown();
    pgrx::log!("pg_bolt GPU daemon shutting down cleanly");
}

#[cfg(all(test, not(feature = "pgrx")))]
mod tests {
    use arrow_array::{Float64Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::guc::with_test_config;
    use crate::runtime::{test_process_lock, BoltRuntimeStatus, CachedTableMeta};

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
    fn tier_roundtrip_and_invalid_rejected() {
        assert_eq!(
            parse_tier(tier_to_str(ExecutionTier::Gpu)).unwrap(),
            ExecutionTier::Gpu
        );
        assert_eq!(parse_tier("Hybrid").unwrap(), ExecutionTier::Hybrid);
        assert_eq!(parse_tier("HOST").unwrap(), ExecutionTier::Host);
        assert!(matches!(parse_tier("invalid"), Err(PgBoltError::Ipc(_))));
    }

    #[test]
    fn start_validates_ipc_config_before_spawning() {
        let bad = BoltConfig {
            ipc_ring_slots: 0,
            ..BoltConfig::default()
        };
        let err = BoltBgWorker::start(bad).unwrap_err();
        assert!(matches!(err, PgBoltError::Ipc(_)));
    }

    #[test]
    fn worker_lifecycle_and_restart_after_shutdown() {
        let _lock = test_process_lock();
        let cfg = BoltConfig {
            enabled: true,
            ..BoltConfig::default()
        };

        with_test_config(cfg.clone(), || {
            let worker = match BoltBgWorker::start(cfg.clone()) {
                Ok(w) => w,
                Err(err) if cfg!(not(feature = "cuda-stub")) => {
                    // On GPU-less hosts running non-stub builds, startup fails cleanly.
                    assert!(matches!(
                        err,
                        PgBoltError::Bolt(_) | PgBoltError::VramExhausted(_)
                    ));
                    return;
                }
                Err(err) => panic!("unexpected cuda-stub bgworker startup failure: {err}"),
            };

            let client = worker.client();
            let ack = client
                .load_batch("public.sales", "sales", &sample_batch(32))
                .unwrap();
            assert!(ack.contains("32 rows"));

            let delta_ack = client.append_delta("sales", &sample_batch(8)).unwrap();
            assert!(delta_ack.contains("40 rows"));

            let tables: Vec<CachedTableMeta> =
                serde_json::from_str(&client.tables_json().unwrap()).unwrap();
            assert_eq!(tables.len(), 1);
            assert_eq!(tables[0].bolt_name, "sales");
            assert_eq!(tables[0].num_rows, 40);

            let tier = client
                .explain_tier("SELECT id, SUM(val) FROM sales GROUP BY id")
                .unwrap();
            assert_eq!(tier, ExecutionTier::Hybrid);

            let gpu_tier = client
                .explain_tier("SELECT id, val * 2.0 AS doubled FROM sales")
                .unwrap();
            assert_eq!(gpu_tier, ExecutionTier::Gpu);

            let status: BoltRuntimeStatus =
                serde_json::from_str(&client.status_json().unwrap()).unwrap();
            assert_eq!(status.cached_tables, 1);

            if cfg!(feature = "cuda-stub") {
                let err = client
                    .execute_sql("SELECT COUNT(*) FROM sales", 1000)
                    .unwrap_err();
                assert!(matches!(err, PgBoltError::Bolt(ref m) if m.contains("cuda-stub")));
                // Worker must remain healthy after returning a query error.
                assert!(client.status_json().is_ok());
            } else {
                let (batch, _elapsed, exec_tier) = client
                    .execute_sql("SELECT COUNT(*) AS cnt FROM sales", 5000)
                    .unwrap();
                assert_eq!(batch.num_rows(), 1);
                assert!(matches!(
                    exec_tier,
                    ExecutionTier::Gpu | ExecutionTier::Hybrid | ExecutionTier::Host
                ));
            }

            assert!(client.evict_table("sales").unwrap());
            assert!(!client.evict_table("sales").unwrap());

            worker.shutdown();

            // Worker must be cleanly restartable after shutdown drops BoltSessionRuntime.
            let restarted = BoltBgWorker::start(cfg).unwrap();
            let restarted_status: BoltRuntimeStatus =
                serde_json::from_str(&restarted.client().status_json().unwrap()).unwrap();
            assert_eq!(restarted_status.cached_tables, 0);
            restarted.shutdown();
        });
    }

    #[test]
    fn worker_propagates_vram_and_oversized_ipc_errors_without_stalling() {
        let _lock = test_process_lock();

        // 1 MiB VRAM cap -> ~1.8 MiB batch must map back as PgBoltError::VramExhausted.
        let vram_cfg = BoltConfig {
            max_vram_mb: 1,
            ..BoltConfig::default()
        };
        with_test_config(vram_cfg.clone(), || {
            let worker = match BoltBgWorker::start(vram_cfg) {
                Ok(w) => w,
                Err(_) if cfg!(not(feature = "cuda-stub")) => return,
                Err(err) => panic!("unexpected startup failure: {err}"),
            };
            let client = worker.client();
            let huge = sample_batch(150_000);
            let err = client.load_batch("public.huge", "huge", &huge).unwrap_err();
            assert!(matches!(err, PgBoltError::VramExhausted(_)));
            assert!(client.status_json().is_ok());
            worker.shutdown();
        });

        // Small IPC slot cap -> single-slot oversized payload fails while status_json and chunked transfer succeed.
        let ipc_cfg = BoltConfig {
            ipc_slot_bytes: 1024,
            ..BoltConfig::default()
        };
        with_test_config(ipc_cfg.clone(), || {
            let worker = match BoltBgWorker::start(ipc_cfg) {
                Ok(w) => w,
                Err(_) if cfg!(not(feature = "cuda-stub")) => return,
                Err(err) => panic!("unexpected startup failure: {err}"),
            };
            let client = worker.client();
            let err = client
                .load_batch("public.medium", "medium", &sample_batch(64))
                .unwrap_err();
            assert!(matches!(err, PgBoltError::Ipc(_)));
            assert!(client.status_json().is_ok());

            let chunked_ack = client
                .load_batch_chunked("public.medium", "medium", &sample_batch(256))
                .unwrap();
            assert!(chunked_ack.contains("256 rows"));
            let delta_ack = client
                .append_delta_chunked("medium", &sample_batch(64))
                .unwrap();
            assert!(delta_ack.contains("320 rows"));
            if cfg!(not(feature = "cuda-stub")) {
                let (streamed_batch, _, _) = client
                    .execute_sql("SELECT id, val FROM medium ORDER BY id", 5000)
                    .unwrap();
                assert_eq!(streamed_batch.num_rows(), 320);
            }
            worker.shutdown();
        });
    }

    #[test]
    fn execute_sql_timeout_ms_propagates_and_overrides_default() {
        let cfg = BoltConfig {
            statement_timeout_ms: 5_000,
            ..BoltConfig::default()
        };
        assert_eq!(resolve_effective_timeout_ms(&cfg, 0), 5_000);
        assert_eq!(resolve_effective_timeout_ms(&cfg, 37), 37);

        let zero_cfg = BoltConfig {
            statement_timeout_ms: 0,
            ..BoltConfig::default()
        };
        assert_eq!(resolve_effective_timeout_ms(&zero_cfg, 0), 0);
        assert_eq!(resolve_effective_timeout_ms(&zero_cfg, 250), 250);

        // Verify client passes explicit `timeout_ms` in IpcCommand::ExecuteSql and honors it as wait deadline.
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 4096,
            })
            .unwrap(),
        );
        let client = BgWorkerClient::new(Arc::clone(&arena), Some(Duration::from_secs(5)));
        let empty_payload = record_batch_to_ipc_stream(&sample_batch(2)).unwrap();

        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            let mut observed_timeouts = Vec::new();
            while let Some((req_id, cmd)) = a_worker.recv_request(Duration::from_secs(2)) {
                match cmd {
                    IpcCommand::ExecuteSql { sql, timeout_ms } => {
                        observed_timeouts.push((sql.clone(), timeout_ms));
                        if sql == "SLEEP" {
                            thread::sleep(Duration::from_millis(150));
                        }
                        let _ = a_worker.complete_request(
                            req_id,
                            IpcResponse::Batch {
                                ipc_payload: empty_payload.clone(),
                                elapsed_us: 11,
                                tier: "gpu".to_owned(),
                            },
                        );
                    }
                    IpcCommand::Shutdown => {
                        let _ = a_worker.complete_request(
                            req_id,
                            IpcResponse::Ack {
                                message: "bye".to_owned(),
                            },
                        );
                        break;
                    }
                    _ => {}
                }
            }
            observed_timeouts
        });

        let (batch, elapsed, tier) = client.execute_sql("SELECT 1", 37).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(elapsed, 11);
        assert_eq!(tier, ExecutionTier::Gpu);

        // With `default_timeout = 5s` and `timeout_ms = 40`, a 150ms worker sleep must time out fast.
        let timed_out = client.execute_sql("SLEEP", 40).unwrap_err();
        assert!(matches!(timed_out, PgBoltError::Ipc(_)));

        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_secs(1)));
        let seen = worker.join().unwrap();
        assert_eq!(
            seen,
            vec![("SELECT 1".to_owned(), 37), ("SLEEP".to_owned(), 40)]
        );
    }

    #[test]
    fn explain_sql_and_set_table_dirty_roundtrip_over_bgworker() {
        let _lock = test_process_lock();
        let cfg = BoltConfig {
            enabled: true,
            ..BoltConfig::default()
        };

        with_test_config(cfg.clone(), || {
            let worker = match BoltBgWorker::start(cfg) {
                Ok(w) => w,
                Err(_) if cfg!(not(feature = "cuda-stub")) => return,
                Err(err) => panic!("unexpected startup failure: {err}"),
            };
            let client = worker.client();
            client
                .load_batch("public.events", "events", &sample_batch(24))
                .unwrap();

            let plan = client
                .explain_sql("SELECT id, val * 2.0 FROM events")
                .unwrap();
            assert!(!plan.trim().is_empty());

            let empty_err = client.explain_sql("   ").unwrap_err();
            assert!(matches!(empty_err, PgBoltError::Bolt(ref m) if m.contains("empty")));

            client.set_table_dirty("events", true).unwrap();
            let tables_dirty: Vec<CachedTableMeta> =
                serde_json::from_str(&client.tables_json().unwrap()).unwrap();
            assert!(tables_dirty
                .iter()
                .any(|t| t.bolt_name == "events" && t.dirty));

            client.set_table_dirty("events", false).unwrap();
            let tables_clean: Vec<CachedTableMeta> =
                serde_json::from_str(&client.tables_json().unwrap()).unwrap();
            assert!(tables_clean
                .iter()
                .any(|t| t.bolt_name == "events" && !t.dirty));

            // Unknown table dirty-sync succeeds without faulting the worker.
            client.set_table_dirty("missing_table", true).unwrap();
            assert!(client.status_json().is_ok());

            worker.shutdown();
        });
    }

    #[test]
    fn large_execute_sql_batch_streams_over_tiny_slot_bytes() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 1024,
            })
            .unwrap()
            .with_max_chunked_payload_bytes(64 * 1024),
        );
        let client = BgWorkerClient::new(Arc::clone(&arena), Some(Duration::from_secs(2)));

        let large_batch = sample_batch(256);
        let large_payload = record_batch_to_ipc_stream(&large_batch).unwrap();
        assert!(large_payload.len() > 1024);

        // Single-frame request exceeding slot_bytes must still be rejected and counted.
        let single_err = client
            .load_batch("public.big", "big", &large_batch)
            .unwrap_err();
        assert!(matches!(single_err, PgBoltError::Ipc(_)));
        assert_eq!(arena.stats().total_rejected_oversized, 1);

        let a_worker = Arc::clone(&arena);
        let expected_payload = large_payload.clone();
        let worker = thread::spawn(move || {
            let mut stash = ResponseChunkStash::new(64 * 1024);
            let mut fail_fetch = false;
            while let Some((req_id, cmd)) = a_worker.recv_request(Duration::from_secs(2)) {
                match cmd {
                    IpcCommand::ExecuteSql { sql, .. } => {
                        fail_fetch = sql == "ABORT_SECOND_CHUNK";
                        let stream_id = a_worker.next_stream_id();
                        let resp = stash
                            .prepare_batch_response(
                                stream_id,
                                a_worker.slot_bytes(),
                                expected_payload.clone(),
                                91,
                                "gpu".to_owned(),
                            )
                            .unwrap();
                        a_worker.complete_request(req_id, resp).unwrap();
                    }
                    IpcCommand::FetchResponseChunk {
                        stream_id,
                        chunk_idx,
                    } => {
                        let resp = if fail_fetch && chunk_idx == 1 {
                            IpcResponse::Error {
                                message: "injected response chunk failure".to_owned(),
                                vram_exhausted: false,
                            }
                        } else {
                            match stash.take_chunk(stream_id, chunk_idx) {
                                Ok(chunk) => chunk,
                                Err(err) => IpcResponse::Error {
                                    message: err.to_string(),
                                    vram_exhausted: false,
                                },
                            }
                        };
                        a_worker.complete_request(req_id, resp).unwrap();
                    }
                    IpcCommand::CancelChunkStream { stream_id } => {
                        stash.discard_stream(stream_id);
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: format!("cancelled:{stream_id}"),
                                },
                            )
                            .unwrap();
                    }
                    IpcCommand::Shutdown => {
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: "bye".to_owned(),
                                },
                            )
                            .unwrap();
                        return stash.active_streams();
                    }
                    _ => {}
                }
            }
            stash.active_streams()
        });

        let (streamed, elapsed, tier) =
            client.execute_sql("SELECT id, val FROM big", 2000).unwrap();
        assert_eq!(streamed, large_batch);
        assert_eq!(elapsed, 91);
        assert_eq!(tier, ExecutionTier::Gpu);

        // Mid-stream fetch error triggers RAII CancelChunkStream cleanup on the worker stash.
        let aborted = client.execute_sql("ABORT_SECOND_CHUNK", 2000).unwrap_err();
        assert!(matches!(aborted, PgBoltError::Bolt(ref m) if m.contains("injected")));

        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_secs(1)));
        let remaining_stash = worker.join().unwrap();
        assert_eq!(remaining_stash, 0);
    }

    #[test]
    fn concurrent_clients_and_drop_wakeup_never_hang() {
        let _lock = test_process_lock();
        let cfg = BoltConfig::default();

        with_test_config(cfg.clone(), || {
            let worker = match BoltBgWorker::start(cfg) {
                Ok(w) => w,
                Err(_) if cfg!(not(feature = "cuda-stub")) => return,
                Err(err) => panic!("unexpected startup failure: {err}"),
            };
            let client = worker.client();
            client
                .load_batch("public.concurrent", "concurrent", &sample_batch(16))
                .unwrap();

            let mut handles = Vec::new();
            for _ in 0..4 {
                let c = client.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..8 {
                        let status: BoltRuntimeStatus =
                            serde_json::from_str(&c.status_json().unwrap()).unwrap();
                        assert_eq!(status.cached_tables, 1);
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }

            // Dropping worker implicitly triggers shutdown + thread join.
            drop(worker);

            // Subsequent client calls must return Err rather than blocking.
            let after_drop = client.status_json().unwrap_err();
            assert!(matches!(after_drop, PgBoltError::Ipc(_)));
        });
    }

    #[test]
    fn shared_bgworker_client_initializes_and_shuts_down_cleanly() {
        let _lock = test_process_lock();
        let cfg = BoltConfig::default();
        with_test_config(cfg, || {
            shutdown_shared_bgworker();
            let client = match shared_bgworker_client() {
                Ok(c) => c,
                Err(_) if cfg!(not(feature = "cuda-stub")) => return,
                Err(err) => panic!("unexpected shared worker startup failure: {err}"),
            };
            assert!(client.status_json().is_ok());
            shutdown_shared_bgworker();
        });
    }

    #[test]
    fn test_worker_memory_budget_tracker_records_transitions() {
        let initial_mb = 512;
        let initial_bytes = initial_mb * 1024 * 1024;
        let mut tracker = WorkerMemoryBudgetTracker::new(Some(initial_bytes));
        assert_eq!(tracker.initial_engine_budget_bytes, Some(initial_bytes));
        assert_eq!(tracker.current_vram_budget_bytes, Some(initial_bytes));
        assert_eq!(tracker.reconfigure_count, 0);
        assert_eq!(tracker.active_engine_budget_bytes(), initial_bytes as u64);

        let note1 = tracker.record_reconfigure(1024);
        assert_eq!(tracker.reconfigure_count, 1);
        assert_eq!(tracker.current_vram_budget_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(
            tracker.active_engine_budget_bytes(),
            (1024 * 1024 * 1024) as u64
        );
        assert!(note1.contains("reconfigures=1"));

        let note2 = tracker.record_reconfigure(0);
        assert_eq!(tracker.reconfigure_count, 2);
        assert_eq!(tracker.current_vram_budget_bytes, None);
        assert_eq!(tracker.active_engine_budget_bytes(), 0);
        assert!(note2.contains("reconfigures=2"));
    }

    #[test]
    fn test_bgworker_client_preference_and_shmem_fallback() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 4096,
            })
            .unwrap(),
        );
        let client = BgWorkerClient::new(Arc::clone(&arena), Some(Duration::from_millis(100)))
            .with_shmem_preference(false)
            .with_timeout(Some(Duration::from_millis(250)));
        assert_eq!(client.default_timeout, Some(Duration::from_millis(250)));
        assert!(!client.use_shmem_when_initialized);
    }

    #[test]
    fn test_bgworker_dispatch_cancel_flag_short_circuit() {
        let _lock = test_process_lock();
        let cfg = BoltConfig::default();
        let arena = SharedMemoryArena::new(IpcRingConfig {
            num_slots: 2,
            slot_bytes: 4096,
        })
        .unwrap();
        let mut runtime = BoltSessionRuntime::new_with_config(&cfg).unwrap();
        let mut reassembler = ChunkReassembler::new(4096);
        let mut response_stash = ResponseChunkStash::new(4096);

        let cancel_flag = Arc::new(AtomicBool::new(true));
        let (resp, should_stop) = dispatch_command(
            &arena,
            &mut runtime,
            &mut reassembler,
            &mut response_stash,
            &cfg,
            42,
            IpcCommand::ExecuteSql {
                sql: "SELECT 1".to_string(),
                timeout_ms: 1000,
            },
            Some(cancel_flag),
        );
        assert!(!should_stop);
        match resp {
            IpcResponse::Error { message, .. } => {
                assert!(message.contains("cancelled"));
            }
            other => panic!("expected Error response on pre-cancelled query, got: {other:?}"),
        }
    }

    #[test]
    fn test_bgworker_cancel_request_command_and_cooperative_cancel() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 4096,
            })
            .unwrap(),
        );
        let client = BgWorkerClient::new(Arc::clone(&arena), Some(Duration::from_millis(500)));

        let a_worker = Arc::clone(&arena);
        let worker_thread = thread::spawn(move || {
            let mut handled_commands = Vec::new();
            while let Some((req_id, cmd, _flag)) =
                a_worker.recv_request_with_cancel(Duration::from_secs(1))
            {
                match cmd {
                    IpcCommand::CancelRequest { req_id: target_id } => {
                        let marked = a_worker.request_cancel(target_id);
                        let msg = if marked {
                            format!("cancelled:{target_id}")
                        } else {
                            format!("not_found:{target_id}")
                        };
                        a_worker
                            .complete_request(req_id, IpcResponse::Ack { message: msg })
                            .unwrap();
                        handled_commands.push("cancel");
                    }
                    IpcCommand::Shutdown => {
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: "bye".to_string(),
                                },
                            )
                            .unwrap();
                        handled_commands.push("shutdown");
                        break;
                    }
                    _ => {}
                }
            }
            handled_commands
        });

        // Cancel on non-existent req_id -> not_found (returns false)
        let cancelled_non_existent = client.request_cancel(99999).unwrap();
        assert!(!cancelled_non_existent);

        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_millis(500)));
        let handled = worker_thread.join().unwrap();
        assert_eq!(handled, vec!["cancel", "shutdown"]);
    }

    #[test]
    fn test_bgworker_request_cancel_preempts_busy_worker_out_of_band() {
        // Test with a 1-slot arena: if request_cancel had to allocate a slot,
        // it would deadlock/time out while req_id 1 is occupying the sole slot.
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 1,
                slot_bytes: 512,
            })
            .unwrap(),
        );
        let client = BgWorkerClient::new(Arc::clone(&arena), Some(Duration::from_secs(2)));

        let a_worker = Arc::clone(&arena);
        let worker_thread = thread::spawn(move || {
            let (req_id, _cmd, cancel_flag) = a_worker
                .recv_request_with_cancel(Duration::from_secs(1))
                .unwrap();

            // Simulate worker busy running query while periodically checking cancel
            let start = Instant::now();
            let mut cancelled = false;
            while start.elapsed() < Duration::from_millis(500) {
                if cancel_flag.load(std::sync::atomic::Ordering::Acquire) {
                    cancelled = true;
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }

            let resp = if cancelled {
                IpcResponse::Error {
                    message: "cancelled by client".to_string(),
                    vram_exhausted: false,
                }
            } else {
                IpcResponse::Ack {
                    message: "completed".to_string(),
                }
            };
            let _ = a_worker.complete_request(req_id, resp);
        });

        // Submit request from client thread
        let a_client = Arc::clone(&arena);
        let client_thread = thread::spawn(move || {
            a_client.submit_request(IpcCommand::GetStatus, Some(Duration::from_secs(2)))
        });

        // Give client a moment to enqueue
        thread::sleep(Duration::from_millis(30));

        // Client 2 calls request_cancel: even with num_slots = 1 and slot 0 in-flight,
        // out-of-band cancellation succeeds immediately without blocking or timing out!
        let cancel_result = client.request_cancel(1).unwrap();
        assert!(
            cancel_result,
            "request_cancel must preemptively succeed out-of-band"
        );

        // The query should return Cancelled or Error
        let query_res = client_thread.join().unwrap();
        assert!(
            matches!(
                query_res,
                Err(PgBoltError::Cancelled(_))
                    | Err(PgBoltError::Bolt(_))
                    | Ok(IpcResponse::Error { .. })
            ),
            "expected cancelled or error response, got {query_res:?}"
        );

        worker_thread.join().unwrap();
        assert_eq!(arena.stats().free_slots, 1);
    }
}
