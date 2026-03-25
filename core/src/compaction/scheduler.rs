use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;

use crate::cache::BlockCache;
use crate::compaction::leveled::LeveledCompaction;
use crate::manifest::VersionSet;

/// A background thread that periodically checks compaction triggers and runs
/// compaction when needed.
///
/// The scheduler holds shared references to the VersionSet and BlockCache via
/// Arc<Mutex<...>>. It wakes up every `poll_interval` and runs at most one
/// compaction round per wake.
pub struct CompactionScheduler {
    thread_handle: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

/// State shared between the scheduler thread and the storage engine.
pub struct SharedCompactionState {
    pub version: Arc<Mutex<VersionSet>>,
    pub cache: Arc<BlockCache>,
    pub data_dir: std::path::PathBuf,
    pub block_size_bytes: usize,
    pub bloom_bits_per_key: usize,
    pub l0_trigger: usize,
}

impl CompactionScheduler {
    /// Spawn the background compaction thread.
    pub fn start(state: Arc<SharedCompactionState>, poll_interval: Duration) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let handle = thread::Builder::new()
            .name("oxendb-compact".into())
            .spawn(move || {
                log::info!("Compaction scheduler started");
                while !shutdown_clone.load(Ordering::Relaxed) {
                    // Sleep first, then check
                    thread::sleep(poll_interval);

                    if shutdown_clone.load(Ordering::Relaxed) {
                        break;
                    }

                    let result = {
                        let mut version = state.version.lock();
                        let mut compaction = LeveledCompaction::new(
                            &mut version,
                            &state.data_dir,
                            state.block_size_bytes,
                            state.bloom_bits_per_key,
                            state.cache.clone(),
                            state.l0_trigger,
                        );
                        compaction.maybe_compact()
                    };

                    match result {
                        Ok(0) => {} // nothing to compact
                        Ok(n) => log::info!("Compaction round completed {n} job(s)"),
                        Err(e) => log::error!("Compaction error: {e}"),
                    }
                }
                log::info!("Compaction scheduler stopped");
            })
            .expect("Failed to spawn compaction thread");

        CompactionScheduler {
            thread_handle: Some(handle),
            shutdown,
        }
    }

    /// Signal the background thread to stop and wait for it to exit.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        self.shutdown();
    }
}
