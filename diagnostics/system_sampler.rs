/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use sysinfo::{Disks, MemoryRefreshKind, Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

/// Background-refreshable cache of OS-level metrics. Owns a sysinfo::System +
/// Disks and refreshes them on a fixed cadence (driven externally by an
/// IntervalRunner in ServerMetrics). Readers see atomic getters; the refresh
/// thread holds the only Mutex.
///
/// CPU seconds are accumulated by integrating sysinfo's percentage-since-last-
/// refresh — sysinfo 0.33 doesn't expose a cumulative CPU counter directly.
/// The first refresh contributes zero by sysinfo's "first sample is used to fix
/// the value" semantics; subsequent refreshes contribute real values.
#[derive(Debug)]
pub(crate) struct SystemSampler {
    // Per-process metrics
    process_cpu_microseconds: AtomicU64,
    process_rss_bytes: AtomicU64,
    process_vsize_bytes: AtomicU64,
    process_open_fds: AtomicU64,
    process_max_fds: AtomicU64,

    // System-level metrics
    total_memory_bytes: AtomicU64,
    available_memory_bytes: AtomicU64,
    disk_total_bytes: AtomicU64,
    disk_available_bytes: AtomicU64,

    // Immutable after construction
    process_start_time_unix_seconds: u64,
    pid: Pid,
    data_directory: PathBuf,

    state: Mutex<SamplerState>,
}

#[derive(Debug)]
struct SamplerState {
    system: System,
    disks: Disks,
    // Wall-clock time of the previous refresh, used to integrate CPU% into seconds.
    last_refresh_at: Option<Instant>,
}

impl SystemSampler {
    pub(crate) fn new(data_directory: PathBuf) -> Self {
        let pid = sysinfo::get_current_pid().expect("Expected to resolve current PID");
        let process_start_time_unix_seconds =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_memory(MemoryRefreshKind::everything())
                .with_processes(ProcessRefreshKind::everything()),
        );
        let disks = Disks::new_with_refreshed_list();

        let sampler = Self {
            process_cpu_microseconds: AtomicU64::new(0),
            process_rss_bytes: AtomicU64::new(0),
            process_vsize_bytes: AtomicU64::new(0),
            process_open_fds: AtomicU64::new(0),
            process_max_fds: AtomicU64::new(0),
            total_memory_bytes: AtomicU64::new(0),
            available_memory_bytes: AtomicU64::new(0),
            disk_total_bytes: AtomicU64::new(0),
            disk_available_bytes: AtomicU64::new(0),
            process_start_time_unix_seconds,
            pid,
            data_directory,
            state: Mutex::new(SamplerState { system, disks, last_refresh_at: None }),
        };
        // Initial synchronous refresh so the first /diagnostics scrape isn't blank.
        // CPU stays 0 until the next refresh per sysinfo semantics.
        sampler.refresh();
        sampler
    }

    /// Re-read sysinfo state and update the cached atomics. Cheap enough to call
    /// every 15s; not cheap enough to call on every /diagnostics request.
    pub(crate) fn refresh(&self) {
        let mut state = self.state.lock().expect("Expected system sampler state lock acquisition");

        state.system.refresh_memory();
        state.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );
        self.total_memory_bytes.store(state.system.total_memory(), Ordering::Relaxed);
        self.available_memory_bytes.store(state.system.available_memory(), Ordering::Relaxed);

        let now = Instant::now();
        if let Some(process) = state.system.process(self.pid) {
            self.process_rss_bytes.store(process.memory(), Ordering::Relaxed);
            self.process_vsize_bytes.store(process.virtual_memory(), Ordering::Relaxed);

            // Integrate cpu_usage% × elapsed-since-previous-refresh. cpu_usage can
            // exceed 100 on multi-core; that's expected — cpu_seconds_total tracks
            // CPU-time, not wall-time, so 200% × 5s = 10 CPU-seconds is correct.
            //
            // Accumulate in microseconds (not milliseconds) so an idle process at
            // e.g. 0.005% CPU still contributes a non-zero delta per refresh and
            // the counter ticks monotonically. At 15s cadence with millisecond
            // storage, anything below ~0.007% per refresh floor-rounds to 0 and
            // never accumulates; microsecond storage cuts that floor by 1000×.
            if let Some(last) = state.last_refresh_at {
                let elapsed = now.duration_since(last).as_secs_f64();
                let cpu_pct = process.cpu_usage() as f64;
                let cpu_micros_delta = ((cpu_pct / 100.0) * elapsed * 1_000_000.0) as u64;
                if cpu_micros_delta > 0 {
                    self.process_cpu_microseconds.fetch_add(cpu_micros_delta, Ordering::Relaxed);
                }
            }
        }
        state.last_refresh_at = Some(now);

        state.disks.refresh(true);
        let disk_info = match self.data_directory.canonicalize() {
            Ok(path) => state.disks.iter().find(|disk| path.starts_with(disk.mount_point())),
            Err(_) => None,
        };
        if let Some(disk) = disk_info {
            self.disk_total_bytes.store(disk.total_space(), Ordering::Relaxed);
            self.disk_available_bytes.store(disk.available_space(), Ordering::Relaxed);
        }

        // File descriptor counts — Linux only. On macOS / Windows the metrics
        // remain at 0; consumers should treat 0 as "unsupported on this platform"
        // (the standard Prometheus process_* convention is to emit the line
        // regardless, with 0 for unsupported, to keep dashboards uniform).
        //
        // On read failure (e.g. /proc unmounted or limits format changed) we leave
        // the previous value in place rather than clearing to 0. Stale-but-bounded
        // is more useful for dashboards than a confusing dip-to-zero.
        #[cfg(target_os = "linux")]
        {
            if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
                // Subtract one for the dirfd that read_dir itself opens to enumerate
                // /proc/self/fd. That FD is included in the entry list (Linux quirk,
                // not Rust-specific). Without this we report N+1 every scrape.
                let count = (entries.count() as u64).saturating_sub(1);
                self.process_open_fds.store(count, Ordering::Relaxed);
            }
            if let Some(max) = read_max_open_files_linux() {
                self.process_max_fds.store(max, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn total_memory_bytes(&self) -> u64 {
        self.total_memory_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn available_memory_bytes(&self) -> u64 {
        self.available_memory_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn disk_total_bytes(&self) -> u64 {
        self.disk_total_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn disk_available_bytes(&self) -> u64 {
        self.disk_available_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn process_cpu_seconds_total(&self) -> f64 {
        self.process_cpu_microseconds.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
    pub(crate) fn process_resident_memory_bytes(&self) -> u64 {
        self.process_rss_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn process_virtual_memory_bytes(&self) -> u64 {
        self.process_vsize_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn process_open_fds(&self) -> u64 {
        self.process_open_fds.load(Ordering::Relaxed)
    }
    pub(crate) fn process_max_fds(&self) -> u64 {
        self.process_max_fds.load(Ordering::Relaxed)
    }
    pub(crate) fn process_start_time_unix_seconds(&self) -> u64 {
        self.process_start_time_unix_seconds
    }
}

/// Read the soft limit for open files from /proc/self/limits. The relevant line
/// looks like:
///
///     Max open files            1024                 1048576              files
///
/// where the soft limit is the first number after the label.
#[cfg(target_os = "linux")]
fn read_max_open_files_linux() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/self/limits").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("Max open files") {
            return rest.split_whitespace().next().and_then(|s| s.parse().ok());
        }
    }
    None
}
