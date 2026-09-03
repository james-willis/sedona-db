// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Page-cache hygiene for spill files.
//!
//! Spill files are written once, sequentially, and read back at most once.
//! Without intervention, every written byte lingers in the page cache as a
//! dirty page until kernel writeback catches up. Inside a memory-limited
//! cgroup (v2), those pages are charged to the container and are not
//! reclaimable while dirty, so a spill-heavy query on a host with a slow
//! disk can drive `memory.current` to the limit — and trigger the cgroup
//! OOM killer — while the process RSS stays small. (Observed in production:
//! RSS flat at ~2.3 GiB while `memory.current` climbed past 11 GiB of a
//! 12 GiB limit during spatial-join spilling.)
//!
//! Strategy (Linux): every `chunk` bytes, start asynchronous writeback for
//! the just-completed chunk (`sync_file_range(SYNC_FILE_RANGE_WRITE)`).
//! Once the writeback frontier is `MAX_LAG_CHUNKS` chunks ahead of the
//! drop frontier, wait for that span's writeback to complete
//! (`SYNC_FILE_RANGE_WAIT_BEFORE | WRITE | WAIT_AFTER`) and drop its pages
//! (`posix_fadvise(POSIX_FADV_DONTNEED)`). The steady-state cost is one
//! non-blocking syscall per chunk; the wait only blocks when the disk has
//! fallen a full lag window behind on this file — which is exactly the
//! condition under which dirty pages would otherwise accumulate without
//! bound. This mirrors PostgreSQL's `pg_flush_data` pattern, with the
//! blocking wait deferred behind a lag window so well-provisioned disks
//! never block. All calls are best-effort: errors (e.g. filesystems that
//! do not support them) are ignored.
//!
//! On non-Linux platforms this module is a no-op.
//!
//! The read side needs hygiene too: spilled data read back re-populates the
//! page cache as clean pages, and a cgroup riding `memory.current ==
//! memory.max` on retained clean cache can lose a reclaim race and be
//! OOM-killed. [`SpillReadAdvisor`] drops pages behind the read frontier and
//! drops the whole file when reading finishes.
//!
//! The chunk size can be tuned with the `SEDONA_SPILL_WRITEBACK_CHUNK_BYTES`
//! environment variable; `0` disables the hygiene entirely.

use std::fs::File;
#[cfg(target_os = "linux")]
use std::sync::OnceLock;

/// The default writeback chunk size: 8 MiB keeps the dirty-page overhang per
/// spill file small (at most two chunks) while issuing only one pair of
/// syscalls per 8 MiB of sequential spill throughput.
#[cfg(target_os = "linux")]
const DEFAULT_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// How many chunks the writeback frontier may run ahead of the drop frontier
/// before the writer blocks on writeback completion. 4 chunks (32 MiB at the
/// default chunk size) keeps the per-file dirty overhang small while letting
/// a healthy disk absorb bursts without ever blocking the writer.
#[cfg(target_os = "linux")]
const MAX_LAG_CHUNKS: u64 = 4;

#[cfg(target_os = "linux")]
fn chunk_bytes() -> u64 {
    static CHUNK: OnceLock<u64> = OnceLock::new();
    *CHUNK.get_or_init(|| {
        std::env::var("SEDONA_SPILL_WRITEBACK_CHUNK_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CHUNK_BYTES)
    })
}

/// Tracks how much of a sequentially written spill file has been queued for
/// writeback and how much has been dropped from the page cache.
#[derive(Debug, Default)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct SpillWritebackAdvisor {
    /// Bytes up to this offset have been queued for asynchronous writeback.
    synced: u64,
    /// Bytes up to this offset have been advised out of the page cache.
    dropped: u64,
}

impl SpillWritebackAdvisor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called after appending to the file; `written` is the current total
    /// file length in bytes. Queues writeback in whole-chunk steps; once the
    /// unqueued-to-undropped span reaches the lag window, waits for its
    /// writeback and drops its pages, bounding the dirty overhang of this
    /// file to roughly `MAX_LAG_CHUNKS + 1` chunks and throttling a producer
    /// that outruns the disk.
    #[cfg(target_os = "linux")]
    pub fn advise_written(&mut self, file: &File, written: u64) {
        use std::os::fd::AsRawFd;
        let chunk = chunk_bytes();
        if chunk == 0 {
            return;
        }
        let fd = file.as_raw_fd();
        while written >= self.synced + chunk {
            // SAFETY: plain syscalls on a valid, open fd; failures are ignored.
            unsafe {
                libc::sync_file_range(
                    fd,
                    self.synced as libc::off64_t,
                    chunk as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                );
            }
            self.synced += chunk;
            if self.synced - self.dropped >= MAX_LAG_CHUNKS * chunk {
                // The disk is a full lag window behind on this file: wait for
                // the span's writeback to finish, then drop its pages. This is
                // the backpressure that keeps the container's dirty page cache
                // bounded on hosts where writeback cannot keep up.
                let len = (self.synced - self.dropped) as libc::off64_t;
                unsafe {
                    libc::sync_file_range(
                        fd,
                        self.dropped as libc::off64_t,
                        len,
                        libc::SYNC_FILE_RANGE_WAIT_BEFORE
                            | libc::SYNC_FILE_RANGE_WRITE
                            | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                    );
                    libc::posix_fadvise(
                        fd,
                        self.dropped as libc::off64_t,
                        len,
                        libc::POSIX_FADV_DONTNEED,
                    );
                }
                self.dropped = self.synced;
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn advise_written(&mut self, _file: &File, _written: u64) {}

    /// Called once after the file is fully written: wait for the tail's
    /// writeback and drop the whole file from the cache. The wait is bounded
    /// by one lag window of writeback (the same budget the steady-state
    /// backpressure already imposes); without it, DONTNEED skips the
    /// still-dirty tail and every finished-but-unread spill file retains up
    /// to a lag window of since-cleaned pages that nothing ever revisits -
    /// multiplied across the many files of a multi-round repartition, that
    /// retention reaches gigabytes on a memory-limited container. Files
    /// smaller than one chunk are left alone: they may be read back moments
    /// later and are too small to endanger the container.
    #[cfg(target_os = "linux")]
    pub fn advise_finished(&mut self, file: &File, written: u64) {
        use std::os::fd::AsRawFd;
        let chunk = chunk_bytes();
        if chunk == 0 || written < chunk {
            return;
        }
        let fd = file.as_raw_fd();
        if written > self.dropped {
            // Wait for everything not yet dropped to reach disk, then drop
            // it. The span is at most a lag window plus the final partial
            // chunk; anything older was already waited on and dropped.
            // SAFETY: see advise_written.
            unsafe {
                libc::sync_file_range(
                    fd,
                    self.dropped as libc::off64_t,
                    (written - self.dropped) as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WAIT_BEFORE
                        | libc::SYNC_FILE_RANGE_WRITE
                        | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                );
            }
            self.synced = written;
        }
        // Length 0 means "to the end of the file"; everything is clean now,
        // so this drops the file's entire cache footprint.
        // SAFETY: see advise_written.
        unsafe {
            libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED);
        }
        self.dropped = written;
    }

    #[cfg(not(target_os = "linux"))]
    pub fn advise_finished(&mut self, _file: &File, _written: u64) {}
}

/// Read-side page-cache hygiene for spill files.
///
/// Bounding the dirty side is not enough inside a memory-limited cgroup:
/// spilled data read back during the probe/merge phase re-populates the page
/// cache as CLEAN pages, and a container that rides `memory.current ==
/// memory.max` on retained clean cache can still lose a reclaim race and get
/// OOM-killed (observed at teardown: 9.46 GB of inactive_file with process
/// RSS flat at 2.39 GiB). Spill files are read exactly once, sequentially,
/// so the pages behind the read frontier have no future value: drop them as
/// the reader advances, and drop the whole file when the reader is done.
#[derive(Debug, Default)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct SpillReadAdvisor {
    /// Bytes up to this offset have been advised out of the page cache.
    dropped: u64,
    /// Whether the whole file has already been advised away.
    finished: bool,
}

impl SpillReadAdvisor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called once after opening a spill file for reading: declare sequential
    /// access so kernel readahead stays effective without over-caching.
    #[cfg(target_os = "linux")]
    pub fn advise_opened(file: &File) {
        use std::os::fd::AsRawFd;
        if chunk_bytes() == 0 {
            return;
        }
        // SAFETY: plain syscall on a valid, open fd; failures are ignored.
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn advise_opened(_file: &File) {}

    /// Called after consuming data from the file: drop whole chunks of page
    /// cache behind the underlying descriptor's read position. Everything
    /// before that position has either been consumed or sits in the reader's
    /// userspace buffer, so the cached pages have no future value.
    #[cfg(target_os = "linux")]
    pub fn advise_progress(&mut self, file: &File) {
        use std::os::fd::AsRawFd;
        let chunk = chunk_bytes();
        if chunk == 0 || self.finished {
            return;
        }
        let fd = file.as_raw_fd();
        // SAFETY: see advise_opened.
        let pos = unsafe { libc::lseek(fd, 0, libc::SEEK_CUR) };
        if pos <= 0 {
            return;
        }
        let droppable = (pos as u64 / chunk) * chunk;
        if droppable > self.dropped {
            // SAFETY: see advise_opened.
            unsafe {
                libc::posix_fadvise(
                    fd,
                    self.dropped as libc::off64_t,
                    (droppable - self.dropped) as libc::off64_t,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
            self.dropped = droppable;
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn advise_progress(&mut self, _file: &File) {}

    /// Called when reading ends (stream exhausted or reader dropped): drop
    /// the whole file from the page cache, including any dirty tail retained
    /// from the write side that has become clean since.
    #[cfg(target_os = "linux")]
    pub fn advise_closed(&mut self, file: &File) {
        use std::os::fd::AsRawFd;
        if chunk_bytes() == 0 || self.finished {
            return;
        }
        // Length 0 means "to the end of the file".
        // SAFETY: see advise_opened.
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
        self.finished = true;
    }

    #[cfg(not(target_os = "linux"))]
    pub fn advise_closed(&mut self, _file: &File) {}
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Write;

    /// Smoke test: the advisor's syscalls must be harmless on a real file and
    /// its offsets must advance in whole chunks with a one-chunk drop lag.
    #[test]
    fn advises_written_file_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill");
        let mut file = File::create(&path).unwrap();
        let chunk = chunk_bytes();
        if chunk == 0 {
            return;
        }

        let mut advisor = SpillWritebackAdvisor::new();
        let payload = vec![7u8; 1024 * 1024];
        let mut written = 0u64;
        while written < (MAX_LAG_CHUNKS + 1) * chunk {
            file.write_all(&payload).unwrap();
            written += payload.len() as u64;
            advisor.advise_written(&file, written);
        }
        // The writeback frontier advances chunk by chunk; the drop frontier
        // catches up in lag-window steps once the lag is reached.
        assert_eq!(advisor.synced, (MAX_LAG_CHUNKS + 1) * chunk);
        assert_eq!(advisor.dropped, MAX_LAG_CHUNKS * chunk);

        advisor.advise_finished(&file, written);
        assert_eq!(advisor.synced, written);
        assert_eq!(advisor.dropped, written);
    }

    /// Smoke test for the read side: syscalls are harmless and the drop
    /// frontier follows the descriptor's read position in whole chunks.
    #[test]
    fn advises_read_file_in_chunks() {
        use std::io::Read;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill_read");
        let chunk = chunk_bytes();
        if chunk == 0 {
            return;
        }
        std::fs::write(&path, vec![7u8; (2 * chunk + chunk / 2) as usize]).unwrap();

        let mut file = File::open(&path).unwrap();
        SpillReadAdvisor::advise_opened(&file);
        let mut advisor = SpillReadAdvisor::new();
        let mut buf = vec![0u8; 1024 * 1024];
        let mut consumed = 0u64;
        loop {
            let n = file.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            consumed += n as u64;
            advisor.advise_progress(&file);
            assert_eq!(advisor.dropped, (consumed / chunk) * chunk);
        }
        advisor.advise_closed(&file);
        assert!(advisor.finished);
    }
}
