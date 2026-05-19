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

//! Dedicated tokio runtime that backs the async→sync bridge for cloud
//! Zarr stores.
//!
//! Cloud Zarr reads come in through the `zarrs::storage::storage_adapter::
//! async_to_sync::AsyncToSyncStorageAdapter`, which calls `block_on` on
//! every `get` / `list` / `bytes` request. We can't reuse DataFusion's
//! executor — `block_on`-ing the current runtime panics — so the bridge
//! gets its own multi-thread runtime, created lazily on first use.

use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};
use zarrs::storage::storage_adapter::async_to_sync::AsyncToSyncBlockOn;

static ZARR_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Return a handle to the process-wide Zarr IO runtime, building it on
/// first call. The thread count is configurable via the
/// `SEDONA_ZARR_RUNTIME_THREADS` environment variable (default 4).
pub(crate) fn handle() -> Handle {
    ZARR_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(
                    std::env::var("SEDONA_ZARR_RUNTIME_THREADS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .filter(|&n: &usize| n > 0)
                        .unwrap_or(4),
                )
                .enable_all()
                .thread_name("sedona-zarr-io")
                .build()
                .expect("build sedona-zarr io runtime")
        })
        .handle()
        .clone()
}

/// Tokio adapter implementing zarrs's `AsyncToSyncBlockOn` trait. Hands
/// futures to the dedicated runtime via [`handle`] rather than the
/// caller's current runtime, which would panic on nested `block_on`.
#[derive(Clone)]
pub(crate) struct TokioBlockOn(pub Handle);

impl TokioBlockOn {
    pub(crate) fn new() -> Self {
        Self(handle())
    }
}

impl AsyncToSyncBlockOn for TokioBlockOn {
    fn block_on<F: core::future::Future>(&self, future: F) -> F::Output {
        self.0.block_on(future)
    }
}
