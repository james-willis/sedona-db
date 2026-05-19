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

//! Trait-object storage type shared by every backend (filesystem and
//! cloud). The loader is generic over this single type so backend
//! dispatch happens once at URI parse time and the rest of the loader
//! stays scheme-agnostic.

use std::sync::Arc;

use zarrs::storage::ReadableListableStorageTraits;

/// `Arc<dyn ReadableListableStorageTraits>` — the storage handle returned
/// by [`crate::source_uri::parse_zarr_uri`] and accepted by every
/// `Group::open` / `Array::open` call in the loader.
///
/// Filesystem stores are wrapped via `Arc::new(FilesystemStore::new(...))`;
/// cloud stores are wrapped via
/// `AsyncToSyncStorageAdapter::new(AsyncObjectStore::new(object_store), TokioBlockOn(handle))`.
/// Both produce values assignable to this alias.
pub type ZarrStorage = Arc<dyn ReadableListableStorageTraits>;
