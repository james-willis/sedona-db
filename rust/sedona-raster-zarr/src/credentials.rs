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

//! Credentials carried from the UDTF call site to the per-scheme
//! `object_store` builders.
//!
//! Keys are namespaced by backend (`aws.*`, `gcp.*`, `azure.*`). Empty
//! values are treated as unset so an empty options JSON falls through
//! cleanly to env-var-based authentication. Unknown keys are ignored so
//! a user's `mode` / `rows_per_batch` / `num_partitions` JSON can be
//! flattened into the same map at the call site.

use std::collections::HashMap;

/// Flat string map of credential overrides keyed by `<scheme>.<field>`.
///
/// The map is small (≤ ~10 entries in practice) and the builders only
/// read it once per group open, so a plain `HashMap<String, String>`
/// keeps the surface honest without imposing a typed schema users have
/// to learn separately from the underlying `object_store` builders.
#[derive(Debug, Default, Clone)]
pub struct ZarrCredentialOptions {
    map: HashMap<String, String>,
}

impl ZarrCredentialOptions {
    /// Wrap a pre-built credential map.
    pub fn new(map: HashMap<String, String>) -> Self {
        Self { map }
    }

    /// Look up an override by full key (e.g. `aws.region`). Returns
    /// `None` if absent or empty.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.map
            .get(key)
            .map(String::as_str)
            .filter(|s| !s.is_empty())
    }

    /// Iterate over `(key, value)` pairs whose key starts with the given
    /// prefix, stripping the prefix on the way out. Used by per-backend
    /// builders to extract just their own namespace.
    pub fn iter_namespace<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.map.iter().filter_map(move |(k, v)| {
            k.strip_prefix(prefix)
                .filter(|_| !v.is_empty())
                .map(|stripped| (stripped, v.as_str()))
        })
    }
}

impl From<HashMap<String, String>> for ZarrCredentialOptions {
    fn from(map: HashMap<String, String>) -> Self {
        Self::new(map)
    }
}
