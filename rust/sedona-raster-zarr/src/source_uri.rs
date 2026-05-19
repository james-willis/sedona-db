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

//! Zarr URI helpers — group locators and per-chunk OutDb anchors.
//!
//! Two URI shapes flow through the loader:
//!
//! 1. **Group URI** (the loader entry-point argument). Identifies a Zarr
//!    group on a backend, e.g. `file:///tmp/datacube.zarr`,
//!    `s3://bucket/datacube.zarr/2024`, or a bare local path. Phase 1
//!    accepts `file://` and bare-path; cloud schemes ship with the
//!    resolver.
//! 2. **Chunk anchor URI** (written into a band's `outdb_uri`). Addresses
//!    one chunk in one array within a group:
//!    `<store-uri>#array=<array-path>&chunk=<i0>,<i1>,...`. The store URI
//!    is the original group URI verbatim (whatever scheme that uses);
//!    array path and chunk indices both live in the fragment so the
//!    store URI is unambiguous even when both contain `/` (e.g.
//!    `s3://bucket/foo.zarr/2024` + `subgroup/B01`). Inner-chunk indices
//!    always — sharding is a storage detail the resolver handles.
//!
//!    The "this is a zarr anchor" signal lives in the band's
//!    `outdb_format = "zarr"` field, not in a URI scheme prefix — matches
//!    the GDAL OutDb convention and lets the format-keyed dispatcher
//!    route without parsing URI schemes.

use std::sync::Arc;

use arrow_schema::ArrowError;
use zarrs::storage::storage_adapter::async_to_sync::AsyncToSyncStorageAdapter;
use zarrs_filesystem::FilesystemStore;
use zarrs_object_store::AsyncObjectStore;

use crate::credentials::ZarrCredentialOptions;
use crate::object_store_backends::{build_azure, build_gcs, build_http, build_s3};
use crate::runtime::TokioBlockOn;
use crate::storage::ZarrStorage;

/// Parts of a chunk-anchor URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAnchor {
    /// Original store URI for the *group* (e.g. `file:///tmp/foo.zarr`,
    /// `s3://bucket/foo.zarr/2024`). Same value across every chunk of every
    /// array in a group.
    pub store_uri: String,
    /// Array's path within the store (e.g. `temperature`, `subgroup/B01`).
    pub array_path: String,
    /// Chunk's position in the array's inner chunk grid (one index per
    /// array dimension).
    pub chunk_indices: Vec<u64>,
}

/// Build a chunk anchor URI from its parts.
///
/// Format: `<store_uri>#array=<array_path>&chunk=<i0>,<i1>,...`. The
/// store URI is the original group URI verbatim; array path and chunk
/// indices live in the fragment so the store URI is unambiguous even
/// when path components themselves contain `/`. "This is zarr" lives
/// in the band's `outdb_format` field rather than in a URI scheme.
pub fn build_chunk_anchor(store_uri: &str, array_path: &str, chunk_indices: &[u64]) -> String {
    let indices = chunk_indices
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let array = array_path.trim_start_matches('/');
    format!("{store_uri}#array={array}&chunk={indices}")
}

/// Parse a chunk-anchor URI back into its parts.
///
/// Strict: rejects URIs that don't carry both `array=` and `chunk=`
/// fragment parameters with valid values.
pub fn parse_chunk_anchor(uri: &str) -> Result<ChunkAnchor, ArrowError> {
    let (store_uri, fragment) = uri.split_once('#').ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "chunk anchor URI missing `#array=...&chunk=...` fragment: {uri}"
        ))
    })?;

    let mut array_path: Option<&str> = None;
    let mut chunk_str: Option<&str> = None;
    for pair in fragment.split('&') {
        if let Some(v) = pair.strip_prefix("array=") {
            array_path = Some(v);
        } else if let Some(v) = pair.strip_prefix("chunk=") {
            chunk_str = Some(v);
        }
        // Unknown fragment params are ignored — forward-compatible for
        // future per-anchor extensions (e.g. version pins).
    }

    let array_path = array_path
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "chunk anchor URI fragment missing `array=` parameter: {uri}"
            ))
        })?
        .to_string();
    let indices_str = chunk_str.ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "chunk anchor URI fragment missing `chunk=` parameter: {uri}"
        ))
    })?;
    if indices_str.is_empty() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "chunk anchor URI has empty index list: {uri}"
        )));
    }
    let chunk_indices = indices_str
        .split(',')
        .map(|s| {
            s.parse::<u64>().map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "chunk index `{s}` is not a non-negative integer in {uri}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ChunkAnchor {
        store_uri: store_uri.to_string(),
        array_path,
        chunk_indices,
    })
}

/// Dispatch a user-supplied group URI to a backing storage handle.
///
/// Returns `(storage, group_path)` where `group_path` is the path of the
/// group inside the store (always `"/"` for filesystem stores; the
/// URL-path component for cloud stores). The caller passes both straight
/// to `zarrs::Group::open(storage, &group_path)`.
///
/// Supported schemes:
///
/// | Scheme(s)                            | Backend                  |
/// |--------------------------------------|--------------------------|
/// | `file://`, bare path                 | `FilesystemStore`        |
/// | `s3://`                              | `AmazonS3` via object_store |
/// | `gs://`, `gcs://`                    | `GoogleCloudStorage` via object_store |
/// | `az://`, `abfs://`, `abfss://`       | `MicrosoftAzure` via object_store |
/// | `http://`, `https://`                | `HttpStore` via object_store |
///
/// Cloud backends go through the async→sync bridge in
/// [`crate::runtime`], so callers don't need to spin up their own tokio
/// runtime.
pub fn parse_zarr_uri(
    uri: &str,
    creds: &ZarrCredentialOptions,
) -> Result<(ZarrStorage, String), ArrowError> {
    let scheme = uri.split_once("://").map(|(s, _)| s);

    let (object_store, path) = match scheme {
        Some("file") => {
            let rest = uri.strip_prefix("file://").unwrap_or(uri);
            return filesystem_storage(rest);
        }
        None => return filesystem_storage(uri),
        Some("s3") => build_s3(uri, creds)?,
        Some("gs") | Some("gcs") => build_gcs(uri, creds)?,
        Some("az") | Some("abfs") | Some("abfss") => build_azure(uri, creds)?,
        Some("http") | Some("https") => build_http(uri, creds)?,
        Some(other) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "unsupported Zarr URI scheme {other:?} in {uri}; \
                 expected one of file/s3/gs/gcs/az/abfs/abfss/http/https or a bare path"
            )));
        }
    };

    let async_store = Arc::new(AsyncObjectStore::new(object_store));
    let storage: ZarrStorage = Arc::new(AsyncToSyncStorageAdapter::new(
        async_store,
        TokioBlockOn::new(),
    ));
    let group_path = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path.trim_start_matches('/'))
    };
    Ok((storage, group_path))
}

fn filesystem_storage(path: &str) -> Result<(ZarrStorage, String), ArrowError> {
    let store = FilesystemStore::new(std::path::PathBuf::from(path)).map_err(|e| {
        ArrowError::ExternalError(Box::new(std::io::Error::other(format!(
            "failed to open Zarr filesystem store at {path}: {e}"
        ))))
    })?;
    let storage: ZarrStorage = Arc::new(store);
    Ok((storage, "/".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_anchor_basic() {
        let uri = build_chunk_anchor("file:///tmp/datacube.zarr", "temperature", &[47, 2, 3]);
        assert_eq!(
            uri,
            "file:///tmp/datacube.zarr#array=temperature&chunk=47,2,3"
        );
    }

    #[test]
    fn build_anchor_strips_leading_slash_on_array_path() {
        let uri = build_chunk_anchor("file:///tmp/foo.zarr", "/array", &[0]);
        assert_eq!(uri, "file:///tmp/foo.zarr#array=array&chunk=0");
    }

    #[test]
    fn build_anchor_with_nested_array_path() {
        let uri = build_chunk_anchor("s3://bucket/foo.zarr/2024", "subgroup/B01", &[1, 5]);
        assert_eq!(
            uri,
            "s3://bucket/foo.zarr/2024#array=subgroup/B01&chunk=1,5"
        );
    }

    #[test]
    fn parse_anchor_basic() {
        let anchor =
            parse_chunk_anchor("file:///tmp/datacube.zarr#array=temperature&chunk=47,2,3").unwrap();
        assert_eq!(anchor.store_uri, "file:///tmp/datacube.zarr");
        assert_eq!(anchor.array_path, "temperature");
        assert_eq!(anchor.chunk_indices, vec![47, 2, 3]);
    }

    #[test]
    fn parse_anchor_with_s3_store_and_nested_array() {
        let anchor =
            parse_chunk_anchor("s3://bucket/foo.zarr/2024#array=subgroup/B01&chunk=0,0,0").unwrap();
        assert_eq!(anchor.store_uri, "s3://bucket/foo.zarr/2024");
        assert_eq!(anchor.array_path, "subgroup/B01");
        assert_eq!(anchor.chunk_indices, vec![0, 0, 0]);
    }

    #[test]
    fn parse_anchor_rejects_missing_fragment() {
        // No `#` means no fragment, so neither `array=` nor `chunk=` is
        // present. The parser surfaces "missing fragment".
        let err = parse_chunk_anchor("file:///foo.zarr")
            .unwrap_err()
            .to_string();
        assert!(err.contains("fragment"), "{err}");
    }

    #[test]
    fn parse_anchor_rejects_missing_array_param() {
        let err = parse_chunk_anchor("file:///foo.zarr#chunk=0")
            .unwrap_err()
            .to_string();
        assert!(err.contains("array="), "{err}");
    }

    #[test]
    fn parse_anchor_rejects_missing_chunk_param() {
        let err = parse_chunk_anchor("file:///foo.zarr#array=a")
            .unwrap_err()
            .to_string();
        assert!(err.contains("chunk="), "{err}");
    }

    #[test]
    fn parse_anchor_rejects_empty_indices() {
        let err = parse_chunk_anchor("file:///foo.zarr#array=a&chunk=")
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty"), "{err}");
    }

    #[test]
    fn parse_anchor_rejects_non_integer_index() {
        let err = parse_chunk_anchor("file:///foo.zarr#array=a&chunk=0,abc,2")
            .unwrap_err()
            .to_string();
        assert!(err.contains("abc"), "{err}");
    }

    #[test]
    fn parse_anchor_ignores_unknown_fragment_params() {
        // Forward-compatible: extra `&key=value` pairs in the fragment
        // shouldn't break parsing — they're reserved for future use.
        let anchor = parse_chunk_anchor("file:///foo.zarr#array=t&chunk=0,0&version=v3").unwrap();
        assert_eq!(anchor.array_path, "t");
        assert_eq!(anchor.chunk_indices, vec![0, 0]);
    }

    #[test]
    fn anchor_roundtrip() {
        let original = ChunkAnchor {
            store_uri: "file:///tmp/foo.zarr".into(),
            array_path: "subgroup/B01".into(),
            chunk_indices: vec![10, 20, 30],
        };
        let uri = build_chunk_anchor(
            &original.store_uri,
            &original.array_path,
            &original.chunk_indices,
        );
        let parsed = parse_chunk_anchor(&uri).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn parse_zarr_uri_file_scheme() {
        let creds = ZarrCredentialOptions::default();
        // FilesystemStore::new succeeds even on a non-existent path —
        // errors are surfaced at first read. We just need the dispatch
        // to land on the filesystem branch and report `/` as the group
        // path.
        let (_storage, group_path) = parse_zarr_uri("file:///tmp/foo.zarr", &creds).unwrap();
        assert_eq!(group_path, "/");
    }

    #[test]
    fn parse_zarr_uri_bare_path() {
        let creds = ZarrCredentialOptions::default();
        let (_storage, group_path) = parse_zarr_uri("/tmp/foo.zarr", &creds).unwrap();
        assert_eq!(group_path, "/");
    }

    #[test]
    fn parse_zarr_uri_rejects_unknown_scheme() {
        let creds = ZarrCredentialOptions::default();
        // `Arc<dyn ReadableListableStorageTraits>` is not `Debug`, so we
        // can't use `unwrap_err` here — match on the Result directly.
        let err = match parse_zarr_uri("ftp://example.com/foo.zarr", &creds) {
            Ok(_) => panic!("expected error for unknown scheme"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("ftp"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
    }
}
