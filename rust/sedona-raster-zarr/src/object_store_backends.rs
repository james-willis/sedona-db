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

//! Per-scheme `object_store` builders.
//!
//! Each function takes a URI and a [`ZarrCredentialOptions`] and returns
//! `(Arc<dyn ObjectStore>, group_path_in_store)`. The group path is the
//! URI's path component minus the bucket / container prefix, so the
//! caller can pass it straight to `Group::open`.
//!
//! Credentials follow a layered model: each builder starts with
//! `*Builder::from_env()` (which picks up the standard env vars for the
//! cloud) and then overlays explicit options from
//! [`ZarrCredentialOptions`]. This matches the existing object-store
//! configuration pattern in `rust/sedona/src/object_storage.rs` so users
//! only have to learn one set of keys.

use std::sync::Arc;

use arrow_schema::ArrowError;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::ObjectStore;
use url::Url;

use crate::credentials::ZarrCredentialOptions;

/// Build an S3 object store from `s3://bucket/path...`.
pub(crate) fn build_s3(
    uri: &str,
    creds: &ZarrCredentialOptions,
) -> Result<(Arc<dyn ObjectStore>, String), ArrowError> {
    let url = parse_url(uri)?;
    let mut builder = AmazonS3Builder::from_env().with_url(uri);

    if let Some(v) = creds.get("aws.access_key_id") {
        builder = builder.with_access_key_id(v);
    }
    if let Some(v) = creds.get("aws.secret_access_key") {
        builder = builder.with_secret_access_key(v);
    }
    if let Some(v) = creds.get("aws.session_token") {
        builder = builder.with_token(v);
    }
    if let Some(v) = creds.get("aws.region") {
        builder = builder.with_region(v);
    }
    if let Some(v) = creds.get("aws.endpoint") {
        builder = builder.with_endpoint(v);
    }
    if let Some(v) = creds.get("aws.allow_http") {
        builder = builder.with_allow_http(parse_bool("aws.allow_http", v)?);
    }
    if let Some(v) = creds.get("aws.skip_signature") {
        builder = builder.with_skip_signature(parse_bool("aws.skip_signature", v)?);
    }

    let store = builder
        .build()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    let path = url.path().trim_start_matches('/').to_string();
    Ok((Arc::new(store), path))
}

/// Build a GCS object store from `gs://bucket/path...` (also accepts
/// `gcs://`).
pub(crate) fn build_gcs(
    uri: &str,
    creds: &ZarrCredentialOptions,
) -> Result<(Arc<dyn ObjectStore>, String), ArrowError> {
    let url = parse_url(uri)?;
    let mut builder = GoogleCloudStorageBuilder::from_env().with_url(uri);

    if let Some(v) = creds.get("gcp.service_account_path") {
        builder = builder.with_service_account_path(v);
    }
    if let Some(v) = creds.get("gcp.service_account_key") {
        builder = builder.with_service_account_key(v);
    }
    if let Some(v) = creds.get("gcp.application_credentials_path") {
        builder = builder.with_application_credentials(v);
    }

    let store = builder
        .build()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    let path = url.path().trim_start_matches('/').to_string();
    Ok((Arc::new(store), path))
}

/// Build an Azure object store from `az://`, `abfs://`, or `abfss://`.
pub(crate) fn build_azure(
    uri: &str,
    creds: &ZarrCredentialOptions,
) -> Result<(Arc<dyn ObjectStore>, String), ArrowError> {
    let url = parse_url(uri)?;
    let mut builder = MicrosoftAzureBuilder::from_env().with_url(uri);

    if let Some(v) = creds.get("azure.account_name") {
        builder = builder.with_account(v);
    }
    if let Some(v) = creds.get("azure.account_key") {
        builder = builder.with_access_key(v);
    }
    if let Some(v) = creds.get("azure.client_id") {
        builder = builder.with_client_id(v);
    }
    if let Some(v) = creds.get("azure.client_secret") {
        builder = builder.with_client_secret(v);
    }
    if let Some(v) = creds.get("azure.tenant_id") {
        builder = builder.with_tenant_id(v);
    }
    if let Some(v) = creds.get("azure.use_emulator") {
        builder = builder.with_use_emulator(parse_bool("azure.use_emulator", v)?);
    }

    let store = builder
        .build()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    let path = url.path().trim_start_matches('/').to_string();
    Ok((Arc::new(store), path))
}

/// Build an HTTP(S) object store. The base URL is `scheme://host[:port]`
/// and the group path is the URL path.
pub(crate) fn build_http(
    uri: &str,
    _creds: &ZarrCredentialOptions,
) -> Result<(Arc<dyn ObjectStore>, String), ArrowError> {
    let url = parse_url(uri)?;
    let base = match url.port() {
        Some(port) => format!(
            "{}://{}:{}",
            url.scheme(),
            url.host_str().ok_or_else(|| missing_host(uri))?,
            port
        ),
        None => format!(
            "{}://{}",
            url.scheme(),
            url.host_str().ok_or_else(|| missing_host(uri))?
        ),
    };
    let store = HttpBuilder::new()
        .with_url(base)
        .build()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    let path = url.path().trim_start_matches('/').to_string();
    Ok((Arc::new(store), path))
}

fn parse_url(uri: &str) -> Result<Url, ArrowError> {
    Url::parse(uri)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("invalid Zarr URI {uri:?}: {e}")))
}

fn missing_host(uri: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Zarr URI {uri:?} has no host component"))
}

fn parse_bool(key: &str, value: &str) -> Result<bool, ArrowError> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "credential option {key:?} must be a boolean (true/false); got {value:?}"
        ))),
    }
}
