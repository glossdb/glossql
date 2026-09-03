//! The S3 family of the data plane, on the engine's own storage crate.
//!
//! iceberg-rust moves every byte through `FileIO` → [`Storage`], and
//! leaves what implements that trait to the caller
//! (`io/storage/mod.rs` invites third-party implementations). This one
//! is built on `object_store` — the Apache crate DataFusion itself
//! runs on, already in the binary — so the process carries one storage
//! stack, not two.
//!
//! Configuration is layered by concern, between the `s3.*` properties
//! a table load answers with and `object_store`'s own environment
//! conventions (`AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT`, …): the catalog
//! says where the store is and, vending, whom it lets in; the
//! environment says how this process reaches it and supplies keys only
//! where the catalog vends none — see [`S3Storage::store`]. No surface
//! of ours either way.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};

/// Builds `S3Storage` for the REST catalog's FileIO — handed to the
/// catalog builder once; `build` runs per table load, with that load's
/// properties.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct S3StorageFactory;

#[typetag::serde(name = "GlossqlS3StorageFactory")]
impl StorageFactory for S3StorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(S3Storage::new(config.props().clone())))
    }
}

/// [`Storage`] over `object_store`'s S3 client.
///
/// A client there is bucket-scoped while the trait receives whole
/// `s3://bucket/key` paths, so clients are built lazily per bucket and
/// shared; in practice a table's FileIO sees one.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct S3Storage {
    props: HashMap<String, String>,
    #[serde(skip)]
    stores: Arc<Mutex<HashMap<String, Arc<AmazonS3>>>>,
}

/// The properties never appear whole: a table load's carry credentials.
impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("endpoint", &self.props.get(iceberg::io::S3_ENDPOINT))
            .field("region", &self.props.get(iceberg::io::S3_REGION))
            .finish_non_exhaustive()
    }
}

/// A whole path split into the bucket and the key within it. The S3
/// family of schemes reads identically (`s3`, `s3a`, `s3n`).
fn location(path: &str) -> Result<(&str, object_store::path::Path)> {
    let rest = ["s3://", "s3a://", "s3n://"]
        .iter()
        .find_map(|scheme| path.strip_prefix(scheme))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("not an S3-family path: `{path}`"),
            )
        })?;
    let (bucket, key) = rest.split_once('/').ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("an S3 path names a bucket and a key: `{path}`"),
        )
    })?;
    Ok((bucket, object_store::path::Path::from(key)))
}

/// An `object_store` failure as the engine's error; a missing object
/// keeps its kind readable for [`S3Storage::exists`].
fn io_error(e: object_store::Error, path: &str) -> Error {
    Error::new(ErrorKind::Unexpected, "the object store refused")
        .with_context("path", path.to_string())
        .with_source(e)
}

impl S3Storage {
    fn new(props: HashMap<String, String>) -> Self {
        S3Storage {
            props,
            stores: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The bucket's client, configured by concern: the catalog's
    /// properties say where the store is and — when it vends — whom it
    /// lets in; the environment's `AWS_*` conventions say how *this*
    /// process reaches it, which wins over the catalog's own vantage (a
    /// local rig's container hostname resolves for the catalog, not
    /// here), and supplies credentials only where the catalog vends
    /// none — a static key must never shadow a vended one.
    fn store(&self, bucket: &str) -> Result<Arc<AmazonS3>> {
        if let Some(store) = self.stores.lock().expect("stores lock").get(bucket) {
            return Ok(Arc::clone(store));
        }
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
        if let Some(endpoint) = self.props.get(iceberg::io::S3_ENDPOINT) {
            builder = builder.with_endpoint(endpoint);
            // A plain-http endpoint is a dev rig; saying the scheme is
            // saying it on purpose.
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let Some(region) = self.props.get(iceberg::io::S3_REGION) {
            builder = builder.with_region(region);
        }
        let vended = self.props.contains_key(iceberg::io::S3_ACCESS_KEY_ID)
            || self.props.contains_key(iceberg::io::S3_SESSION_TOKEN);
        if let Some(key) = self.props.get(iceberg::io::S3_ACCESS_KEY_ID) {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = self.props.get(iceberg::io::S3_SECRET_ACCESS_KEY) {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = self.props.get(iceberg::io::S3_SESSION_TOKEN) {
            builder = builder.with_token(token);
        }
        if let Some(path_style) = self.props.get(iceberg::io::S3_PATH_STYLE_ACCESS) {
            builder = builder.with_virtual_hosted_style_request(path_style != "true");
        }
        // The environment pass mirrors `AmazonS3Builder::from_env`,
        // minus credentials the catalog already vended.
        for (name, value) in std::env::vars() {
            if !name.starts_with("AWS_") {
                continue;
            }
            let Ok(key) = name.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() else {
                continue;
            };
            let credential = matches!(
                key,
                AmazonS3ConfigKey::AccessKeyId
                    | AmazonS3ConfigKey::SecretAccessKey
                    | AmazonS3ConfigKey::Token
            );
            if credential && vended {
                continue;
            }
            builder = builder.with_config(key, value);
        }
        let store = Arc::new(builder.build().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "the S3 configuration does not build")
                .with_context("bucket", bucket.to_string())
                .with_source(e)
        })?);
        self.stores
            .lock()
            .expect("stores lock")
            .insert(bucket.to_string(), Arc::clone(&store));
        Ok(store)
    }

    fn at(&self, path: &str) -> Result<(Arc<AmazonS3>, object_store::path::Path)> {
        let (bucket, key) = location(path)?;
        Ok((self.store(bucket)?, key))
    }
}

#[async_trait]
#[typetag::serde(name = "GlossqlS3Storage")]
impl Storage for S3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, key) = self.at(path)?;
        match store.head(&key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(io_error(e, path)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, key) = self.at(path)?;
        let meta = store.head(&key).await.map_err(|e| io_error(e, path))?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (store, key) = self.at(path)?;
        store
            .get(&key)
            .await
            .map_err(|e| io_error(e, path))?
            .bytes()
            .await
            .map_err(|e| io_error(e, path))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, key) = self.at(path)?;
        Ok(Box::new(S3FileRead {
            store,
            key,
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (store, key) = self.at(path)?;
        store
            .put(&key, bs.into())
            .await
            .map_err(|e| io_error(e, path))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, key) = self.at(path)?;
        let upload = store
            .put_multipart(&key)
            .await
            .map_err(|e| io_error(e, path))?;
        Ok(Box::new(S3FileWrite {
            upload: Some(WriteMultipart::new(upload)),
            path: path.to_string(),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (store, key) = self.at(path)?;
        match store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(io_error(e, path)),
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (store, key) = self.at(path)?;
        let locations = store.list(Some(&key)).map(|m| m.map(|m| m.location));
        let mut deleting = store.delete_stream(locations.boxed());
        while let Some(deleted) = deleting.next().await {
            match deleted {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(io_error(e, path)),
            }
        }
        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Ranged reads over one object — the shape the parquet reader drives,
/// with the coalescing done above this seam (iceberg's own reader).
struct S3FileRead {
    store: Arc<AmazonS3>,
    key: object_store::path::Path,
    path: String,
}

#[async_trait]
impl FileRead for S3FileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.key, range)
            .await
            .map_err(|e| io_error(e, &self.path))
    }
}

/// A multipart upload, completed at close — dropped without one, the
/// store never sees a completed object.
struct S3FileWrite {
    upload: Option<WriteMultipart>,
    path: String,
}

/// The concurrent part-uploads one writer may have in flight.
const UPLOAD_CONCURRENCY: usize = 8;

impl S3FileWrite {
    fn open(&mut self) -> Result<&mut WriteMultipart> {
        self.upload.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "written after close")
                .with_context("path", self.path.clone())
        })
    }
}

#[async_trait]
impl FileWrite for S3FileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let path = self.path.clone();
        let upload = self.open()?;
        upload
            .wait_for_capacity(UPLOAD_CONCURRENCY)
            .await
            .map_err(|e| io_error(e, &path))?;
        upload.put(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let path = self.path.clone();
        self.open()?;
        let upload = self.upload.take().expect("just opened");
        upload.finish().await.map_err(|e| io_error(e, &path))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// The path split: bucket and key, over the whole S3 scheme
    /// family; a path without both halves is refused by name.
    #[test]
    fn a_path_splits_into_bucket_and_key() {
        let (bucket, key) = super::location("s3://lake/ns/t/data/x.parquet").unwrap();
        assert_eq!(bucket, "lake");
        assert_eq!(key.as_ref(), "ns/t/data/x.parquet");
        let (bucket, _) = super::location("s3a://lake/k").unwrap();
        assert_eq!(bucket, "lake");
        assert!(super::location("file:///tmp/x").is_err());
        assert!(super::location("s3://bucketonly").is_err());
    }
}
