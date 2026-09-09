//! The data plane behind every catalog: iceberg's `Storage` seam on the
//! engine's own storage crate.
//!
//! iceberg-rust moves every byte through `FileIO` → [`Storage`], and
//! leaves what implements that trait to the caller
//! (`io/storage/mod.rs` invites third-party implementations). This one
//! is built on `object_store` — the Apache crate DataFusion itself
//! runs on, already in the binary — so the process carries one storage
//! stack, not two. Two families answer: S3 (`s3://`, `s3a://`,
//! `s3n://`) and Azure (`abfss://container@account.dfs.core.windows.net/…`,
//! `abfs://`, `az://`, `azure://`, `adl://`, `wasbs://`). A client is
//! scoped to one bucket or container, so clients are built lazily per
//! authority and shared; in practice a table's FileIO sees one.
//!
//! Configuration is layered by concern, between the `s3.*` / `adls.*`
//! properties a table load answers with and object_store's own
//! environment conventions (`AWS_*`, `AZURE_*`): the catalog says where
//! the store is and, vending, whom it lets in; the environment says how
//! this process reaches it and supplies credentials only where the
//! catalog vends none — a static key must never shadow a vended one.
//! On Azure with nothing set at all, object_store's client reads the
//! managed identity from `IDENTITY_ENDPOINT` and `IDENTITY_HEADER`,
//! which is the Container Apps arrangement: the identity is the
//! credential and there is no secret. No surface of ours either way.

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
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
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
use url::Url;

/// Where a warehouse lives: a directory on this machine, or a location
/// in an object store this seam reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Warehouse {
    Local(PathBuf),
    Remote(String),
}

impl Warehouse {
    /// A path, `file://<path>`, or a remote location in one of the two
    /// families. Anything else is refused by its scheme.
    pub fn parse(warehouse: &str) -> crate::Result<Self> {
        let w = warehouse.trim();
        let Some((scheme, rest)) = w.split_once("://") else {
            return Ok(Warehouse::Local(PathBuf::from(w)));
        };
        match scheme.to_ascii_lowercase().as_str() {
            "file" => Ok(Warehouse::Local(PathBuf::from(rest))),
            s if family(s).is_some() => Ok(Warehouse::Remote(w.trim_end_matches('/').to_string())),
            other => Err(crate::Error::Workspace(format!(
                "the warehouse scheme `{other}` is not one this binary reaches — a directory, \
                 file://, s3://, or abfss://container@account.dfs.core.windows.net/"
            ))),
        }
    }
}

/// The two families of stores, by scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    S3,
    Azure,
}

fn family(scheme: &str) -> Option<Family> {
    match scheme {
        "s3" | "s3a" | "s3n" => Some(Family::S3),
        "abfs" | "abfss" | "az" | "azure" | "adl" | "wasb" | "wasbs" => Some(Family::Azure),
        _ => None,
    }
}

/// A whole location split into the store it lives in and the key
/// within it: `authority` keys the shared client, `root` is what the
/// family's builder parses (the URL up to the path).
struct Location {
    family: Family,
    authority: String,
    root: String,
    key: object_store::path::Path,
}

fn location(path: &str) -> Result<Location> {
    let invalid = |what: &str| Error::new(ErrorKind::DataInvalid, format!("{what}: `{path}`"));
    let url =
        Url::parse(path).map_err(|e| invalid("not a location this seam reaches").with_source(e))?;
    let scheme = url.scheme().to_ascii_lowercase();
    let family = family(&scheme).ok_or_else(|| invalid("not an S3 or Azure location"))?;
    let host = url
        .host_str()
        .ok_or_else(|| invalid("a location names a bucket or a container"))?;
    let authority = if url.username().is_empty() {
        host.to_string()
    } else {
        format!("{}@{host}", url.username())
    };
    // The Hadoop `wasb(s)` spelling is the `abfss` layout under another
    // scheme; object_store's builder parses the latter.
    let root_scheme = match scheme.as_str() {
        "wasb" | "wasbs" => "abfss",
        s => s,
    };
    Ok(Location {
        family,
        authority: format!("{scheme}://{authority}"),
        root: format!("{root_scheme}://{authority}"),
        key: object_store::path::Path::from(url.path().trim_start_matches('/')),
    })
}

/// Builds [`ObjectStorage`] for a catalog's FileIO — handed to the
/// catalog builder once; `build` runs per table load, with that load's
/// properties (a SQL catalog delivers none: the environment configures).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ObjectStorageFactory;

#[typetag::serde(name = "GlossqlObjectStorageFactory")]
impl StorageFactory for ObjectStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(ObjectStorage::new(config.props().clone())))
    }
}

/// [`Storage`] over object_store's clients, one per authority.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ObjectStorage {
    props: HashMap<String, String>,
    #[serde(skip)]
    stores: Arc<Mutex<HashMap<String, Arc<dyn ObjectStore>>>>,
}

/// The properties never appear whole: a table load's carry credentials.
impl std::fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStorage")
            .field("s3.endpoint", &self.props.get(iceberg::io::S3_ENDPOINT))
            .field("adls.account-name", &self.props.get(ADLS_ACCOUNT_NAME))
            .finish_non_exhaustive()
    }
}

// iceberg's `adls.*` property names (`io/storage/config/azdls.rs` in the
// pin) — the keys a catalog's table load answers with for the Azure
// family; the S3 ones are exported at `iceberg::io`.
const ADLS_ACCOUNT_NAME: &str = "adls.account-name";
const ADLS_ACCOUNT_KEY: &str = "adls.account-key";
const ADLS_SAS_TOKEN: &str = "adls.sas-token";
const ADLS_TENANT_ID: &str = "adls.tenant-id";
const ADLS_CLIENT_ID: &str = "adls.client-id";
const ADLS_CLIENT_SECRET: &str = "adls.client-secret";
const ADLS_AUTHORITY_HOST: &str = "adls.authority-host";

/// An `object_store` failure as the engine's error; a missing object
/// keeps its kind readable for [`ObjectStorage::exists`].
fn io_error(e: object_store::Error, path: &str) -> Error {
    Error::new(ErrorKind::Unexpected, "the object store refused")
        .with_context("path", path.to_string())
        .with_source(e)
}

fn does_not_build(family: &str, authority: &str, e: object_store::Error) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("the {family} configuration does not build"),
    )
    .with_context("store", authority.to_string())
    .with_source(e)
}

impl ObjectStorage {
    fn new(props: HashMap<String, String>) -> Self {
        ObjectStorage {
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
    /// none.
    fn s3(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
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
        Ok(Arc::new(
            builder
                .build()
                .map_err(|e| does_not_build("S3", bucket, e))?,
        ))
    }

    /// The container's client, the same layering over the Azure family:
    /// the catalog's `adls.*` properties, then the environment's
    /// `AZURE_*` conventions minus credentials the catalog vended. With
    /// no credential from either side, object_store's client resolves
    /// the managed identity itself.
    fn azure(&self, root: &str, authority: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = MicrosoftAzureBuilder::new().with_url(root);
        let p = &self.props;
        if let Some(v) = p.get(ADLS_ACCOUNT_NAME) {
            builder = builder.with_account(v);
        }
        if let Some(v) = p.get(ADLS_ACCOUNT_KEY) {
            builder = builder.with_access_key(v);
        }
        if let Some(v) = p.get(ADLS_SAS_TOKEN) {
            builder = builder.with_config(AzureConfigKey::SasKey, v);
        }
        if let Some(v) = p.get(ADLS_TENANT_ID) {
            builder = builder.with_tenant_id(v);
        }
        if let Some(v) = p.get(ADLS_CLIENT_ID) {
            builder = builder.with_client_id(v);
        }
        if let Some(v) = p.get(ADLS_CLIENT_SECRET) {
            builder = builder.with_client_secret(v);
        }
        if let Some(v) = p.get(ADLS_AUTHORITY_HOST) {
            builder = builder.with_authority_host(v);
        }
        let vended = p.contains_key(ADLS_ACCOUNT_KEY)
            || p.contains_key(ADLS_SAS_TOKEN)
            || p.contains_key(ADLS_CLIENT_SECRET);
        let mut plain_http = false;
        for (name, value) in std::env::vars() {
            if !name.starts_with("AZURE_") {
                continue;
            }
            let Ok(key) = name.to_ascii_lowercase().parse::<AzureConfigKey>() else {
                continue;
            };
            let credential = matches!(
                key.as_ref(),
                "azure_storage_account_key"
                    | "azure_storage_sas_key"
                    | "azure_storage_client_secret"
                    | "azure_storage_token"
            );
            if credential && vended {
                continue;
            }
            if key.as_ref() == "azure_storage_endpoint" && value.starts_with("http://") {
                plain_http = true;
            }
            builder = builder.with_config(key, value);
        }
        // A plain-http endpoint is a dev rig (Azurite); saying the
        // scheme is saying it on purpose.
        if plain_http {
            builder = builder.with_allow_http(true);
        }
        Ok(Arc::new(
            builder
                .build()
                .map_err(|e| does_not_build("Azure", authority, e))?,
        ))
    }

    fn store(&self, at: &Location) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.lock().expect("stores lock").get(&at.authority) {
            return Ok(Arc::clone(store));
        }
        let store = match at.family {
            Family::S3 => self.s3(at.authority.rsplit("://").next().unwrap_or_default())?,
            Family::Azure => self.azure(&at.root, &at.authority)?,
        };
        self.stores
            .lock()
            .expect("stores lock")
            .insert(at.authority.clone(), Arc::clone(&store));
        Ok(store)
    }

    fn at(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path)> {
        let at = location(path)?;
        Ok((self.store(&at)?, at.key))
    }
}

#[async_trait]
#[typetag::serde(name = "GlossqlObjectStorage")]
impl Storage for ObjectStorage {
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
        Ok(Box::new(ObjectFileRead {
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
        Ok(Box::new(ObjectFileWrite {
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
struct ObjectFileRead {
    store: Arc<dyn ObjectStore>,
    key: object_store::path::Path,
    path: String,
}

#[async_trait]
impl FileRead for ObjectFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.key, range)
            .await
            .map_err(|e| io_error(e, &self.path))
    }
}

/// A multipart upload, completed at close — dropped without one, the
/// store never sees a completed object.
struct ObjectFileWrite {
    upload: Option<WriteMultipart>,
    path: String,
}

/// The concurrent part-uploads one writer may have in flight.
const UPLOAD_CONCURRENCY: usize = 8;

impl ObjectFileWrite {
    fn open(&mut self) -> Result<&mut WriteMultipart> {
        self.upload.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "written after close")
                .with_context("path", self.path.clone())
        })
    }
}

#[async_trait]
impl FileWrite for ObjectFileWrite {
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
    use super::*;

    /// The split: family, the client's key, and the object key, over
    /// both families' spellings; a location without a bucket or
    /// container, or off the two families, is refused by name.
    #[test]
    fn a_location_splits_into_its_store_and_its_key() {
        let at = location("s3://lake/ns/t/data/x.parquet").unwrap();
        assert_eq!(at.family, Family::S3);
        assert_eq!(at.authority, "s3://lake");
        assert_eq!(at.key.as_ref(), "ns/t/data/x.parquet");
        let at = location("s3a://lake/k").unwrap();
        assert_eq!(at.authority, "s3a://lake");

        let at = location("abfss://lake@acme.dfs.core.windows.net/warehouse/ns/t/m.json").unwrap();
        assert_eq!(at.family, Family::Azure);
        assert_eq!(at.authority, "abfss://lake@acme.dfs.core.windows.net");
        assert_eq!(at.root, "abfss://lake@acme.dfs.core.windows.net");
        assert_eq!(at.key.as_ref(), "warehouse/ns/t/m.json");
        let at = location("wasbs://lake@acme.blob.core.windows.net/p/q").unwrap();
        assert_eq!(at.root, "abfss://lake@acme.blob.core.windows.net");
        assert_eq!(at.key.as_ref(), "p/q");
        let at = location("az://lake/p").unwrap();
        assert_eq!(at.authority, "az://lake");

        assert!(location("file:///tmp/x").is_err());
        assert!(location("gs://lake/x").is_err());
        assert!(location("s3://bucketonly").unwrap().key.as_ref().is_empty());
    }

    /// A warehouse is a directory or a remote location; the scheme
    /// decides, and an unreachable one is refused by name.
    #[test]
    fn a_warehouse_is_local_or_remote_by_scheme() {
        assert_eq!(
            Warehouse::parse("/tmp/w").unwrap(),
            Warehouse::Local(PathBuf::from("/tmp/w"))
        );
        assert_eq!(
            Warehouse::parse("file:///tmp/w").unwrap(),
            Warehouse::Local(PathBuf::from("/tmp/w"))
        );
        assert_eq!(
            Warehouse::parse("s3://lake/warehouse/").unwrap(),
            Warehouse::Remote("s3://lake/warehouse".into())
        );
        assert_eq!(
            Warehouse::parse("abfss://lake@acme.dfs.core.windows.net/warehouse").unwrap(),
            Warehouse::Remote("abfss://lake@acme.dfs.core.windows.net/warehouse".into())
        );
        assert!(Warehouse::parse("gs://lake/warehouse").is_err());
    }
}
