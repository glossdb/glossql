//! The data plane behind every catalog: iceberg's `Storage` seam on the
//! engine's own storage crate.
//!
//! iceberg-rust moves every byte through `FileIO` → [`Storage`], and
//! leaves what implements that trait to the caller
//! (`io/storage/mod.rs` invites third-party implementations). This one
//! is built on `object_store` — the Apache crate DataFusion itself
//! runs on, already in the binary — so the process carries one storage
//! stack, not two. Three families answer: S3 (`s3://`, `s3a://`,
//! `s3n://`), Azure (`abfss://container@account.dfs.core.windows.net/…`,
//! `abfs://`, `az://`, `azure://`, `adl://`, `wasbs://`) and Google
//! Cloud Storage (`gs://`, `gcs://`). A client is
//! scoped to one bucket or container, so clients are built lazily per
//! authority and shared; in practice a table's FileIO sees one.
//!
//! Configuration is layered by concern, between the `s3.*` / `adls.*` /
//! `gcs.*` properties a table load answers with and object_store's own
//! environment conventions (`AWS_*`, `AZURE_*`, `GOOGLE_*`): the catalog
//! says where
//! the store is and, vending, whom it lets in; the environment says how
//! this process reaches it and supplies credentials only where the
//! catalog vends none — a static key must never shadow a vended one.
//! On Azure with no credential set at all, the client asks the managed
//! identity — the Container Apps arrangement: the identity is the
//! credential and there is no secret. The platform names its token
//! endpoint in `IDENTITY_ENDPOINT`, which this seam hands the builder
//! (object_store reads it only in its own `from_env`); the client reads
//! `IDENTITY_HEADER` itself at token time. Without the endpoint the
//! client would ask the virtual machine's metadata address, which
//! Container Apps does not offer. A user-assigned identity is named by
//! `AZURE_STORAGE_CLIENT_ID`. On Google Cloud with no credential set,
//! the client asks the metadata server for the attached service
//! account's token — the Cloud Run arrangement, the same shape. No
//! surface of ours either way.
//!
//! The same clients serve a file source whose location is in a store
//! ([`environment_store`]): there no catalog vends, the environment
//! alone configures, and the location carries no credential
//! (SPEC.md §3).

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

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
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey};
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
    /// A path, `file://<path>`, or a remote location in one of the
    /// three families. Anything else is refused by its scheme.
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
                 file://, s3://, gs://, or abfss://container@account.dfs.core.windows.net/"
            ))),
        }
    }
}

/// The three families of stores, by scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    S3,
    Azure,
    Gcs,
}

fn family(scheme: &str) -> Option<Family> {
    match scheme {
        "s3" | "s3a" | "s3n" => Some(Family::S3),
        "abfs" | "abfss" | "az" | "azure" | "adl" | "wasb" | "wasbs" => Some(Family::Azure),
        "gs" | "gcs" => Some(Family::Gcs),
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
    let family = family(&scheme)
        .ok_or_else(|| invalid("not an S3, Azure or Google Cloud Storage location"))?;
    let host = url
        .host_str()
        .ok_or_else(|| invalid("a location names a bucket or a container"))?;
    let authority = if url.username().is_empty() {
        host.to_string()
    } else {
        format!("{}@{host}", url.username())
    };
    // The Hadoop `wasb(s)` spelling is the `abfss` layout under another
    // scheme, and `gcs` is `gs` under another; object_store's builders
    // parse the latter of each.
    let root_scheme = match scheme.as_str() {
        "wasb" | "wasbs" => "abfss",
        "gcs" => "gs",
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

/// How many distinct property sets keep their storage. A warehouse
/// answers one set for all its tables, so a handful covers a process; a
/// catalog that vends fresh credentials with every load answers a new
/// set each time, and the bound is what keeps those from accumulating.
const HELD_STORAGES: u64 = 64;

/// A property set in one order — the key a storage is held under.
type Properties = Vec<(String, String)>;

/// The storages built so far, by the properties they were built from.
/// The catalog builds a FileIO at every table load, and a storage
/// built anew starts with no client — a connection pool and a
/// credential fetch per scanned table. Properties are compared whole,
/// so a load that carries different ones gets a client of its own.
static STORAGES: LazyLock<moka::sync::Cache<Properties, Arc<ObjectStorage>>> =
    LazyLock::new(|| moka::sync::Cache::new(HELD_STORAGES));

#[typetag::serde(name = "GlossqlObjectStorageFactory")]
impl StorageFactory for ObjectStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let mut key: Properties = config
            .props()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        key.sort();
        Ok(STORAGES.get_with(key, || Arc::new(ObjectStorage::new(config.props().clone()))))
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
            .field("gcs.service.host", &self.props.get(GCS_SERVICE_HOST))
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

// iceberg's `gcs.*` property names (`io/storage/config/gcs.rs` in the
// pin), the same for the Google family.
const GCS_SERVICE_HOST: &str = "gcs.service.host";
const GCS_CREDENTIALS_JSON: &str = "gcs.credentials-json";
const GCS_TOKEN: &str = "gcs.oauth2.token";
const GCS_NO_AUTH: &str = "gcs.no-auth";

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
    /// the managed identity itself, at the endpoint the platform names.
    fn azure(&self, root: &str, authority: &str) -> Result<Arc<dyn ObjectStore>> {
        Ok(Arc::new(
            self.azure_builder(root, std::env::vars())
                .build()
                .map_err(|e| does_not_build("Azure", authority, e))?,
        ))
    }

    /// The builder behind [`Self::azure`], the environment passed in so
    /// the pass is testable without touching the process's own.
    fn azure_builder(
        &self,
        root: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> MicrosoftAzureBuilder {
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
        for (name, value) in env {
            // The platform's token endpoint, outside the `AZURE_*` family.
            if name == "IDENTITY_ENDPOINT" {
                builder = builder.with_msi_endpoint(value);
                continue;
            }
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
        builder
    }

    /// The bucket's client, the same layering over the Google family:
    /// the catalog's `gcs.*` properties, then the environment's
    /// `GOOGLE_*` conventions minus credentials the catalog vended. With
    /// no credential from either side, object_store's client asks the
    /// metadata server for the attached service account's token
    /// (object_store `gcp/builder.rs`, `InstanceCredentialProvider`).
    fn gcs(&self, root: &str, authority: &str) -> Result<Arc<dyn ObjectStore>> {
        Ok(Arc::new(
            self.gcs_builder(root, std::env::vars())
                .build()
                .map_err(|e| does_not_build("Google Cloud Storage", authority, e))?,
        ))
    }

    /// The builder behind [`Self::gcs`], the environment passed in so
    /// the pass is testable without touching the process's own.
    fn gcs_builder(
        &self,
        root: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> GoogleCloudStorageBuilder {
        let mut builder = GoogleCloudStorageBuilder::new().with_url(root);
        let p = &self.props;
        let mut base_url = p.get(GCS_SERVICE_HOST).cloned();
        if let Some(v) = p.get(GCS_CREDENTIALS_JSON) {
            builder = builder.with_service_account_key(v);
        }
        // A vended token is the whole credential: the client sends it
        // as given and asks nobody for another.
        if let Some(v) = p.get(GCS_TOKEN) {
            builder = builder.with_credentials(Arc::new(
                object_store::StaticCredentialProvider::new(GcpCredential { bearer: v.clone() }),
            ));
        }
        if p.get(GCS_NO_AUTH).is_some_and(|v| v == "true") {
            builder = builder.with_skip_signature(true);
        }
        let vended = p.contains_key(GCS_CREDENTIALS_JSON) || p.contains_key(GCS_TOKEN);
        for (name, value) in env {
            if !name.starts_with("GOOGLE_") {
                continue;
            }
            let Ok(key) = name.to_ascii_lowercase().parse::<GoogleConfigKey>() else {
                continue;
            };
            let credential = matches!(
                key,
                GoogleConfigKey::ServiceAccount
                    | GoogleConfigKey::ServiceAccountKey
                    | GoogleConfigKey::ApplicationCredentials
            );
            if credential && vended {
                continue;
            }
            if key == GoogleConfigKey::BaseUrl {
                base_url = Some(value.clone());
            }
            builder = builder.with_config(key, value);
        }
        if let Some(url) = &base_url {
            builder = builder.with_base_url(url);
            // A plain-http host is a dev rig (an emulator); saying the
            // scheme is saying it on purpose.
            if url.starts_with("http://") {
                builder = builder
                    .with_client_options(object_store::ClientOptions::new().with_allow_http(true));
            }
        }
        builder
    }

    fn store(&self, at: &Location) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.lock().expect("stores lock").get(&at.authority) {
            return Ok(Arc::clone(store));
        }
        let store = match at.family {
            Family::S3 => self.s3(at.authority.rsplit("://").next().unwrap_or_default())?,
            Family::Azure => self.azure(&at.root, &at.authority)?,
            Family::Gcs => self.gcs(&at.root, &at.authority)?,
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

/// The client for a location this seam reaches, configured by the
/// environment alone — a file source's root, where no catalog vends —
/// and the URL it registers under in an engine context: the location's
/// `scheme://authority/`, which is how the engine keys a store
/// (datafusion-execution object_store.rs, `get_url_key`: scheme and
/// host, the container name in the userinfo dropped). A scratch context
/// registers one root, so the key collides with nothing.
pub fn environment_store(location: &str) -> crate::Result<(Arc<dyn ObjectStore>, Url)> {
    let at = self::location(location)?;
    let store = ObjectStorage::new(HashMap::new()).store(&at)?;
    let key = Url::parse(&format!("{}/", at.authority)).map_err(|e| {
        crate::Error::Workspace(format!("location `{}` is not a URL: {e}", at.authority))
    })?;
    Ok((store, key))
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
    /// container, or off the three families, is refused by name.
    /// A table load with the properties of the last one gets the
    /// storage — and so the clients — the last one built; new
    /// properties get their own.
    #[test]
    fn a_repeated_load_reuses_its_storage() {
        let thin = |s: &Arc<dyn Storage>| Arc::as_ptr(s).cast::<()>();
        let config = |region: &str| {
            StorageConfig::new().with_prop(iceberg::io::S3_REGION, region.to_string())
        };
        let first = ObjectStorageFactory.build(&config("eu-north-1")).unwrap();
        let again = ObjectStorageFactory.build(&config("eu-north-1")).unwrap();
        let other = ObjectStorageFactory.build(&config("eu-west-1")).unwrap();
        assert_eq!(thin(&first), thin(&again));
        assert_ne!(thin(&first), thin(&other));
    }

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

        let at = location("gs://lake/warehouse/ns/t/m.json").unwrap();
        assert_eq!(at.family, Family::Gcs);
        assert_eq!(at.authority, "gs://lake");
        assert_eq!(at.key.as_ref(), "warehouse/ns/t/m.json");
        let at = location("gcs://lake/p").unwrap();
        assert_eq!(at.authority, "gcs://lake");
        assert_eq!(at.root, "gs://lake");

        assert!(location("file:///tmp/x").is_err());
        assert!(location("oss://lake/x").is_err());
        assert!(location("s3://bucketonly").unwrap().key.as_ref().is_empty());
    }

    /// The platform's token endpoint reaches the client, and the
    /// user-assigned identity's client id with it, while a name outside
    /// the two conventions passes by. The pass is checked on the
    /// builder: the endpoint is asked only at token time.
    #[test]
    fn the_managed_identity_endpoint_reaches_the_client() {
        let env = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let storage = ObjectStorage::new(HashMap::new());
        let root = "abfss://lake@acme.dfs.core.windows.net";
        let builder = storage.azure_builder(
            root,
            env(&[
                ("IDENTITY_ENDPOINT", "http://localhost:42356/msi/token"),
                (
                    "AZURE_STORAGE_CLIENT_ID",
                    "11111111-2222-3333-4444-555555555555",
                ),
                ("HOME", "/home/glossql"),
            ]),
        );
        assert_eq!(
            builder
                .get_config_value(&AzureConfigKey::MsiEndpoint)
                .as_deref(),
            Some("http://localhost:42356/msi/token")
        );
        assert_eq!(
            builder
                .get_config_value(&AzureConfigKey::ClientId)
                .as_deref(),
            Some("11111111-2222-3333-4444-555555555555")
        );
        let bare = storage.azure_builder(root, env(&[]));
        assert_eq!(bare.get_config_value(&AzureConfigKey::MsiEndpoint), None);
    }

    /// The Google family's pass: the environment configures, a
    /// credential the catalog vended keeps the environment's own out,
    /// and a name outside the convention passes by.
    #[test]
    fn a_vended_gcs_token_keeps_the_environments_credential_out() {
        let env = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let pairs = [
            ("GOOGLE_SERVICE_ACCOUNT_PATH", "/secrets/sa.json"),
            ("GOOGLE_BASE_URL", "http://localhost:4443"),
            ("HOME", "/home/glossql"),
        ];
        let bare = ObjectStorage::new(HashMap::new()).gcs_builder("gs://lake", env(&pairs));
        assert_eq!(
            bare.get_config_value(&GoogleConfigKey::ServiceAccount)
                .as_deref(),
            Some("/secrets/sa.json")
        );
        assert_eq!(
            bare.get_config_value(&GoogleConfigKey::BaseUrl).as_deref(),
            Some("http://localhost:4443")
        );
        assert_eq!(
            bare.get_config_value(&GoogleConfigKey::Bucket).as_deref(),
            None,
            "the bucket is read from the URL at build"
        );
        let vended = ObjectStorage::new(HashMap::from([(GCS_TOKEN.into(), "ya29.token".into())]))
            .gcs_builder("gs://lake", env(&pairs));
        assert_eq!(
            vended.get_config_value(&GoogleConfigKey::ServiceAccount),
            None
        );
    }

    /// A file source's root builds its client from the environment alone
    /// and registers under the location's authority; off the three
    /// families it is refused by name.
    #[test]
    fn a_source_root_registers_under_its_authority() {
        let (_, key) =
            environment_store("abfss://lake@acme.dfs.core.windows.net/sources/finance").unwrap();
        assert_eq!(key.as_str(), "abfss://lake@acme.dfs.core.windows.net/");
        let (_, key) = environment_store("wasbs://lake@acme.blob.core.windows.net/p").unwrap();
        assert_eq!(key.as_str(), "wasbs://lake@acme.blob.core.windows.net/");
        assert!(environment_store("oss://lake/sources").is_err());
        assert!(environment_store("/tmp/sources").is_err());
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
        assert_eq!(
            Warehouse::parse("gs://lake/warehouse").unwrap(),
            Warehouse::Remote("gs://lake/warehouse".into())
        );
        assert!(Warehouse::parse("oss://lake/warehouse").is_err());
    }
}
