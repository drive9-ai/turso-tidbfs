//! Experimental Turso I/O over TiDB Filesystem HTTP. No local database files or FUSE.
//!
//! Readers pin immutable snapshots. A renewable writer lease shares the manifest
//! CAS with file publication; expired or replaced sessions fail closed.
mod session;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
pub use session::SessionOptions;
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use turso_core::{
    Buffer, Completion, LimboError, Result,
    io::{
        Clock, File, FileId, FileSyncType, IO, OpenFlags,
        clock::{DefaultClock, MonotonicInstant, WallClockInstant},
    },
};

const MANIFEST_FORMAT: &str = "turso-tidbfs";
pub const BLOCK_SIZE: usize = 32 * 1024;
const MAX_FILE: usize = 8 * 1024 * 1024;
const MAX_MANIFEST: usize = 48 * 1024;
const MAX_FILES: usize = 8;
const CACHE_BLOCKS: usize = 64;
fn err(message: impl Into<String>) -> LimboError {
    LimboError::InternalError(message.into())
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() < 128
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}

/// Revisions must come from the same atomic read as the returned bytes.
/// put(expected=0) means create-only; every other put is compare-and-swap.
pub trait Store: Send + Sync {
    fn get(&self, name: &str) -> Result<Option<(Vec<u8>, u64)>>;
    fn put(&self, name: &str, bytes: &[u8], expected: u64) -> Result<u64>;
    /// Conditional publication must finish within the session's remaining lifetime.
    fn put_manifest(&self, bytes: &[u8], expected: u64, deadline: Instant) -> Result<u64> {
        session::before_deadline(Instant::now(), deadline)?;
        let revision = self.put("manifest.json", bytes, expected)?;
        session::before_deadline(Instant::now(), deadline)?;
        Ok(revision)
    }
    /// Upload immutable data before publishing any manifest reference.
    /// A failure may leave orphan blocks, but must never publish a partial image.
    fn put_blocks(&self, blocks: &BTreeMap<String, Vec<u8>>) -> Result<()> {
        for (hash, bytes) in blocks {
            let path = format!("blocks/{hash}");
            match self.get(&path)? {
                Some((existing, _)) if existing == *bytes => (),
                Some(_) => return Err(err("immutable FS block hash mismatch")),
                None => {
                    self.put(&path, bytes, 0)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default, Serialize)]
pub struct Metrics {
    pub gets: u64,
    pub puts: u64,
    pub batch_requests: u64,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
}
pub struct HttpStore {
    client: reqwest::blocking::Client,
    base: reqwest::Url,
    token: String,
    metrics: Mutex<Metrics>,
}
impl HttpStore {
    pub fn new(endpoint: &str, token: String, root: &str) -> Result<Self> {
        let mut base = reqwest::Url::parse(endpoint).map_err(|_| err("invalid FS endpoint"))?;
        if base.scheme() != "https"
            && !(base.scheme() == "http"
                && matches!(base.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")))
        {
            return Err(err(
                "FS endpoint requires HTTPS (HTTP is allowed on loopback)",
            ));
        }
        if !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || !matches!(base.path(), "" | "/")
        {
            return Err(err("FS endpoint must be an origin URL"));
        }
        if token.is_empty()
            || !root.starts_with('/')
            || root == "/"
            || root.ends_with('/')
            || !root[1..].split('/').all(valid_name)
        {
            return Err(err(
                "provide a token and a non-root ASCII FS directory without dot segments",
            ));
        }
        base.set_path(&format!("/v1/fs{root}/"));
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| err("create HTTP client"))?;
        Ok(Self {
            client,
            base,
            token,
            metrics: Mutex::default(),
        })
    }
    fn url(&self, name: &str) -> Result<reqwest::Url> {
        self.base.join(name).map_err(|_| err("invalid object path"))
    }
    /// The caller creates the root; blocks are stored in a child directory.
    pub fn prepare(&self) -> Result<()> {
        for url in [self.base.clone(), self.url("blocks/")?] {
            let response = self
                .client
                .post(url)
                .query(&[("mkdir", "")])
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| err("FS mkdir transport failure"))?;
            if !response.status().is_success() && response.status().as_u16() != 409 {
                return Err(err(format!("FS mkdir HTTP {}", response.status())));
            }
        }
        Ok(())
    }
    pub fn metrics_snapshot(&self) -> Metrics {
        self.metrics.lock().unwrap().clone()
    }
    pub fn metrics(&self) -> String {
        serde_json::to_string(&*self.metrics.lock().unwrap()).unwrap()
    }
    fn put_object(
        &self,
        name: &str,
        bytes: &[u8],
        expected: u64,
        timeout: Duration,
    ) -> Result<u64> {
        let response = self
            .client
            .put(self.url(name)?)
            .timeout(timeout)
            .bearer_auth(&self.token)
            .header("Content-Type", "application/octet-stream")
            .header("X-Dat9-Expected-Revision", expected)
            .body(bytes.to_vec())
            .send()
            .map_err(|_| err("FS PUT transport failure; outcome unknown, reopen session"))?;
        {
            let mut m = self.metrics.lock().unwrap();
            m.puts += 1;
            m.uploaded_bytes += bytes.len() as u64;
        }
        if response.status().as_u16() == 409 {
            return Err(LimboError::LockingError(
                "FS manifest CAS conflict (HTTP 409)".into(),
            ));
        }
        if !response.status().is_success() {
            return Err(err(format!(
                "FS PUT HTTP {}; reopen session",
                response.status()
            )));
        }
        #[derive(Deserialize)]
        struct Ack {
            revision: u64,
        }
        let ack: Ack = response
            .json()
            .map_err(|_| err("FS PUT invalid acknowledgement"))?;
        if ack.revision <= expected {
            return Err(err("FS PUT non-increasing revision"));
        }
        Ok(ack.revision)
    }
}
impl Store for HttpStore {
    fn get(&self, name: &str) -> Result<Option<(Vec<u8>, u64)>> {
        let response = self
            .client
            .get(self.url(name)?)
            .bearer_auth(&self.token)
            .send()
            .map_err(|_| err("FS GET transport failure"))?;
        self.metrics.lock().unwrap().gets += 1;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(err(format!("FS GET HTTP {}", response.status())));
        }
        let revision = response
            .headers()
            .get("X-Dat9-Revision")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse().ok())
            .filter(|v| *v > 0)
            .ok_or_else(|| err("FS GET missing revision"))?;
        let mut bytes = Vec::new();
        response
            .take((MAX_MANIFEST.max(BLOCK_SIZE) + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| err("FS GET body failed"))?;
        if bytes.len() > MAX_MANIFEST.max(BLOCK_SIZE) {
            return Err(err("FS object exceeds prototype limit"));
        }
        self.metrics.lock().unwrap().downloaded_bytes += bytes.len() as u64;
        Ok(Some((bytes, revision)))
    }
    fn put(&self, name: &str, bytes: &[u8], expected: u64) -> Result<u64> {
        self.put_object(name, bytes, expected, Duration::from_secs(60))
    }
    fn put_manifest(&self, bytes: &[u8], expected: u64, deadline: Instant) -> Result<u64> {
        let now = Instant::now();
        session::before_deadline(now, deadline)?;
        let revision = self.put_object("manifest.json", bytes, expected, deadline - now)?;
        session::before_deadline(Instant::now(), deadline)?;
        Ok(revision)
    }
    fn put_blocks(&self, blocks: &BTreeMap<String, Vec<u8>>) -> Result<()> {
        #[derive(Serialize)]
        struct Item {
            path: String,
            expected_revision: u64,
            data: String,
        }
        #[derive(Deserialize)]
        struct Ack {
            path: String,
            status: u16,
            #[serde(default)]
            revision: u64,
        }
        #[derive(Deserialize)]
        struct Response {
            results: Vec<Ack>,
        }
        let root = self
            .base
            .path()
            .strip_prefix("/v1/fs")
            .ok_or_else(|| err("invalid FS root"))?;
        let blocks: Vec<_> = blocks.iter().collect();
        // 128 x 32 KiB stays within the protocol's 128-item / 4-MiB limit.
        for group in blocks.chunks(128) {
            let items: Vec<_> = group
                .iter()
                .map(|(hash, bytes)| Item {
                    path: format!("{root}blocks/{hash}"),
                    expected_revision: 0,
                    data: BASE64.encode(bytes),
                })
                .collect();
            let body = serde_json::to_vec(&serde_json::json!({"items": items}))
                .map_err(|_| err("encode block batch"))?;
            let uploaded_bytes = body.len() as u64;
            let response = self
                .client
                .post(self.url("/v1/fs:batch-write")?)
                .bearer_auth(&self.token)
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .map_err(|_| err("FS block batch transport failure; reopen session"))?;
            {
                let mut metrics = self.metrics.lock().unwrap();
                metrics.batch_requests += 1;
                metrics.uploaded_bytes += uploaded_bytes;
            }
            if !response.status().is_success() {
                return Err(err(format!(
                    "FS block batch HTTP {}; reopen session",
                    response.status()
                )));
            }
            let response: Response = response
                .json()
                .map_err(|_| err("invalid FS batch acknowledgement"))?;
            if response.results.len() != items.len() {
                return Err(err("FS batch acknowledgement count mismatch"));
            }
            for ((ack, item), (_, bytes)) in response.results.iter().zip(&items).zip(group) {
                if ack.path != item.path {
                    return Err(err("FS batch acknowledgement path mismatch"));
                }
                match (ack.status, ack.revision) {
                    (200, revision) if revision > 0 => (),
                    // Only a definite create conflict can reuse a verified existing block.
                    (409, 0) => {
                        let relative = item
                            .path
                            .strip_prefix(root)
                            .ok_or_else(|| err("invalid block path"))?;
                        let (existing, _) = self
                            .get(relative)?
                            .ok_or_else(|| err("conflicting FS block missing"))?;
                        if existing != **bytes {
                            return Err(err("immutable FS block hash mismatch"));
                        }
                    }
                    _ => {
                        return Err(err(format!(
                            "FS block batch item HTTP {}; reopen session",
                            ack.status
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Image {
    size: usize,
    blocks: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inline: Option<InlineBlock>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InlineBlock {
    hash: String,
    data: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    files: BTreeMap<String, Image>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease: Option<session::RemoteLease>,
}
impl Default for Manifest {
    fn default() -> Self {
        Self {
            format: MANIFEST_FORMAT.into(),
            lease: None,
            files: BTreeMap::new(),
        }
    }
}
impl Manifest {
    // A full inline manifest must still fit when heartbeat sequence digits grow.
    fn lease_padding(&self) -> usize {
        self.lease
            .as_ref()
            .map_or(0, |lease| 20 - lease.sequence.to_string().len())
    }
}
struct Block {
    bytes: Vec<u8>,
    touched: u64,
}
struct Cached {
    size: usize,
    base: Image,
    // A truncate must never resurrect bytes from the old immutable snapshot.
    base_limit: usize,
    blocks: BTreeMap<usize, Block>,
    dirty: BTreeSet<usize>,
    clock: u64,
    locked: bool,
}
impl Cached {
    fn new(base: Image) -> Self {
        Self {
            size: base.size,
            base_limit: base.size,
            base,
            blocks: BTreeMap::new(),
            dirty: BTreeSet::new(),
            clock: 0,
            locked: false,
        }
    }
    fn ensure_block(&mut self, store: &dyn Store, index: usize) -> Result<()> {
        self.clock += 1;
        if !self.blocks.contains_key(&index) {
            let start = index * BLOCK_SIZE;
            let mut bytes = vec![0; BLOCK_SIZE];
            if start < self.base_limit {
                let hash = self
                    .base
                    .blocks
                    .get(index)
                    .ok_or_else(|| err("missing block reference"))?;
                let remote = if let Some(inline) = &self.base.inline
                    && inline.hash == *hash
                {
                    BASE64
                        .decode(&inline.data)
                        .map_err(|_| err("invalid inline block encoding"))?
                } else {
                    store
                        .get(&format!("blocks/{hash}"))?
                        .ok_or_else(|| err("missing FS block"))?
                        .0
                };
                let original_len = (self.base.size - start).min(BLOCK_SIZE);
                if remote.len() != original_len || digest(&remote) != *hash {
                    return Err(err("corrupt FS block"));
                }
                let retained = original_len.min(self.base_limit - start);
                bytes[..retained].copy_from_slice(&remote[..retained]);
            }
            self.blocks.insert(
                index,
                Block {
                    bytes,
                    touched: self.clock,
                },
            );
        }
        self.blocks.get_mut(&index).unwrap().touched = self.clock;
        self.evict(Some(index));
        Ok(())
    }
    fn evict(&mut self, keep: Option<usize>) {
        while self.blocks.len() > CACHE_BLOCKS {
            let victim = self
                .blocks
                .iter()
                .filter(|(i, _)| Some(**i) != keep && !self.dirty.contains(i))
                .min_by_key(|(_, b)| b.touched)
                .map(|(i, _)| *i);
            let Some(index) = victim else {
                break;
            };
            self.blocks.remove(&index);
        }
    }
}
struct State {
    manifest: Manifest,
    revision: u64,
    files: BTreeMap<String, Cached>,
    poisoned: bool,
    readonly: bool,
    lease: Option<session::LocalLease>,
}
struct Inner {
    store: Arc<dyn Store>,
    state: Mutex<State>,
    session: String,
    clock: Arc<dyn session::SessionClock>,
}
pub struct FsIo {
    inner: Arc<Inner>,
    heartbeat: Mutex<Option<session::Heartbeat>>,
}
impl FsIo {
    pub fn new(store: Arc<dyn Store>) -> Result<Self> {
        Self::writer(store, SessionOptions::default())
    }
    fn load(store: Arc<dyn Store>) -> Result<Self> {
        let (manifest, revision) = match store.get("manifest.json")? {
            Some((bytes, rev)) => (
                serde_json::from_slice::<Manifest>(&bytes)
                    .map_err(|_| err("invalid FS manifest"))?,
                rev,
            ),
            None => (Manifest::default(), 0),
        };
        if manifest.format != MANIFEST_FORMAT || manifest.files.len() > MAX_FILES {
            return Err(err("unsupported FS manifest"));
        }
        if let Some(lease) = &manifest.lease {
            lease.validate()?;
        }
        for (name, image) in &manifest.files {
            if !valid_name(name)
                || image.size > MAX_FILE
                || image.blocks.len() != image.size.div_ceil(BLOCK_SIZE)
                || image.blocks.iter().any(|b| {
                    b.len() != 64
                        || !b
                            .bytes()
                            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
                })
            {
                return Err(err("invalid manifest file or block reference"));
            }
            if let Some(inline) = &image.inline {
                if image.blocks.last() != Some(&inline.hash) {
                    return Err(err("invalid inline block reference"));
                }
                let bytes = BASE64
                    .decode(&inline.data)
                    .map_err(|_| err("invalid inline block encoding"))?;
                let length = image.size - (image.blocks.len() - 1) * BLOCK_SIZE;
                if bytes.len() != length || digest(&bytes) != inline.hash {
                    return Err(err("corrupt inline block"));
                }
            }
        }
        let mut entropy = [0u8; 32];
        getrandom::fill(&mut entropy).map_err(|_| err("generate unique session owner"))?;
        let session = digest(&entropy);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                session,
                clock: Arc::new(session::SystemClock),
                state: Mutex::new(State {
                    manifest,
                    revision,
                    files: BTreeMap::new(),
                    poisoned: false,
                    readonly: false,
                    lease: None,
                }),
            }),
            heartbeat: Mutex::new(None),
        })
    }
    #[cfg(test)]
    fn unlocked_for_test(store: Arc<dyn Store>) -> Result<Self> {
        Self::load(store)
    }
}
impl Inner {
    fn healthy(&self, state: &State) -> Result<()> {
        if state.poisoned
            || state
                .lease
                .as_ref()
                .is_some_and(|l| self.clock.now() >= l.deadline)
        {
            Err(err(
                "FS session failed or expired; discard it and reopen from remote state",
            ))
        } else {
            Ok(())
        }
    }
    fn publish(&self, state: &mut State, mut manifest: Manifest) -> Result<()> {
        self.healthy(state)?;
        if state.readonly {
            return Err(err("read-only snapshot"));
        }
        let started = self.clock.now();
        if state.lease.is_some() {
            let remote = manifest
                .lease
                .as_mut()
                .ok_or_else(|| err("missing writer lease"))?;
            if remote.owner != self.session {
                return Err(err("writer lease owner mismatch"));
            }
            remote.sequence = remote
                .sequence
                .checked_add(1)
                .ok_or_else(|| err("lease sequence overflow"))?;
        }
        let bytes = serde_json::to_vec(&manifest).map_err(|_| err("encode manifest"))?;
        if bytes.len() + manifest.lease_padding() > MAX_MANIFEST {
            return Err(err("manifest exceeds prototype limit"));
        }
        let revision = if let Some(lease) = &state.lease {
            let revision = self
                .store
                .put_manifest(&bytes, state.revision, lease.deadline)?;
            self.healthy(state)?;
            session::before_deadline(self.clock.now(), started + lease.ttl)?;
            revision
        } else {
            self.store.put("manifest.json", &bytes, state.revision)?
        };
        state.revision = revision;
        state.manifest = manifest;
        if let Some(lease) = &mut state.lease {
            lease.deadline = started + lease.ttl;
        }
        Ok(())
    }
    fn sync(&self, name: &str, state: &mut State) -> Result<()> {
        self.healthy(state)?;
        if state.readonly {
            return Ok(());
        }
        let result = (|| {
            let cached = state
                .files
                .get_mut(name)
                .ok_or_else(|| err("file removed"))?;
            let previous = state.manifest.files.get(name);
            if cached.dirty.is_empty() && previous.is_some_and(|p| p.size == cached.size) {
                return Ok(());
            }
            let mut blocks = Vec::new();
            let mut uploads = BTreeMap::new();
            for index in 0..cached.size.div_ceil(BLOCK_SIZE) {
                let start = index * BLOCK_SIZE;
                let length = (cached.size - start).min(BLOCK_SIZE);
                if !cached.dirty.contains(&index)
                    && start + length <= cached.base_limit
                    && length == (cached.base.size - start).min(BLOCK_SIZE)
                    && let Some(hash) = cached.base.blocks.get(index)
                {
                    blocks.push(hash.clone());
                    continue;
                }
                cached.ensure_block(self.store.as_ref(), index)?;
                let bytes = &cached.blocks.get(&index).unwrap().bytes[..length];
                let hash = digest(bytes);
                if previous.and_then(|p| p.blocks.get(index)) != Some(&hash) {
                    uploads.insert(hash.clone(), bytes.to_vec());
                }
                blocks.push(hash);
            }
            let mut image = Image {
                size: cached.size,
                blocks,
                inline: None,
            };
            // A self-contained WAL tail and its CAS manifest form one durable update.
            // Older inline tails that remain referenced must first become immutable objects.
            let previous_inline = cached.base.inline.clone();
            if name.ends_with("-wal")
                && let Some(hash) = image.blocks.last()
            {
                let bytes = uploads.get(hash).cloned().or_else(|| {
                    previous_inline
                        .as_ref()
                        .filter(|b| b.hash == *hash)
                        .and_then(|b| BASE64.decode(&b.data).ok())
                });
                if let Some(bytes) = bytes {
                    image.inline = Some(InlineBlock {
                        hash: hash.clone(),
                        data: BASE64.encode(bytes),
                    });
                }
            }
            let mut manifest = state.manifest.clone();
            manifest.files.insert(name.to_owned(), image);
            if serde_json::to_vec(&manifest)
                .map_err(|_| err("encode manifest"))?
                .len()
                + manifest.lease_padding()
                > MAX_MANIFEST
            {
                manifest.files.get_mut(name).unwrap().inline = None;
            }
            let image = &manifest.files[name];
            if let Some(previous) = previous_inline
                && image.blocks.contains(&previous.hash)
                && image
                    .inline
                    .as_ref()
                    .is_none_or(|b| b.hash != previous.hash)
            {
                uploads.insert(
                    previous.hash,
                    BASE64
                        .decode(&previous.data)
                        .map_err(|_| err("invalid inline block encoding"))?,
                );
            }
            if let Some(inline) = &image.inline {
                uploads.remove(&inline.hash);
            }
            self.store.put_blocks(&uploads)?;
            self.publish(state, manifest)?;
            let cached = state.files.get_mut(name).unwrap();
            cached.base = state.manifest.files[name].clone();
            cached.base_limit = cached.size;
            cached.dirty.clear();
            cached.evict(None);
            Ok(())
        })();
        if result.is_err() {
            state.poisoned = true;
        }
        result
    }
}
impl Clock for FsIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}
impl IO for FsIo {
    fn open_file(&self, name: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        if !valid_name(name) {
            return Err(err("use a database basename, not a local path"));
        }
        let mut state = self.inner.state.lock().unwrap();
        self.inner.healthy(&state)?;
        if !state.files.contains_key(name) {
            let base = if let Some(image) = state.manifest.files.get(name) {
                image.clone()
            } else {
                if !flags.contains(OpenFlags::Create) || flags.contains(OpenFlags::ReadOnly) {
                    return Err(turso_core::io_error(
                        std::io::ErrorKind::NotFound.into(),
                        "open",
                    ));
                }
                if state
                    .manifest
                    .files
                    .keys()
                    .chain(state.files.keys())
                    .collect::<BTreeSet<_>>()
                    .len()
                    >= MAX_FILES
                {
                    return Err(err("too many files"));
                }
                Image::default()
            };
            state.files.insert(name.to_owned(), Cached::new(base));
        }
        let owns_lock = !flags.intersects(OpenFlags::NoLock | OpenFlags::ReadOnly);
        if owns_lock {
            let cached = state.files.get_mut(name).unwrap();
            if cached.locked {
                return Err(LimboError::LockingError(
                    "database already open in this session".into(),
                ));
            }
            cached.locked = true;
        }
        Ok(Arc::new(FsFile {
            inner: self.inner.clone(),
            name: name.to_owned(),
            readonly: flags.contains(OpenFlags::ReadOnly),
            owns_lock: AtomicBool::new(owns_lock),
        }))
    }
    fn file_id(&self, name: &str) -> Result<FileId> {
        Ok(FileId::from_path_hash(&format!(
            "{}:{name}",
            self.inner.session
        )))
    }
    fn remove_file(&self, name: &str) -> Result<()> {
        let mut state = self.inner.state.lock().unwrap();
        self.inner.healthy(&state)?;
        if state.readonly {
            return Err(err("read-only snapshot"));
        }
        if state.files.get(name).is_some_and(|c| c.locked) {
            return Err(err("cannot remove open database"));
        }
        let mut manifest = state.manifest.clone();
        manifest.files.remove(name);
        if let Err(e) = self.inner.publish(&mut state, manifest) {
            state.poisoned = true;
            return Err(e);
        }
        state.files.remove(name);
        Ok(())
    }
}
struct FsFile {
    inner: Arc<Inner>,
    name: String,
    readonly: bool,
    owns_lock: AtomicBool,
}
impl FsFile {
    fn access<T>(&self, write: bool, f: impl FnOnce(&mut Cached) -> Result<T>) -> Result<T> {
        let mut state = self.inner.state.lock().unwrap();
        self.inner.healthy(&state)?;
        if write && (self.readonly || state.readonly) {
            return Err(err("read-only file"));
        }
        let result = f(state
            .files
            .get_mut(&self.name)
            .ok_or_else(|| err("file removed"))?);
        // A lazy source read can fail after earlier blocks were modified.
        // Never allow a later sync to publish that partially applied write.
        let result = result.and_then(|value| {
            self.inner.healthy(&state)?;
            Ok(value)
        });
        if write && result.is_err() {
            state.poisoned = true;
        }
        result
    }
}
impl Drop for FsFile {
    fn drop(&mut self) {
        let _ = self.unlock_file();
    }
}
impl File for FsFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        self.access(false, |cached| {
            if self.owns_lock.load(Ordering::Relaxed) {
                return Ok(());
            }
            if cached.locked {
                return Err(LimboError::LockingError("file already locked".into()));
            }
            cached.locked = true;
            self.owns_lock.store(true, Ordering::Relaxed);
            Ok(())
        })
    }
    fn unlock_file(&self) -> Result<()> {
        let mut state = self.inner.state.lock().unwrap();
        if self.owns_lock.swap(false, Ordering::Relaxed)
            && let Some(cached) = state.files.get_mut(&self.name)
        {
            cached.locked = false;
        }
        Ok(())
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let n = self.access(false, |cached| {
            let target = c.as_read().buf();
            let start = usize::try_from(pos).unwrap_or(usize::MAX);
            let n = cached.size.saturating_sub(start).min(target.len());
            let output = target.as_mut_slice();
            output.fill(0);
            let mut copied = 0;
            while copied < n {
                let offset = start + copied;
                let index = offset / BLOCK_SIZE;
                let in_block = offset % BLOCK_SIZE;
                let length = (BLOCK_SIZE - in_block).min(n - copied);
                cached.ensure_block(self.inner.store.as_ref(), index)?;
                output[copied..copied + length]
                    .copy_from_slice(&cached.blocks[&index].bytes[in_block..in_block + length]);
                copied += length;
            }
            Ok(n)
        })?;
        c.complete(n as i32);
        Ok(c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.access(true, |cached| {
            let start = usize::try_from(pos).map_err(|_| err("write offset overflow"))?;
            let end = start
                .checked_add(buffer.len())
                .filter(|end| *end <= MAX_FILE)
                .ok_or_else(|| err("file exceeds 8 MiB prototype limit"))?;
            let mut copied = 0;
            while copied < buffer.len() {
                let offset = start + copied;
                let index = offset / BLOCK_SIZE;
                let in_block = offset % BLOCK_SIZE;
                let length = (BLOCK_SIZE - in_block).min(buffer.len() - copied);
                // Full-block overwrites never need to download the previous block.
                if in_block == 0 && length == BLOCK_SIZE {
                    cached.blocks.insert(
                        index,
                        Block {
                            bytes: vec![0; BLOCK_SIZE],
                            touched: cached.clock,
                        },
                    );
                } else {
                    cached.ensure_block(self.inner.store.as_ref(), index)?;
                }
                cached.dirty.insert(index);
                cached.blocks.get_mut(&index).unwrap().bytes[in_block..in_block + length]
                    .copy_from_slice(&buffer.as_slice()[copied..copied + length]);
                copied += length;
            }
            if !buffer.is_empty() {
                cached.size = cached.size.max(end);
            }
            Ok(())
        })?;
        c.complete(buffer.len() as i32);
        Ok(c)
    }
    fn sync(&self, c: Completion, _kind: FileSyncType) -> Result<Completion> {
        let mut state = self.inner.state.lock().unwrap();
        self.inner.sync(&self.name, &mut state)?;
        drop(state);
        c.complete(0);
        Ok(c)
    }
    fn size(&self) -> Result<u64> {
        self.access(false, |c| Ok(c.size as u64))
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.access(true, |cached| {
            let len = usize::try_from(len)
                .ok()
                .filter(|n| *n <= MAX_FILE)
                .ok_or_else(|| err("truncate exceeds prototype limit"))?;
            if len < cached.size {
                if len % BLOCK_SIZE != 0 {
                    let index = len / BLOCK_SIZE;
                    cached.ensure_block(self.inner.store.as_ref(), index)?;
                    cached.blocks.get_mut(&index).unwrap().bytes[len % BLOCK_SIZE..].fill(0);
                    cached.dirty.insert(index);
                }
                cached.blocks.retain(|i, _| *i < len.div_ceil(BLOCK_SIZE));
                cached.dirty.retain(|i| *i < len.div_ceil(BLOCK_SIZE));
                cached.base_limit = cached.base_limit.min(len);
            }
            cached.size = len;
            Ok(())
        })?;
        c.complete(0);
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::{Database, SqliteDialect, StepResult};

    #[derive(Default)]
    struct MemoryStore {
        objects: Mutex<BTreeMap<String, (Vec<u8>, u64)>>,
        fail_manifest: AtomicBool,
        lose_ack: AtomicBool,
        block_gets: AtomicU64,
    }
    impl Store for MemoryStore {
        fn get(&self, name: &str) -> Result<Option<(Vec<u8>, u64)>> {
            if name.starts_with("blocks/") {
                self.block_gets.fetch_add(1, Ordering::Relaxed);
            }
            Ok(self.objects.lock().unwrap().get(name).cloned())
        }
        fn put(&self, name: &str, bytes: &[u8], expected: u64) -> Result<u64> {
            if name == "manifest.json" && self.fail_manifest.load(Ordering::Relaxed) {
                return Err(err("injected manifest failure"));
            }
            let mut objects = self.objects.lock().unwrap();
            let revision = objects.get(name).map_or(0, |v| v.1);
            if revision != expected {
                return Err(err("revision conflict"));
            }
            objects.insert(name.into(), (bytes.to_vec(), revision + 1));
            if name == "manifest.json" && self.lose_ack.load(Ordering::Relaxed) {
                return Err(err("injected lost acknowledgement after commit"));
            }
            Ok(revision + 1)
        }
    }
    fn write(file: &dyn File, pos: u64, bytes: &[u8]) {
        let completion = file
            .pwrite(
                pos,
                Arc::new(Buffer::new(bytes.to_vec())),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        assert!(completion.finished());
    }
    fn sync(file: &dyn File) -> Result<()> {
        file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .map(|_| ())
    }
    fn read(file: &dyn File, size: usize) -> Vec<u8> {
        let buf = Arc::new(Buffer::new_temporary(size));
        let completion = file
            .pread(0, Completion::new_read(buf.clone(), |_| None))
            .unwrap();
        assert!(completion.finished());
        buf.as_slice().to_vec()
    }
    #[test]
    fn inline_wal_tail_is_one_manifest_write_and_survives_rollover() {
        let store = Arc::new(MemoryStore::default());
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io
            .open_file("agent.db-wal", OpenFlags::Create, false)
            .unwrap();
        write(file.as_ref(), 0, &vec![7; BLOCK_SIZE]);
        sync(file.as_ref()).unwrap();
        assert_eq!(
            store.objects.lock().unwrap().len(),
            1,
            "small WAL sync should only publish its self-contained manifest"
        );
        write(file.as_ref(), BLOCK_SIZE as u64, b"tail");
        sync(file.as_ref()).unwrap();
        assert!(
            store
                .objects
                .lock()
                .unwrap()
                .contains_key(&format!("blocks/{}", digest(&vec![7; BLOCK_SIZE]))),
            "previous inline block must be externalized before it is replaced"
        );
        store.block_gets.store(0, Ordering::Relaxed);
        let fresh = FsIo::unlocked_for_test(store.clone())
            .unwrap()
            .open_file("agent.db-wal", OpenFlags::None, false)
            .unwrap();
        let all = read(fresh.as_ref(), BLOCK_SIZE + 4);
        assert!(all[..BLOCK_SIZE].iter().all(|v| *v == 7));
        assert_eq!(&all[BLOCK_SIZE..], b"tail");
        assert_eq!(
            store.block_gets.load(Ordering::Relaxed),
            1,
            "embedded tail should need no separate GET"
        );
    }
    #[test]
    fn inline_budget_falls_back_to_verified_remote_blocks() {
        let store = Arc::new(MemoryStore::default());
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        for (name, value) in [("first-wal", 1), ("second-wal", 2)] {
            let file = io.open_file(name, OpenFlags::Create, false).unwrap();
            write(file.as_ref(), 0, &vec![value; BLOCK_SIZE]);
            sync(file.as_ref()).unwrap();
        }
        let state = io.inner.state.lock().unwrap();
        assert!(state.manifest.files["first-wal"].inline.is_some());
        assert!(state.manifest.files["second-wal"].inline.is_none());
        drop(state);
        let fresh = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("second-wal", OpenFlags::None, false)
            .unwrap();
        assert_eq!(read(fresh.as_ref(), BLOCK_SIZE), vec![2; BLOCK_SIZE]);
    }
    #[test]
    fn manifest_identity_and_inline_integrity_are_checked() {
        let store = Arc::new(MemoryStore::default());
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io
            .open_file("agent.db-wal", OpenFlags::Create, false)
            .unwrap();
        write(file.as_ref(), 0, b"inline");
        sync(file.as_ref()).unwrap();
        let (body, revision) = store.get("manifest.json").unwrap().unwrap();
        let original: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(original["format"], MANIFEST_FORMAT);
        for damage in ["identity", "reference", "encoding", "length", "checksum"] {
            let mut manifest = original.clone();
            match damage {
                "identity" => manifest["format"] = serde_json::json!("unrelated-storage"),
                "reference" => {
                    manifest["files"]["agent.db-wal"]["inline"]["hash"] =
                        serde_json::json!("0".repeat(64))
                }
                "encoding" => {
                    manifest["files"]["agent.db-wal"]["inline"]["data"] =
                        serde_json::json!("invalid%base64")
                }
                "length" => {
                    manifest["files"]["agent.db-wal"]["inline"]["data"] =
                        serde_json::json!(BASE64.encode(b"short"))
                }
                _ => {
                    manifest["files"]["agent.db-wal"]["inline"]["data"] =
                        serde_json::json!(BASE64.encode(b"broken"))
                }
            }
            store.objects.lock().unwrap().insert(
                "manifest.json".into(),
                (serde_json::to_vec(&manifest).unwrap(), revision),
            );
            assert!(
                FsIo::unlocked_for_test(store.clone()).is_err(),
                "must reject {damage}"
            );
        }
        store
            .objects
            .lock()
            .unwrap()
            .insert("manifest.json".into(), (body, revision));
        let fresh = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("agent.db-wal", OpenFlags::None, false)
            .unwrap();
        assert_eq!(read(fresh.as_ref(), 6), b"inline");
    }
    #[test]
    fn demand_reads_cache_eviction_and_partial_overwrites() {
        let store = Arc::new(MemoryStore::default());
        {
            let io = FsIo::unlocked_for_test(store.clone()).unwrap();
            let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
            for i in 0..80 {
                write(
                    file.as_ref(),
                    (i * BLOCK_SIZE) as u64,
                    &vec![i as u8; BLOCK_SIZE],
                );
            }
            sync(file.as_ref()).unwrap();
        }
        store.block_gets.store(0, Ordering::Relaxed);
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io.open_file("agent.db", OpenFlags::None, false).unwrap();
        assert_eq!(file.size().unwrap(), (80 * BLOCK_SIZE) as u64);
        assert_eq!(
            store.block_gets.load(Ordering::Relaxed),
            0,
            "open and stat must not download blocks"
        );
        assert_eq!(read(file.as_ref(), 8), vec![0; 8]);
        assert_eq!(read(file.as_ref(), 8), vec![0; 8]);
        assert_eq!(
            store.block_gets.load(Ordering::Relaxed),
            1,
            "repeat read must hit cache"
        );
        write(file.as_ref(), (79 * BLOCK_SIZE + 2) as u64, b"changed");
        assert_eq!(
            store.block_gets.load(Ordering::Relaxed),
            2,
            "partial overwrite downloads only its source block"
        );
        write(
            file.as_ref(),
            (78 * BLOCK_SIZE) as u64,
            &vec![99; BLOCK_SIZE],
        );
        assert_eq!(
            store.block_gets.load(Ordering::Relaxed),
            2,
            "full overwrite needs no source block"
        );
        let all = read(file.as_ref(), 80 * BLOCK_SIZE);
        assert_eq!(&all[79 * BLOCK_SIZE..79 * BLOCK_SIZE + 2], &[79, 79]);
        assert_eq!(&all[79 * BLOCK_SIZE + 2..79 * BLOCK_SIZE + 9], b"changed");
        assert!(
            io.inner.state.lock().unwrap().files["agent.db"]
                .blocks
                .len()
                <= CACHE_BLOCKS + 2
        );
        sync(file.as_ref()).unwrap();
        let fresh = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        let all = read(fresh.as_ref(), 80 * BLOCK_SIZE);
        assert!(
            all[78 * BLOCK_SIZE..79 * BLOCK_SIZE]
                .iter()
                .all(|b| *b == 99)
        );
        assert_eq!(&all[79 * BLOCK_SIZE + 2..79 * BLOCK_SIZE + 9], b"changed");
    }
    #[test]
    fn failed_lazy_source_read_cannot_publish_a_partial_write() {
        let store = Arc::new(MemoryStore::default());
        {
            let io = FsIo::unlocked_for_test(store.clone()).unwrap();
            let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
            write(file.as_ref(), 0, &vec![1; BLOCK_SIZE]);
            write(file.as_ref(), BLOCK_SIZE as u64, &vec![2; BLOCK_SIZE]);
            sync(file.as_ref()).unwrap();
        }
        let path = format!("blocks/{}", digest(&vec![2; BLOCK_SIZE]));
        store.objects.lock().unwrap().get_mut(&path).unwrap().0[0] = 99;
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io.open_file("agent.db", OpenFlags::None, false).unwrap();
        let result = file.pwrite(
            0,
            Arc::new(Buffer::new(vec![3; BLOCK_SIZE + 1])),
            Completion::new_write(|_| {}),
        );
        assert!(result.is_err());
        assert!(
            sync(file.as_ref()).is_err(),
            "partially applied write must poison the session"
        );
        store.objects.lock().unwrap().get_mut(&path).unwrap().0[0] = 2;
        let fresh = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        assert_eq!(read(fresh.as_ref(), BLOCK_SIZE), vec![1; BLOCK_SIZE]);
    }
    #[test]
    fn sparse_truncate_and_incremental_sync_survive_reopen() {
        let store = Arc::new(MemoryStore::default());
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
        write(file.as_ref(), 0, &vec![7; BLOCK_SIZE * 2]);
        sync(file.as_ref()).unwrap();
        let count = store.objects.lock().unwrap().len();
        write(file.as_ref(), 3, &[9]);
        sync(file.as_ref()).unwrap();
        assert_eq!(
            store.objects.lock().unwrap().len(),
            count + 1,
            "only one changed block uploaded"
        );
        assert!(
            file.truncate(5, Completion::new_trunc(|_| {}))
                .unwrap()
                .finished()
        );
        assert!(
            file.truncate(20, Completion::new_trunc(|_| {}))
                .unwrap()
                .finished()
        );
        write(file.as_ref(), (BLOCK_SIZE + 2) as u64, &[8]);
        sync(file.as_ref()).unwrap();
        let reopened = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        let bytes = read(reopened.as_ref(), BLOCK_SIZE + 3);
        assert_eq!(&bytes[..5], &[7, 7, 7, 9, 7]);
        assert!(bytes[5..BLOCK_SIZE + 2].iter().all(|b| *b == 0));
        assert_eq!(bytes[BLOCK_SIZE + 2], 8);
    }
    #[test]
    fn stale_writer_and_failed_manifest_cannot_publish_partial_state() {
        let store = Arc::new(MemoryStore::default());
        let a = FsIo::unlocked_for_test(store.clone()).unwrap();
        let b = FsIo::unlocked_for_test(store.clone()).unwrap();
        let fa = a.open_file("agent.db", OpenFlags::Create, false).unwrap();
        let fb = b.open_file("agent.db", OpenFlags::Create, false).unwrap();
        write(fa.as_ref(), 0, b"winner");
        sync(fa.as_ref()).unwrap();
        write(fb.as_ref(), 0, b"stale");
        assert!(sync(fb.as_ref()).is_err());
        assert!(fb.size().is_err(), "failed session must remain poisoned");
        store.fail_manifest.store(true, Ordering::Relaxed);
        write(fa.as_ref(), 0, b"failed");
        assert!(sync(fa.as_ref()).is_err());
        store.fail_manifest.store(false, Ordering::Relaxed);
        let fresh = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        assert_eq!(read(fresh.as_ref(), 6), b"winner");
    }
    #[test]
    fn lost_acknowledgement_requires_fresh_session_and_recovers_committed_state() {
        let store = Arc::new(MemoryStore::default());
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
        write(file.as_ref(), 0, b"old");
        sync(file.as_ref()).unwrap();
        store.lose_ack.store(true, Ordering::Relaxed);
        write(file.as_ref(), 0, b"new");
        assert!(sync(file.as_ref()).is_err());
        assert!(
            sync(file.as_ref()).is_err(),
            "uncertain commit must not be retried"
        );
        store.lose_ack.store(false, Ordering::Relaxed);
        let fresh = FsIo::unlocked_for_test(store)
            .unwrap()
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        assert_eq!(read(fresh.as_ref(), 3), b"new");
    }
    #[test]
    fn locks_readonly_and_file_limits_are_enforced() {
        let io = FsIo::unlocked_for_test(Arc::new(MemoryStore::default())).unwrap();
        let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
        assert!(io.open_file("agent.db", OpenFlags::Create, false).is_err());
        file.unlock_file().unwrap();
        let second = io.open_file("agent.db", OpenFlags::None, false).unwrap();
        drop(file); // Old unlocked handle must not release the new owner's lock.
        assert!(io.open_file("agent.db", OpenFlags::None, false).is_err());
        let readonly = io
            .open_file("agent.db", OpenFlags::ReadOnly, false)
            .unwrap();
        assert!(
            readonly
                .pwrite(
                    0,
                    Arc::new(Buffer::new(vec![1])),
                    Completion::new_write(|_| {})
                )
                .is_err()
        );
        assert!(io.open_file("../escape", OpenFlags::Create, false).is_err());
        for index in 1..MAX_FILES {
            io.open_file(&format!("file{index}"), OpenFlags::Create, false)
                .unwrap();
        }
        assert!(io.open_file("overflow", OpenFlags::Create, false).is_err());
        assert!(
            second
                .truncate((MAX_FILE + 1) as u64, Completion::new_trunc(|_| {}))
                .is_err()
        );
    }
    #[test]
    fn corrupt_blocks_and_invalid_manifest_paths_are_rejected() {
        let store = Arc::new(MemoryStore::default());
        let io = FsIo::unlocked_for_test(store.clone()).unwrap();
        let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
        write(file.as_ref(), 0, b"valid");
        sync(file.as_ref()).unwrap();
        store
            .objects
            .lock()
            .unwrap()
            .get_mut(&format!("blocks/{}", digest(b"valid")))
            .unwrap()
            .0 = b"wrong".to_vec();
        let corrupt = FsIo::unlocked_for_test(store.clone())
            .unwrap()
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        assert!(
            corrupt
                .pread(
                    0,
                    Completion::new_read(Arc::new(Buffer::new_temporary(5)), |_| None)
                )
                .is_err()
        );
        let invalid = br#"{"format":"turso-tidbfs","files":{"../escape":{"size":0,"blocks":[]}}}"#;
        store
            .objects
            .lock()
            .unwrap()
            .insert("manifest.json".into(), (invalid.to_vec(), 100));
        assert!(FsIo::unlocked_for_test(store).is_err());
    }
    #[test]
    fn shared_database_connections_keep_reader_snapshots_and_exclude_second_writer() {
        fn value(c: &Arc<turso_core::Connection>) -> String {
            let mut s = c
                .query("SELECT value FROM items WHERE id=1")
                .unwrap()
                .unwrap();
            assert!(matches!(s.step().unwrap(), StepResult::Row));
            let value = s.row().unwrap().get_value(0).to_string();
            assert!(matches!(s.step().unwrap(), StepResult::Done));
            value
        }

        let store = Arc::new(MemoryStore::default());
        let io = Arc::new(FsIo::unlocked_for_test(store.clone()).unwrap());
        let db = Database::open_file(io, "agent.db", Arc::new(SqliteDialect)).unwrap();
        let writer = db.connect().unwrap();
        let reader = db.connect().unwrap();
        let contender = db.connect().unwrap();
        writer.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1,'initial');").unwrap();

        reader.execute("BEGIN").unwrap();
        assert_eq!(value(&reader), "initial");
        writer
            .execute("BEGIN IMMEDIATE; UPDATE items SET value='committed' WHERE id=1;")
            .unwrap();
        assert_eq!(
            value(&reader),
            "initial",
            "uncommitted data must stay hidden"
        );
        assert!(matches!(
            contender.execute("BEGIN IMMEDIATE"),
            Err(LimboError::Busy)
        ));
        writer.execute("COMMIT").unwrap();
        assert_eq!(
            value(&reader),
            "initial",
            "reader must retain its snapshot after commit"
        );
        reader.execute("COMMIT").unwrap();
        assert_eq!(value(&reader), "committed");
        contender
            .execute("BEGIN IMMEDIATE; UPDATE items SET value='next' WHERE id=1; COMMIT;")
            .unwrap();
        assert_eq!(
            value(&reader),
            "next",
            "writer ownership must pass after commit"
        );

        reader.close().unwrap();
        contender.close().unwrap();
        writer.close().unwrap();
        drop(db);
        let io = Arc::new(FsIo::unlocked_for_test(store).unwrap());
        let db = Database::open_file(io, "agent.db", Arc::new(SqliteDialect)).unwrap();
        let reopened = db.connect().unwrap();
        assert_eq!(
            value(&reopened),
            "next",
            "commits must survive a fresh session"
        );
    }

    #[test]
    fn turso_transactions_checkpoint_and_wal_recovery() {
        let store = Arc::new(MemoryStore::default());
        // Dropping does not explicitly checkpoint: a new engine must recover WAL.
        {
            let io = Arc::new(FsIo::unlocked_for_test(store.clone()).unwrap());
            let db = Database::open_file(io, "agent.db", Arc::new(SqliteDialect)).unwrap();
            let c = db.connect().unwrap();
            c.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); BEGIN; INSERT INTO items VALUES(1, 'kept'); COMMIT; BEGIN; INSERT INTO items VALUES(2, 'discarded'); ROLLBACK;").unwrap();
        }
        {
            let io = Arc::new(FsIo::unlocked_for_test(store.clone()).unwrap());
            let db = Database::open_file(io, "agent.db", Arc::new(SqliteDialect)).unwrap();
            let c = db.connect().unwrap();
            let mut s = c
                .query("SELECT count(*) FROM items WHERE value='kept'")
                .unwrap()
                .unwrap();
            assert!(matches!(s.step().unwrap(), StepResult::Row));
            assert_eq!(s.row().unwrap().get_value(0).to_string(), "1");
            drop(s);
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            c.close().unwrap();
        }
        let io = Arc::new(FsIo::unlocked_for_test(store).unwrap());
        let db = Database::open_file(io, "agent.db", Arc::new(SqliteDialect)).unwrap();
        let c = db.connect().unwrap();
        let mut s = c.query("PRAGMA integrity_check").unwrap().unwrap();
        assert!(matches!(s.step().unwrap(), StepResult::Row));
        assert_eq!(s.row().unwrap().get_value(0).to_string(), "ok");
    }
}

#[cfg(test)]
mod http_tests;
