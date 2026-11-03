//! Directory-level writer ownership. Lease updates and database publication use
//! the same CAS object, so takeover fences even an already in-flight old write.
use super::*;
use std::{
    sync::{Condvar, Weak},
    thread::{self, JoinHandle},
};

#[derive(Clone, Debug)]
pub struct SessionOptions {
    /// Confirmed lifetime, measured from the start of a publication request.
    pub ttl: Duration,
    /// Idle renewal interval; commits also renew ownership.
    pub heartbeat: Duration,
    /// Time to observe/retry contention. Zero means fail immediately when busy.
    pub lock_timeout: Duration,
}
impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            heartbeat: Duration::from_secs(5),
            lock_timeout: Duration::ZERO,
        }
    }
}
impl SessionOptions {
    fn validate(&self) -> Result<()> {
        if self.ttl < Duration::from_millis(100)
            || self.ttl > Duration::from_secs(300)
            || self.heartbeat.is_zero()
            || self.heartbeat > self.ttl / 3
        {
            return Err(err(
                "lease TTL must be 100..300000 ms; heartbeat must be positive and at most TTL/3",
            ));
        }
        Ok(())
    }
    pub fn from_env() -> Result<Self> {
        fn duration(key: &str, default: Duration) -> Result<Duration> {
            match std::env::var(key) {
                Ok(value) => value
                    .parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|_| err(format!("invalid {key}"))),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(_) => Err(err(format!("invalid {key}"))),
            }
        }
        let defaults = Self::default();
        let options = Self {
            ttl: duration("TIDBFS_LEASE_TTL_MS", defaults.ttl)?,
            heartbeat: duration("TIDBFS_HEARTBEAT_MS", defaults.heartbeat)?,
            lock_timeout: duration("TIDBFS_LOCK_TIMEOUT_MS", Duration::from_secs(90))?,
        };
        options.validate()?;
        Ok(options)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoteLease {
    pub owner: String,
    pub sequence: u64,
    pub ttl_ms: u64,
}
impl RemoteLease {
    pub fn validate(&self) -> Result<()> {
        if self.owner.len() != 64
            || !self.owner.bytes().all(|c| c.is_ascii_hexdigit())
            || !(100..=300000).contains(&self.ttl_ms)
        {
            return Err(err("invalid remote writer lease"));
        }
        Ok(())
    }
}
pub(super) struct LocalLease {
    pub deadline: Instant,
    pub ttl: Duration,
}
pub(super) trait SessionClock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}
pub(super) struct SystemClock;
impl SessionClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn sleep(&self, duration: Duration) {
        thread::sleep(duration);
    }
}
pub(super) fn before_deadline(now: Instant, deadline: Instant) -> Result<()> {
    if now >= deadline {
        Err(err("FS session failed or expired; reopen"))
    } else {
        Ok(())
    }
}

impl FsIo {
    /// Acquire the directory's writer lease before opening Turso. Never reuse an
    /// expired instance: create a new FsIo and a new Database after takeover.
    pub fn writer(store: Arc<dyn Store>, options: SessionOptions) -> Result<Self> {
        Self::acquire(store, options, Arc::new(SystemClock), true)
    }
    /// Pin a read-only, immutable database + WAL snapshot without blocking the
    /// writer. Reopen to see newer commits. No block GC may reclaim its blocks.
    pub fn snapshot(store: Arc<dyn Store>) -> Result<Self> {
        let io = Self::load(store)?;
        io.inner.state.lock().unwrap().readonly = true;
        Ok(io)
    }
    pub fn ensure_healthy(&self) -> Result<()> {
        self.inner.healthy(&self.inner.state.lock().unwrap())
    }
    fn acquire(
        store: Arc<dyn Store>,
        options: SessionOptions,
        clock: Arc<dyn SessionClock>,
        background: bool,
    ) -> Result<Self> {
        options.validate()?;
        let started = clock.now();
        let mut observed: Option<(RemoteLease, Instant)> = None;
        loop {
            let mut io = Self::load(store.clone())?;
            Arc::get_mut(&mut io.inner).unwrap().clock = clock.clone();
            let mut state = io.inner.state.lock().unwrap();
            let now = clock.now();
            let available = if let Some(remote) = &state.manifest.lease {
                if observed.as_ref().is_none_or(|(old, _)| old != remote) {
                    observed = Some((remote.clone(), now));
                }
                now.duration_since(observed.as_ref().unwrap().1)
                    >= Duration::from_millis(remote.ttl_ms)
            } else {
                true
            };
            if available {
                let mut manifest = state.manifest.clone();
                manifest.lease = Some(RemoteLease {
                    owner: io.inner.session.clone(),
                    sequence: 0,
                    ttl_ms: options.ttl.as_millis() as u64,
                });
                let bytes =
                    serde_json::to_vec(&manifest).map_err(|_| err("encode writer lease"))?;
                if bytes.len() + manifest.lease_padding() > MAX_MANIFEST {
                    return Err(err("manifest exceeds prototype limit"));
                }
                let claim_started = clock.now();
                match store.put_manifest(&bytes, state.revision, claim_started + options.ttl) {
                    Ok(revision) => {
                        before_deadline(clock.now(), claim_started + options.ttl)?;
                        state.manifest = manifest;
                        state.revision = revision;
                        state.lease = Some(LocalLease {
                            deadline: claim_started + options.ttl,
                            ttl: options.ttl,
                        });
                        drop(state);
                        if background {
                            *io.heartbeat.lock().unwrap() = Some(Heartbeat::start(
                                Arc::downgrade(&io.inner),
                                options.heartbeat,
                            )?);
                        }
                        return Ok(io);
                    }
                    // Only a definite CAS conflict is safe to re-observe. An unknown
                    // claim outcome must expire; do not retry or release its owner.
                    Err(LimboError::LockingError(_)) => (),
                    Err(e) => return Err(e),
                }
            }
            drop(state);
            let elapsed = clock.now().duration_since(started);
            if elapsed >= options.lock_timeout {
                return Err(LimboError::LockingError(
                    "remote database is busy; writer lease is held (lock wait timed out)".into(),
                ));
            }
            clock.sleep(Duration::from_millis(250).min(options.lock_timeout - elapsed));
        }
    }
    /// Stop renewal and release ownership after all connections have closed.
    /// Failed sessions are never released or revived; contenders wait for expiry.
    pub fn close(&self) -> Result<()> {
        if let Some(worker) = self.heartbeat.lock().unwrap().take() {
            worker.stop();
        }
        let mut state = self.inner.state.lock().unwrap();
        if state.poisoned {
            return Ok(());
        }
        if state.lease.is_none() {
            if state.readonly {
                state.poisoned = true;
            }
            return Ok(());
        }
        let result = (|| {
            self.inner.healthy(&state)?;
            let mut manifest = state.manifest.clone();
            manifest.lease = None;
            let bytes = serde_json::to_vec(&manifest).map_err(|_| err("encode lease release"))?;
            let revision = self.inner.store.put_manifest(
                &bytes,
                state.revision,
                state.lease.as_ref().unwrap().deadline,
            )?;
            self.inner.healthy(&state)?;
            state.revision = revision;
            state.manifest = manifest;
            Ok(())
        })();
        state.poisoned = true;
        result
    }
}
impl Drop for FsIo {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl Inner {
    fn renew(&self, interval: Duration) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let result = (|| {
            self.healthy(&state)?;
            let lease = state
                .lease
                .as_ref()
                .ok_or_else(|| err("missing local lease"))?;
            if self.clock.now() < lease.deadline - lease.ttl + interval {
                return Ok(());
            }
            let manifest = state.manifest.clone();
            self.publish(&mut state, manifest)
        })();
        if result.is_err() {
            state.poisoned = true;
        }
        result
    }
}
pub(super) struct Heartbeat {
    stop: Arc<(Mutex<bool>, Condvar)>,
    worker: JoinHandle<()>,
}
impl Heartbeat {
    fn start(inner: Weak<Inner>, interval: Duration) -> Result<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let signal = stop.clone();
        let worker = thread::Builder::new()
            .name("tidbfs-heartbeat".into())
            .spawn(move || {
                loop {
                    let (lock, cv) = &*signal;
                    let guard = lock.lock().unwrap();
                    let (guard, _) = cv
                        .wait_timeout_while(guard, interval, |stop| !*stop)
                        .unwrap();
                    if *guard {
                        break;
                    }
                    drop(guard);
                    let Some(inner) = inner.upgrade() else {
                        break;
                    };
                    if inner.renew(interval).is_err() {
                        break;
                    }
                }
            })
            .map_err(|_| err("start writer heartbeat"))?;
        Ok(Self { stop, worker })
    }
    fn stop(self) {
        let (lock, cv) = &*self.stop;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        let _ = self.worker.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use turso_core::{Database, OpenOptions, SqliteDialect, StepResult};

    struct ManualClock {
        base: Instant,
        millis: AtomicU64,
    }
    impl ManualClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                base: Instant::now(),
                millis: AtomicU64::new(0),
            })
        }
        fn advance(&self, ms: u64) {
            self.millis.fetch_add(ms, Ordering::SeqCst);
        }
    }
    impl SessionClock for ManualClock {
        fn now(&self) -> Instant {
            self.base + Duration::from_millis(self.millis.load(Ordering::SeqCst))
        }
        fn sleep(&self, duration: Duration) {
            self.advance(duration.as_millis() as u64);
        }
    }
    type PublicationGate = (mpsc::Sender<()>, mpsc::Receiver<()>);
    #[derive(Default)]
    struct Memory {
        objects: Mutex<BTreeMap<String, (Vec<u8>, u64)>>,
        lose_ack: AtomicBool,
        gate: Mutex<Option<PublicationGate>>,
        late_ack: Mutex<Option<Arc<ManualClock>>>,
    }
    impl Store for Memory {
        fn get(&self, name: &str) -> Result<Option<(Vec<u8>, u64)>> {
            Ok(self.objects.lock().unwrap().get(name).cloned())
        }
        fn put(&self, name: &str, bytes: &[u8], expected: u64) -> Result<u64> {
            let gate = self.gate.lock().unwrap().take();
            if let Some((entered, resume)) = gate {
                entered.send(()).unwrap();
                resume.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            let mut objects = self.objects.lock().unwrap();
            let revision = objects.get(name).map_or(0, |v| v.1);
            if revision != expected {
                return Err(LimboError::LockingError("CAS conflict".into()));
            }
            objects.insert(name.into(), (bytes.to_vec(), revision + 1));
            if let Some(clock) = self.late_ack.lock().unwrap().take() {
                clock.advance(1000);
            }
            if self.lose_ack.swap(false, Ordering::SeqCst) {
                return Err(err("lost acknowledgement"));
            }
            Ok(revision + 1)
        }
    }
    fn options(wait: u64) -> SessionOptions {
        SessionOptions {
            ttl: Duration::from_millis(900),
            heartbeat: Duration::from_millis(200),
            lock_timeout: Duration::from_millis(wait),
        }
    }
    fn writer(store: &Arc<Memory>, clock: &Arc<ManualClock>, wait: u64) -> FsIo {
        FsIo::acquire(store.clone(), options(wait), clock.clone(), false).unwrap()
    }
    fn scalar(db: &Arc<Database>) -> String {
        let c = db.connect().unwrap();
        let mut s = c
            .query("SELECT value FROM items WHERE id=1")
            .unwrap()
            .unwrap();
        assert!(matches!(s.step().unwrap(), StepResult::Row));
        let value = s.row().unwrap().get_value(0).to_string();
        assert!(matches!(s.step().unwrap(), StepResult::Done));
        value
    }
    #[test]
    fn writer_exclusion_heartbeat_release_and_expired_owner() {
        let store = Arc::new(Memory::default());
        let clock = ManualClock::new();
        let first = writer(&store, &clock, 0);
        assert!(FsIo::acquire(store.clone(), options(0), clock.clone(), false).is_err());
        clock.advance(500);
        first.inner.renew(options(0).heartbeat).unwrap();
        assert!(first.ensure_healthy().is_ok());
        let prior = store.get("manifest.json").unwrap().unwrap();
        let second = writer(&store, &clock, 1000);
        assert!(first.ensure_healthy().is_err());
        assert!(first.inner.renew(options(0).heartbeat).is_err());
        assert!(
            store.put("manifest.json", &prior.0, prior.1).is_err(),
            "takeover must fence an old request at the store CAS"
        );
        let owner = second.inner.session.clone();
        let _ = first.close();
        let remote: Manifest =
            serde_json::from_slice(&store.get("manifest.json").unwrap().unwrap().0).unwrap();
        assert_eq!(
            remote.lease.unwrap().owner,
            owner,
            "expired close cannot release another owner"
        );
        second.close().unwrap();
        let third = writer(&store, &clock, 0);
        assert!(
            third.ensure_healthy().is_ok(),
            "clean close permits immediate acquisition"
        );
    }
    #[test]
    fn late_or_lost_renewal_ack_never_revives_a_session() {
        for late in [true, false] {
            let store = Arc::new(Memory::default());
            let clock = ManualClock::new();
            let first = writer(&store, &clock, 0);
            clock.advance(300);
            if late {
                *store.late_ack.lock().unwrap() = Some(clock.clone());
            } else {
                store.lose_ack.store(true, Ordering::SeqCst);
            }
            assert!(first.inner.renew(options(0).heartbeat).is_err());
            assert!(first.ensure_healthy().is_err());
            assert!(first.inner.renew(Duration::ZERO).is_err());
            let second = writer(&store, &clock, 1000);
            assert!(second.ensure_healthy().is_ok());
        }
    }
    #[test]
    fn uncertain_or_late_claim_is_not_retried_or_released() {
        for late in [false, true] {
            let store = Arc::new(Memory::default());
            let clock = ManualClock::new();
            if late {
                *store.late_ack.lock().unwrap() = Some(clock.clone());
            } else {
                store.lose_ack.store(true, Ordering::SeqCst);
            }
            assert!(FsIo::acquire(store.clone(), options(2000), clock.clone(), false).is_err());
            let (bytes, revision) = store.get("manifest.json").unwrap().unwrap();
            let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
            assert!(
                manifest.lease.is_some(),
                "unknown claim must be left to expire"
            );
            assert_eq!(
                revision, 1,
                "neither retry nor release may follow an unknown claim"
            );
            let next = writer(&store, &clock, 1000);
            assert!(next.ensure_healthy().is_ok());
        }
    }
    #[test]
    fn delayed_old_publication_is_rejected_after_takeover() {
        let store = Arc::new(Memory::default());
        let clock = ManualClock::new();
        let first = Arc::new(writer(&store, &clock, 0));
        let file = first
            .open_file("agent.db-wal", OpenFlags::Create, false)
            .unwrap();
        let completion = file
            .pwrite(
                0,
                Arc::new(Buffer::new(b"old".to_vec())),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        assert!(completion.finished());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        *store.gate.lock().unwrap() = Some((entered_tx, resume_rx));
        let old_request =
            thread::spawn(move || file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync));
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = writer(&store, &clock, 1000);
        resume_tx.send(()).unwrap();
        assert!(old_request.join().unwrap().is_err());
        assert!(first.ensure_healthy().is_err());
        assert!(
            !second
                .inner
                .state
                .lock()
                .unwrap()
                .manifest
                .files
                .contains_key("agent.db-wal"),
            "old bytes cannot be published after takeover"
        );
        second.close().unwrap();
    }
    #[test]
    fn racing_acquisitions_grant_exactly_one_writer() {
        let store = Arc::new(Memory::default());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    FsIo::acquire(store, options(0), Arc::new(SystemClock), false)
                })
            })
            .collect();
        barrier.wait();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    }
    #[test]
    fn independent_readers_pin_database_and_wal_while_writer_commits() {
        let store = Arc::new(Memory::default());
        let clock = ManualClock::new();
        let io = Arc::new(writer(&store, &clock, 0));
        let db = Database::open_file(io.clone(), "agent.db", Arc::new(SqliteDialect)).unwrap();
        let c = db.connect().unwrap();
        c.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1,'initial');").unwrap();
        let open_reader = || {
            let io = Arc::new(FsIo::snapshot(store.clone()).unwrap());
            Database::open(
                io,
                "agent.db",
                OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::ReadOnly),
            )
            .unwrap()
        };
        let reader = open_reader();
        assert_eq!(scalar(&reader), "initial");
        c.execute("BEGIN; UPDATE items SET value='committed'; COMMIT;")
            .unwrap();
        assert_eq!(scalar(&reader), "initial");
        let latest = open_reader();
        assert_eq!(scalar(&latest), "committed");
        let read_conn = latest.connect().unwrap();
        assert!(
            read_conn
                .execute("UPDATE items SET value='forbidden'")
                .is_err()
        );
        c.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(
            scalar(&reader),
            "initial",
            "checkpoint must not invalidate an older snapshot"
        );
        assert_eq!(scalar(&latest), "committed");
        c.close().unwrap();
        io.close().unwrap();
        let successor = Arc::new(writer(&store, &clock, 0));
        let db =
            Database::open_file(successor.clone(), "agent.db", Arc::new(SqliteDialect)).unwrap();
        assert_eq!(scalar(&db), "committed");
    }
    #[test]
    fn snapshot_adapter_rejects_mutation_even_with_writable_handle_flags() {
        let store = Arc::new(Memory::default());
        let clock = ManualClock::new();
        let owner = writer(&store, &clock, 0);
        let file = owner
            .open_file("agent.db", OpenFlags::Create, false)
            .unwrap();
        let completion = file
            .pwrite(
                0,
                Arc::new(Buffer::new(b"saved".to_vec())),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        assert!(completion.finished());
        let completion = file
            .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .unwrap();
        assert!(completion.finished());
        let snapshot = FsIo::snapshot(store.clone()).unwrap();
        let before = store.get("manifest.json").unwrap();
        let handle = snapshot
            .open_file("agent.db", OpenFlags::None, false)
            .unwrap();
        assert!(
            handle
                .pwrite(
                    0,
                    Arc::new(Buffer::new(b"wrong".to_vec())),
                    Completion::new_write(|_| {})
                )
                .is_err()
        );
        assert!(handle.truncate(0, Completion::new_trunc(|_| {})).is_err());
        assert!(snapshot.remove_file("agent.db").is_err());
        assert_eq!(store.get("manifest.json").unwrap(), before);
    }
}
