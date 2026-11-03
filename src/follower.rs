//! Prepare immutable engines off the query path; switch only between transactions.
use super::*;
use std::{
    sync::Condvar,
    thread::{self, JoinHandle},
};
use turso_core::{Connection, Database, OpenOptions, SqliteDialect, StepResult};

#[derive(Clone, Debug)]
pub struct ReaderOptions {
    pub refresh_interval: Duration,
    pub cache_blocks: usize,
}
impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(1),
            cache_blocks: 256,
        }
    }
}
impl ReaderOptions {
    pub fn from_env() -> Result<Self> {
        let defaults = Self::default();
        fn number(key: &str, default: u64) -> Result<u64> {
            match std::env::var(key) {
                Ok(value) => value.parse().map_err(|_| err(format!("invalid {key}"))),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(_) => Err(err(format!("invalid {key}"))),
            }
        }
        let options = Self {
            refresh_interval: Duration::from_millis(number(
                "TIDBFS_REFRESH_MS",
                defaults.refresh_interval.as_millis() as u64,
            )?),
            cache_blocks: usize::try_from(number(
                "TIDBFS_CACHE_BLOCKS",
                defaults.cache_blocks as u64,
            )?)
            .map_err(|_| err("invalid TIDBFS_CACHE_BLOCKS"))?,
        };
        options.validate()?;
        Ok(options)
    }
    fn validate(&self) -> Result<()> {
        if self.refresh_interval < Duration::from_millis(10)
            || self.refresh_interval > Duration::from_secs(60)
            || !(1..=4096).contains(&self.cache_blocks)
        {
            return Err(err(
                "reader refresh must be 10..60000 ms; cache must hold 1..4096 blocks",
            ));
        }
        Ok(())
    }
}
struct Generation {
    io: Arc<FsIo>,
    db: Arc<Database>,
    revision: u64,
    files: BTreeMap<String, Image>,
}
impl Generation {
    fn open(io: FsIo, name: &str) -> Result<Arc<Self>> {
        let (revision, files) = {
            let s = io.inner.state.lock().unwrap();
            (s.revision, s.manifest.files.clone())
        };
        let io = Arc::new(io);
        let db = Database::open(
            io.clone(),
            name,
            OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::ReadOnly),
        )?;
        let conn = db.connect()?;
        read_rows(&conn, io.as_ref(), "PRAGMA schema_version")?;
        conn.close()?;
        Ok(Arc::new(Self {
            io,
            db,
            revision,
            files,
        }))
    }
}
struct State {
    current: Arc<Generation>,
    observed_revision: u64,
    generations: u64,
    last_refresh_error: Option<String>,
}
struct Inner {
    store: Arc<dyn Store>,
    name: String,
    cache: Arc<BlockCache>,
    state: Mutex<State>,
    refreshing: Mutex<()>,
}
#[derive(Clone, Debug, Serialize)]
pub struct ReaderStatus {
    pub applied_revision: u64,
    pub observed_revision: u64,
    pub generations: u64,
    pub last_refresh_error: Option<String>,
    pub cache: CacheStats,
}
impl Inner {
    fn refresh(&self) -> Result<bool> {
        // Serialize background and explicit refreshes without blocking SQL on I/O.
        let _refresh = self.refreshing.lock().unwrap();
        let result = (|| {
            let io = FsIo::snapshot_with_cache(self.store.clone(), self.cache.clone())?;
            let (revision, files) = {
                let s = io.inner.state.lock().unwrap();
                (s.revision, s.manifest.files.clone())
            };
            {
                let mut s = self.state.lock().unwrap();
                if revision < s.observed_revision {
                    return Err(err("remote manifest revision moved backwards"));
                }
                s.observed_revision = revision;
                if files == s.current.files {
                    return Ok(false);
                } // Lease-only update.
            }
            let candidate = Generation::open(io, &self.name)?;
            let mut s = self.state.lock().unwrap();
            s.current = candidate;
            s.generations += 1;
            Ok(true)
        })();
        self.state.lock().unwrap().last_refresh_error =
            result.as_ref().err().map(ToString::to_string);
        result
    }
}
pub struct FollowingReader {
    inner: Arc<Inner>,
    worker: Option<Worker>,
}
impl FollowingReader {
    pub fn open(store: Arc<dyn Store>, name: &str, options: ReaderOptions) -> Result<Self> {
        Self::start(store, name, options, true)
    }
    fn start(
        store: Arc<dyn Store>,
        name: &str,
        options: ReaderOptions,
        background: bool,
    ) -> Result<Self> {
        options.validate()?;
        let cache = Arc::new(BlockCache::new(options.cache_blocks)?);
        let current = Generation::open(
            FsIo::snapshot_with_cache(store.clone(), cache.clone())?,
            name,
        )?;
        let observed_revision = current.revision;
        let inner = Arc::new(Inner {
            store,
            name: name.into(),
            cache,
            state: Mutex::new(State {
                current,
                observed_revision,
                generations: 1,
                last_refresh_error: None,
            }),
            refreshing: Mutex::new(()),
        });
        let worker = if background {
            Some(Worker::start(&inner, options.refresh_interval)?)
        } else {
            None
        };
        Ok(Self { inner, worker })
    }
    pub fn connect(&self) -> Result<FollowingConnection> {
        let generation = self.inner.state.lock().unwrap().current.clone();
        let conn = generation.db.connect()?;
        Ok(FollowingConnection {
            inner: self.inner.clone(),
            generation,
            conn,
        })
    }
    /// Poll once, retaining the last usable engine if a newer one cannot open.
    pub fn refresh(&self) -> Result<bool> {
        self.inner.refresh()
    }
    pub fn status(&self) -> ReaderStatus {
        let s = self.inner.state.lock().unwrap();
        ReaderStatus {
            applied_revision: s.current.revision,
            observed_revision: s.observed_revision,
            generations: s.generations,
            last_refresh_error: s.last_refresh_error.clone(),
            cache: self.inner.cache.stats(),
        }
    }
}
impl Drop for FollowingReader {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.stop();
        }
    }
}

/// Each execute/query call is an upgrade boundary, unless an explicit SQL
/// transaction or savepoint is open. Results are fully materialized before return.
pub struct FollowingConnection {
    inner: Arc<Inner>,
    generation: Arc<Generation>,
    conn: Arc<Connection>,
}
impl FollowingConnection {
    fn upgrade(&mut self) -> Result<()> {
        if self.conn.is_closed() {
            return Err(err("read connection is closed"));
        }
        if !self.conn.get_auto_commit() {
            return Ok(());
        }
        let latest = self.inner.state.lock().unwrap().current.clone();
        if !Arc::ptr_eq(&latest, &self.generation) {
            let conn = latest.db.connect()?;
            self.conn.close()?;
            self.conn = conn;
            self.generation = latest;
        }
        Ok(())
    }
    pub fn execute(&mut self, sql: &str) -> Result<()> {
        self.upgrade()?;
        self.conn.execute(sql)
    }
    pub fn query(&mut self, sql: &str) -> Result<Vec<Vec<String>>> {
        self.upgrade()?;
        read_rows(&self.conn, self.generation.io.as_ref(), sql)
    }
    pub fn snapshot_revision(&self) -> u64 {
        self.generation.revision
    }
    pub fn close(&self) -> Result<()> {
        self.conn.close()
    }
}
impl Drop for FollowingConnection {
    fn drop(&mut self) {
        let _ = self.conn.close();
    }
}
fn read_rows(conn: &Arc<Connection>, io: &dyn IO, sql: &str) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    if let Some(mut stmt) = conn.query(sql)? {
        loop {
            match stmt.step()? {
                StepResult::Row => rows.push(
                    stmt.row()
                        .ok_or_else(|| err("missing query row"))?
                        .get_values()
                        .map(ToString::to_string)
                        .collect(),
                ),
                StepResult::Done => break,
                StepResult::IO | StepResult::Yield => io.step()?,
                StepResult::Sleep { duration } => thread::sleep(duration),
                _ => return Err(err("query busy or interrupted")),
            }
        }
    }
    Ok(rows)
}
struct Worker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    handle: JoinHandle<()>,
}
impl Worker {
    fn start(inner: &Arc<Inner>, interval: Duration) -> Result<Self> {
        let weak = Arc::downgrade(inner);
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let signal = stop.clone();
        let handle = thread::Builder::new()
            .name("tidbfs-reader".into())
            .spawn(move || {
                loop {
                    let (lock, cv) = &*signal;
                    let (guard, _) = cv
                        .wait_timeout_while(lock.lock().unwrap(), interval, |stop| !*stop)
                        .unwrap();
                    if *guard {
                        break;
                    }
                    drop(guard);
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let _ = inner.refresh(); // Error is exposed in ReaderStatus; next poll retries.
                }
            })
            .map_err(|_| err("start reader refresh"))?;
        Ok(Self { stop, handle })
    }
    fn stop(self) {
        let (lock, cv) = &*self.stop;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        let _ = self.handle.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Memory {
        objects: Mutex<BTreeMap<String, (Vec<u8>, u64)>>,
        gets: Mutex<BTreeMap<String, usize>>,
        fail_reads: AtomicBool,
        gate: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    }
    impl Store for Memory {
        fn get(&self, name: &str) -> Result<Option<(Vec<u8>, u64)>> {
            if name == "manifest.json" {
                let gate = self.gate.lock().unwrap().take();
                if let Some((entered, resume)) = gate {
                    entered.send(()).unwrap();
                    resume.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            }
            if self.fail_reads.load(Ordering::SeqCst) {
                return Err(err("injected read outage"));
            }
            *self.gets.lock().unwrap().entry(name.into()).or_default() += 1;
            Ok(self.objects.lock().unwrap().get(name).cloned())
        }
        fn put(&self, name: &str, bytes: &[u8], expected: u64) -> Result<u64> {
            let mut s = self.objects.lock().unwrap();
            let revision = s.get(name).map_or(0, |v| v.1);
            if revision != expected {
                return Err(LimboError::LockingError("conflict".into()));
            }
            s.insert(name.into(), (bytes.to_vec(), revision + 1));
            Ok(revision + 1)
        }
    }
    struct Writer {
        io: Arc<FsIo>,
        _db: Arc<Database>,
        conn: Arc<Connection>,
    }
    impl Writer {
        fn new(store: Arc<Memory>) -> Self {
            let io = Arc::new(FsIo::new(store).unwrap());
            let db = Database::open_file(io.clone(), "agent.db", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0")
                .unwrap();
            Self { io, _db: db, conn }
        }
        fn close(self) {
            self.conn.close().unwrap();
            self.io.close().unwrap();
        }
    }
    fn follower(store: Arc<Memory>) -> FollowingReader {
        FollowingReader::start(
            store,
            "agent.db",
            ReaderOptions {
                cache_blocks: 64,
                ..ReaderOptions::default()
            },
            false,
        )
        .unwrap()
    }
    fn count(conn: &mut FollowingConnection) -> String {
        conn.query("SELECT count(*) FROM items").unwrap()[0][0].clone()
    }
    #[test]
    fn queries_follow_commits_but_transactions_and_savepoints_keep_their_generation() {
        let store = Arc::new(Memory::default());
        let w = Writer::new(store.clone());
        w.conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
            .unwrap();
        let reader = follower(store.clone());
        let mut pinned = reader.connect().unwrap();
        let mut live = reader.connect().unwrap();
        pinned.execute("BEGIN").unwrap();
        assert_eq!(count(&mut pinned), "1");
        let revision = pinned.snapshot_revision();
        w.conn
            .execute("BEGIN; INSERT INTO items VALUES(2)")
            .unwrap();
        assert!(!reader.refresh().unwrap());
        assert_eq!(count(&mut live), "1", "uncommitted rows must stay hidden");
        w.conn.execute("COMMIT").unwrap();
        assert!(reader.refresh().unwrap());
        assert_eq!(count(&mut live), "2");
        assert_eq!(count(&mut pinned), "1");
        assert_eq!(pinned.snapshot_revision(), revision);
        pinned.execute("COMMIT").unwrap();
        assert_eq!(count(&mut pinned), "2");
        pinned.execute("SAVEPOINT outer_read").unwrap();
        assert_eq!(count(&mut pinned), "2");
        w.conn.execute("INSERT INTO items VALUES(3)").unwrap();
        reader.refresh().unwrap();
        assert_eq!(count(&mut pinned), "2");
        pinned
            .execute("ROLLBACK TO outer_read; RELEASE outer_read")
            .unwrap();
        assert_eq!(count(&mut pinned), "3");
        assert!(live.execute("INSERT INTO items VALUES(9)").is_err());
        assert_eq!(count(&mut live), "3");
        live.close().unwrap();
        w.conn.execute("INSERT INTO items VALUES(4)").unwrap();
        reader.refresh().unwrap();
        assert!(
            live.query("SELECT count(*) FROM items").is_err(),
            "refresh must not reopen a closed connection"
        );
        w.close();
    }
    #[test]
    fn checkpoint_wal_restart_and_writer_handoff_preserve_old_readers_and_follow_schema() {
        let store = Arc::new(Memory::default());
        let w = Writer::new(store.clone());
        w.conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT); INSERT INTO items VALUES(1,'old')").unwrap();
        let reader = follower(store.clone());
        let mut old = reader.connect().unwrap();
        let mut live = reader.connect().unwrap();
        old.execute("BEGIN").unwrap();
        assert_eq!(count(&mut old), "1");
        w.conn.execute("INSERT INTO items VALUES(2,'new')").unwrap();
        reader.refresh().unwrap();
        assert_eq!(count(&mut live), "2");
        w.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        reader.refresh().unwrap();
        assert_eq!(count(&mut live), "2");
        assert_eq!(count(&mut old), "1");
        for id in 3..6 {
            w.conn
                .execute(format!("INSERT INTO items VALUES({id},'restart')"))
                .unwrap();
            assert!(reader.refresh().unwrap());
            assert_eq!(count(&mut live), id.to_string());
            w.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            reader.refresh().unwrap();
        }
        w.close();
        reader.refresh().unwrap();
        let generations = reader.status().generations;
        let next = Writer::new(store.clone());
        assert!(
            !reader.refresh().unwrap(),
            "lease acquisition must not reopen an unchanged engine"
        );
        assert_eq!(reader.status().generations, generations);
        next.conn.execute("ALTER TABLE items ADD COLUMN note TEXT; INSERT INTO items VALUES(6,'handoff','new schema')").unwrap();
        reader.refresh().unwrap();
        assert_eq!(
            live.query("SELECT note FROM items WHERE id=6").unwrap(),
            vec![vec!["new schema"]]
        );
        assert_eq!(count(&mut old), "1");
        old.execute("ROLLBACK").unwrap();
        assert_eq!(count(&mut old), "6");
        assert_eq!(
            old.query("PRAGMA integrity_check").unwrap(),
            vec![vec!["ok"]]
        );
        next.close();
    }
    #[test]
    fn shared_cache_reuses_unchanged_database_blocks_across_generations() {
        let store = Arc::new(Memory::default());
        let w = Writer::new(store.clone());
        w.conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT); BEGIN")
            .unwrap();
        for id in 0..128 {
            w.conn
                .execute(format!(
                    "INSERT INTO items VALUES({id},'{}')",
                    "x".repeat(1024)
                ))
                .unwrap();
        }
        w.conn
            .execute("COMMIT; PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        let reader = follower(store.clone());
        let mut conn = reader.connect().unwrap();
        assert_eq!(
            conn.query("SELECT sum(length(payload)) FROM items")
                .unwrap(),
            vec![vec![(128 * 1024).to_string()]]
        );
        let before = store.gets.lock().unwrap().clone();
        let block_gets: usize = before
            .iter()
            .filter(|(k, _)| k.starts_with("blocks/"))
            .map(|(_, n)| n)
            .sum();
        assert!(block_gets > 0);
        w.conn
            .execute("INSERT INTO items VALUES(128,'small commit')")
            .unwrap();
        reader.refresh().unwrap();
        assert_eq!(count(&mut conn), "129");
        for (key, reads) in &before {
            if key.starts_with("blocks/") {
                assert_eq!(
                    store.gets.lock().unwrap()[key],
                    *reads,
                    "unchanged block re-downloaded: {key}"
                );
            }
        }
        assert!(reader.status().cache.hits > 0);
        assert!(reader.status().cache.bytes <= 64 * BLOCK_SIZE);
        w.close();
    }
    #[test]
    fn preparing_a_generation_does_not_hold_the_query_selection_mutex() {
        use std::sync::mpsc;
        let store = Arc::new(Memory::default());
        let w = Writer::new(store.clone());
        w.conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
            .unwrap();
        let reader = follower(store.clone());
        let mut conn = reader.connect().unwrap();
        assert_eq!(count(&mut conn), "1");
        w.conn.execute("INSERT INTO items VALUES(2)").unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        *store.gate.lock().unwrap() = Some((entered_tx, resume_rx));
        let inner = reader.inner.clone();
        let prepare = thread::spawn(move || inner.refresh());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (rows_tx, rows_rx) = mpsc::channel();
        let query = thread::spawn(move || {
            rows_tx.send(count(&mut conn)).unwrap();
            conn
        });
        let rows = rows_rx.recv_timeout(Duration::from_secs(1));
        resume_tx.send(()).unwrap();
        assert_eq!(
            rows.unwrap(),
            "1",
            "cached queries must continue while refresh I/O is gated"
        );
        assert!(prepare.join().unwrap().unwrap());
        let mut conn = query.join().unwrap();
        assert_eq!(count(&mut conn), "2");
        w.close();
    }
    #[test]
    fn failed_refresh_keeps_last_usable_snapshot_and_recovers_without_restarting() {
        let store = Arc::new(Memory::default());
        let w = Writer::new(store.clone());
        w.conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
            .unwrap();
        let reader = follower(store.clone());
        let mut conn = reader.connect().unwrap();
        let initial = reader.status().applied_revision;
        w.conn.execute("INSERT INTO items VALUES(2)").unwrap();
        store.fail_reads.store(true, Ordering::SeqCst);
        assert!(reader.refresh().is_err());
        assert_eq!(reader.status().applied_revision, initial);
        assert!(reader.status().last_refresh_error.is_some());
        assert_eq!(count(&mut conn), "1");
        store.fail_reads.store(false, Ordering::SeqCst);
        assert!(reader.refresh().unwrap());
        assert_eq!(count(&mut conn), "2");
        assert!(reader.status().last_refresh_error.is_none());
        // A malformed candidate must not displace the verified engine either.
        let original = store.objects.lock().unwrap()["manifest.json"].clone();
        store.objects.lock().unwrap().insert(
            "manifest.json".into(),
            (b"invalid".to_vec(), original.1 + 1),
        );
        assert!(reader.refresh().is_err());
        assert_eq!(count(&mut conn), "2");
        store
            .objects
            .lock()
            .unwrap()
            .insert("manifest.json".into(), (original.0, original.1 + 2));
        assert!(!reader.refresh().unwrap());
        assert!(reader.status().last_refresh_error.is_none());
        // Writer's old revision was intentionally invalidated by this fixture.
        w.io.inner.state.lock().unwrap().poisoned = true;
    }
}
