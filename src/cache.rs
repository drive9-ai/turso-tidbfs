//! Bounded, verified immutable blocks shared across reader generations.
use super::*;
use std::sync::Condvar;

type Bytes = Arc<[u8]>;
type FetchResult = std::result::Result<Bytes, String>;
#[derive(Default)]
struct Flight {
    result: Mutex<Option<FetchResult>>,
    ready: Condvar,
}
struct Entry {
    bytes: Bytes,
    touched: u64,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<String, Entry>,
    flights: BTreeMap<String, Arc<Flight>>,
    clock: u64,
    hits: u64,
    misses: u64,
    waits: u64,
    evictions: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub waits: u64,
    pub evictions: u64,
    pub entries: usize,
    pub bytes: usize,
    pub capacity_blocks: usize,
}
pub struct BlockCache {
    capacity: usize,
    state: Mutex<State>,
}
impl BlockCache {
    pub fn new(capacity_blocks: usize) -> Result<Self> {
        if !(1..=4096).contains(&capacity_blocks) {
            return Err(err("shared cache must hold 1..4096 blocks"));
        }
        Ok(Self {
            capacity: capacity_blocks,
            state: Mutex::default(),
        })
    }
    pub fn stats(&self) -> CacheStats {
        let s = self.state.lock().unwrap();
        CacheStats {
            hits: s.hits,
            misses: s.misses,
            waits: s.waits,
            evictions: s.evictions,
            entries: s.entries.len(),
            bytes: s.entries.values().map(|e| e.bytes.len()).sum(),
            capacity_blocks: self.capacity,
        }
    }
    pub(super) fn get(
        &self,
        hash: &str,
        length: usize,
        fetch: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<Bytes> {
        let (flight, leader) = {
            let mut s = self.state.lock().unwrap();
            s.clock += 1;
            let now = s.clock;
            if let Some(entry) = s.entries.get_mut(hash) {
                if entry.bytes.len() != length {
                    return Err(err("corrupt FS block length"));
                }
                entry.touched = now;
                let bytes = entry.bytes.clone();
                s.hits += 1;
                return Ok(bytes);
            }
            if let Some(flight) = s.flights.get(hash).cloned() {
                s.waits += 1;
                (flight, false)
            } else {
                let flight = Arc::new(Flight::default());
                s.flights.insert(hash.into(), flight.clone());
                s.misses += 1;
                (flight, true)
            }
        };
        if !leader {
            let guard = flight.result.lock().unwrap();
            let guard = flight
                .ready
                .wait_while(guard, |value| value.is_none())
                .unwrap();
            let bytes = guard.as_ref().unwrap().clone().map_err(err)?;
            if bytes.len() != length {
                return Err(err("corrupt FS block length"));
            }
            return Ok(bytes);
        }
        // No cache mutex is held during I/O. Failures never become cache entries.
        let result = fetch()
            .and_then(|bytes| {
                if bytes.len() != length || bytes.len() > BLOCK_SIZE || digest(&bytes) != hash {
                    return Err(err("corrupt FS block"));
                }
                Ok(Arc::<[u8]>::from(bytes))
            })
            .map_err(|e| e.to_string());
        {
            let mut s = self.state.lock().unwrap();
            if let Ok(bytes) = &result {
                s.clock += 1;
                let touched = s.clock;
                s.entries.insert(
                    hash.into(),
                    Entry {
                        bytes: bytes.clone(),
                        touched,
                    },
                );
                while s.entries.len() > self.capacity {
                    let victim = s
                        .entries
                        .iter()
                        .min_by_key(|(_, e)| e.touched)
                        .unwrap()
                        .0
                        .clone();
                    s.entries.remove(&victim);
                    s.evictions += 1;
                }
            }
            *flight.result.lock().unwrap() = Some(result.clone());
            s.flights.remove(hash);
        }
        flight.ready.notify_all();
        result.map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread};
    #[test]
    fn verified_cache_is_bounded_and_does_not_retain_failed_fetches() {
        let cache = BlockCache::new(2).unwrap();
        let a = digest(b"a");
        let b = digest(b"b");
        let c = digest(b"c");
        cache.get(&a, 1, || Ok(b"a".to_vec())).unwrap();
        cache.get(&b, 1, || Ok(b"b".to_vec())).unwrap();
        cache
            .get(&a, 1, || panic!("cached block downloaded twice"))
            .unwrap();
        cache.get(&c, 1, || Ok(b"c".to_vec())).unwrap();
        assert_eq!(cache.stats().evictions, 1);
        assert_eq!(cache.stats().bytes, 2);
        assert!(cache.get(&b, 1, || Ok(b"wrong".to_vec())).is_err());
        assert!(cache.get(&b, 1, || Err(err("offline"))).is_err());
        cache.get(&b, 1, || Ok(b"b".to_vec())).unwrap();
        assert!(
            cache
                .get(&b, 2, || panic!("wrong length must fail on cache hit"))
                .is_err()
        );
        assert_eq!(cache.stats().entries, 2);
    }
    #[test]
    fn same_hash_downloads_are_coalesced_without_blocking_other_hashes() {
        let cache = Arc::new(BlockCache::new(2).unwrap());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let first = cache.clone();
        let leader = thread::spawn(move || {
            first.get(&digest(b"a"), 1, || {
                entered_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(b"a".to_vec())
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = cache.clone();
        let waiter =
            thread::spawn(move || second.get(&digest(b"a"), 1, || panic!("duplicate download")));
        let deadline = Instant::now() + Duration::from_secs(5);
        while cache.stats().waits == 0 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        cache.get(&digest(b"b"), 1, || Ok(b"b".to_vec())).unwrap();
        resume_tx.send(()).unwrap();
        assert_eq!(&*leader.join().unwrap().unwrap(), b"a");
        assert_eq!(&*waiter.join().unwrap().unwrap(), b"a");
        assert_eq!(cache.stats().misses, 2);
    }
}
