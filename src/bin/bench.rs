//! Identical SQL workloads over the same pinned Turso engine and FULL sync.
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use turso_core::{Connection, Database, SqliteDialect, StepResult, UnixIO, io::IO};
use turso_fs::{FsIo, HttpStore};
const ROWS: usize = 1024;
fn query(
    c: &Arc<Connection>,
    io: &dyn IO,
    sql: &str,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    if let Some(mut stmt) = c.query(sql)? {
        loop {
            match stmt.step()? {
                StepResult::Row => rows.push(
                    stmt.row()
                        .ok_or("missing row")?
                        .get_values()
                        .map(ToString::to_string)
                        .collect(),
                ),
                StepResult::Done => break,
                StepResult::IO | StepResult::Yield => io.step()?,
                StepResult::Sleep { duration } => std::thread::sleep(duration),
                _ => return Err("query interrupted or busy".into()),
            }
        }
    }
    Ok(rows)
}
fn measure(
    name: &str,
    ops: usize,
    store: Option<&HttpStore>,
    task: impl FnOnce() -> Result<(), Box<dyn std::error::Error>>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let before = store.map(HttpStore::metrics_snapshot);
    let start = Instant::now();
    task()?;
    let ms = start.elapsed().as_secs_f64() * 1000.;
    let mut result =
        json!({"name":name, "operations":ops, "elapsed_ms":ms, "ms_per_operation":ms / ops as f64});
    if let (Some(before), Some(store)) = (before, store) {
        let after = store.metrics_snapshot();
        result["http"] = json!({"gets":after.gets-before.gets,"puts":after.puts-before.puts,"downloaded_bytes":after.downloaded_bytes-before.downloaded_bytes,"uploaded_bytes":after.uploaded_bytes-before.uploaded_bytes});
    }
    Ok(result)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: bench local|fs PATH seed|read|transactions".into());
    }
    let (store, io, path): (Option<Arc<HttpStore>>, Arc<dyn IO>, String) = match args[1].as_str() {
        "local" => (None, Arc::new(UnixIO::new()?), args[2].clone()),
        "fs" => {
            let store = Arc::new(HttpStore::new(
                &std::env::var("DRIVE9_BASE")?,
                std::env::var("DRIVE9_API_KEY")?,
                &args[2],
            )?);
            store.prepare()?;
            (
                Some(store.clone()),
                Arc::new(FsIo::new(store)?),
                "agent.db".into(),
            )
        }
        _ => return Err("choose local or fs".into()),
    };
    let open_started = Instant::now();
    let db = Database::open_file(io.clone(), &path, Arc::new(SqliteDialect))?;
    let c = db.connect()?;
    c.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; PRAGMA cache_size=-256;")?;
    let mut phases = vec![
        json!({"name":"open_engine","operations":1,"elapsed_ms":open_started.elapsed().as_secs_f64()*1000.,"http_cumulative":store.as_ref().map(|s|s.metrics_snapshot())}),
    ];
    let st = store.as_deref();
    match args[3].as_str() {
        "seed" => {
            c.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT);")?;
            c.execute("BEGIN")?;
            phases.push(measure("buffered_insert", ROWS, st, || {
                for id in 0..ROWS {
                    c.execute(format!(
                        "INSERT INTO items VALUES({id},'{}')",
                        "x".repeat(512)
                    ))?;
                }
                Ok(())
            })?);
            phases.push(measure("bulk_commit", 1, st, || {
                c.execute("COMMIT")?;
                Ok(())
            })?);
            phases.push(measure("checkpoint", 1, st, || {
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
                Ok(())
            })?);
        }
        "read" => {
            for (name, count) in [
                ("point_read_one", 1),
                ("point_read_first_pass", 32),
                ("point_read_repeat", 128),
            ] {
                phases.push(measure(name, count, st, || {
                    for i in 0..count {
                        let id = (i % 32) * 31;
                        let rows = query(
                            &c,
                            io.as_ref(),
                            &format!("SELECT length(payload) FROM items WHERE id={id}"),
                        )?;
                        assert_eq!(rows, vec![vec!["512".to_owned()]]);
                    }
                    Ok(())
                })?);
            }
            phases.push(measure("full_scan", ROWS, st, || {
                let rows = query(&c, io.as_ref(), "SELECT sum(length(payload)) FROM items")?;
                assert_eq!(rows[0][0], (ROWS * 512).to_string());
                Ok(())
            })?);
        }
        "transactions" => {
            for (name, txns, rows) in [("single_row_commit", 12, 1), ("batch_32_commit", 6, 32)] {
                let mut commits = Vec::new();
                let mut writes = Vec::new();
                for round in 0..txns {
                    c.execute("BEGIN")?;
                    let start = Instant::now();
                    for i in 0..rows {
                        c.execute(format!(
                            "UPDATE items SET payload='{}' WHERE id={}",
                            if round % 2 == 0 {
                                "y".repeat(512)
                            } else {
                                "z".repeat(512)
                            },
                            (round * rows + i) % ROWS
                        ))?;
                    }
                    writes.push(start.elapsed().as_secs_f64() * 1000.);
                    let start = Instant::now();
                    c.execute("COMMIT")?;
                    commits.push(start.elapsed().as_secs_f64() * 1000.);
                }
                let mut ordered = commits.clone();
                ordered.sort_by(f64::total_cmp);
                phases.push(json!({"name":name,"operations":txns,"rows_per_transaction":rows,"elapsed_ms":commits.iter().sum::<f64>(),"ms_per_operation":commits.iter().sum::<f64>()/txns as f64,"commit_ms_samples":commits,"write_ms_samples":writes,"p50_ms":ordered[ordered.len()/2],"p95_ms":ordered[(ordered.len()*95/100).min(ordered.len()-1)]}));
            }
        }
        _ => return Err("choose seed, read or transactions".into()),
    }
    assert_eq!(
        query(&c, io.as_ref(), "PRAGMA integrity_check")?,
        vec![vec!["ok".to_owned()]]
    );
    c.close()?;
    println!(
        "{}",
        json!({"backend":args[1],"mode":args[3],"rows":ROWS,"payload_bytes":512,"synchronous":"FULL","pager_cache_kib":256,"phases":phases,"http_total":store.as_ref().map(|s|s.metrics_snapshot())})
    );
    Ok(())
}
