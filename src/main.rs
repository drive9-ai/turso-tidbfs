use std::{
    io::{self, BufRead, Write},
    sync::Arc,
};
use turso_core::{
    Database, OpenOptions, SqliteDialect, StepResult,
    io::{IO, OpenFlags},
};
use turso_fs::{FsIo, HttpStore, SessionOptions};

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let first = args.next().ok_or("usage: turso-fs [--read-only] /remote/directory [database.db] < statements.sql\nSet TIDBFS_ENDPOINT and TIDBFS_TOKEN. One complete SQL batch per input line.")?;
    let readonly = first == "--read-only";
    let root = if readonly {
        args.next()
            .ok_or("--read-only requires a remote directory")?
    } else {
        first
    };
    let name = args.next().unwrap_or_else(|| "agent.db".into());
    if args.next().is_some() {
        return Err("too many arguments".into());
    }
    let store = Arc::new(HttpStore::new(
        &std::env::var("TIDBFS_ENDPOINT")?,
        std::env::var("TIDBFS_TOKEN")?,
        &root,
    )?);
    let io = Arc::new(if readonly {
        FsIo::snapshot(store.clone())?
    } else {
        store.prepare()?;
        FsIo::writer(store.clone(), SessionOptions::from_env()?)?
    });
    let flags = if readonly {
        OpenFlags::ReadOnly
    } else {
        OpenFlags::Create
    };
    let db = Database::open(
        io.clone(),
        &name,
        OpenOptions::new(Arc::new(SqliteDialect)).flags(flags),
    )?;
    let conn = db.connect()?;
    conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;")?;
    println!(
        "{}",
        serde_json::json!({"ready": true, "root": root, "database": name, "read_only": readonly})
    );
    io::stdout().flush()?;
    for line in io::stdin().lock().lines() {
        let sql = line?;
        if sql.trim().is_empty() {
            continue;
        }
        io.ensure_healthy()?;
        // Query output is intentionally explicit: prefix a single SELECT/PRAGMA
        // with '?' to return rows. Other lines can contain a complete SQL batch.
        let rows = if let Some(query) = sql.strip_prefix('?') {
            let mut rows = Vec::new();
            if let Some(mut stmt) = conn.query(query)? {
                loop {
                    match stmt.step()? {
                        StepResult::Row => rows.push(
                            stmt.row()
                                .ok_or("missing row")?
                                .get_values()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>(),
                        ),
                        StepResult::Done => break,
                        StepResult::IO | StepResult::Yield => io.step()?,
                        StepResult::Sleep { duration } => std::thread::sleep(duration),
                        StepResult::Busy | StepResult::Interrupt => {
                            return Err("query busy or interrupted".into());
                        }
                    }
                }
            }
            rows
        } else {
            conn.execute(sql)?;
            Vec::new()
        };
        io.ensure_healthy()?;
        println!("{}", serde_json::json!({"ok": true, "rows": rows}));
        io::stdout().flush()?;
    }
    conn.close()?;
    io.close()?;
    eprintln!("FS transfer metrics: {}", store.metrics());
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("turso-fs: {error}");
        std::process::exit(1);
    }
}
