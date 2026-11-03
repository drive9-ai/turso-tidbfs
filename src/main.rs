use std::{
    io::{self, BufRead, Write},
    sync::Arc,
};
use turso_core::{Database, SqliteDialect, StepResult, io::IO};
use turso_fs::{FsIo, HttpStore};

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().ok_or("usage: turso-fs /remote/directory [database.db] < statements.sql\nSet DRIVE9_BASE and DRIVE9_API_KEY. One complete SQL batch per input line.")?;
    let name = args.next().unwrap_or_else(|| "agent.db".into());
    if args.next().is_some() {
        return Err("too many arguments".into());
    }
    let store = Arc::new(HttpStore::new(
        &std::env::var("DRIVE9_BASE")?,
        std::env::var("DRIVE9_API_KEY")?,
        &root,
    )?);
    store.prepare()?;
    let io = Arc::new(FsIo::new(store.clone())?);
    let db = Database::open_file(io.clone(), &name, Arc::new(SqliteDialect))?;
    let conn = db.connect()?;
    conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;")?;
    println!(
        "{}",
        serde_json::json!({"ready": true, "root": root, "database": name})
    );
    io::stdout().flush()?;
    for line in io::stdin().lock().lines() {
        let sql = line?;
        if sql.trim().is_empty() {
            continue;
        }
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
        println!("{}", serde_json::json!({"ok": true, "rows": rows}));
        io::stdout().flush()?;
    }
    conn.close()?;
    eprintln!("FS transfer metrics: {}", store.metrics());
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("turso-fs: {error}");
        std::process::exit(1);
    }
}
