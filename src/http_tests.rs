use super::*;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    thread,
};
type Handler = Box<dyn Fn(&str, Value) -> (u16, Vec<u8>) + Send>;
fn server(handlers: Vec<Handler>) -> (HttpStore, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        for handler in handlers {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => panic!("test HTTP accept failed: {e}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let parsed = if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body).unwrap()
            };
            let (code, body) = handler(&first, parsed);
            write!(stream,"HTTP/1.1 {code} Test\r\nContent-Length: {}\r\nX-Dat9-Revision: 1\r\nConnection: close\r\n\r\n",body.len()).unwrap();
            stream.write_all(&body).unwrap();
        }
    });
    (
        HttpStore::new(
            &format!("http://{address}"),
            "local-http-fixture".into(),
            "/test",
        )
        .unwrap(),
        worker,
    )
}
fn blocks() -> BTreeMap<String, Vec<u8>> {
    [
        (digest(b"first"), b"first".to_vec()),
        (digest(b"second"), b"second".to_vec()),
    ]
    .into_iter()
    .collect()
}
fn ack(body: &Value, statuses: &[(u16, u64)]) -> Vec<u8> {
    let results: Vec<_> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .zip(statuses)
        .map(|(item, (status, rev))| json!({"path":item["path"],"status":status,"revision":rev}))
        .collect();
    serde_json::to_vec(&json!({"results":results})).unwrap()
}
#[test]
fn http_batch_upload_has_no_existence_reads() {
    let (store, worker) = server(vec![Box::new(|request, body| {
        assert!(request.starts_with("POST /v1/fs:batch-write "));
        assert_eq!(body["items"].as_array().unwrap().len(), 2);
        for item in body["items"].as_array().unwrap() {
            assert_eq!(item["expected_revision"], 0);
            let bytes = BASE64.decode(item["data"].as_str().unwrap()).unwrap();
            assert_eq!(item["path"], format!("/test/blocks/{}", digest(&bytes)));
        }
        (200, ack(&body, &[(200, 1), (200, 1)]))
    })]);
    store.put_blocks(&blocks()).unwrap();
    assert_eq!(store.metrics_snapshot().batch_requests, 1);
    assert_eq!(store.metrics_snapshot().gets, 0);
    worker.join().unwrap();
}
#[test]
fn http_existing_block_conflict_requires_matching_content() {
    for valid in [true, false] {
        let (store, worker) = server(vec![
            Box::new(|_, body| (200, ack(&body, &[(409, 0)]))),
            Box::new(move |request, _| {
                assert!(request.starts_with("GET /v1/fs/test/blocks/"));
                (
                    200,
                    if valid {
                        b"first".to_vec()
                    } else {
                        b"wrong".to_vec()
                    },
                )
            }),
        ]);
        assert_eq!(
            store
                .put_blocks(
                    &[(digest(b"first"), b"first".to_vec())]
                        .into_iter()
                        .collect()
                )
                .is_ok(),
            valid
        );
        worker.join().unwrap();
    }
}
#[test]
fn http_partial_batch_failure_never_publishes_manifest() {
    let (store, worker) = server(vec![
        Box::new(|request, _| {
            assert!(request.starts_with("GET /v1/fs/test/manifest.json "));
            (404, b"{}".to_vec())
        }),
        Box::new(|request, body| {
            assert!(request.starts_with("POST /v1/fs:batch-write "));
            (200, ack(&body, &[(200, 1), (500, 2)]))
        }),
    ]);
    let store = Arc::new(store);
    let io = FsIo::unlocked_for_test(store.clone()).unwrap();
    let file = io.open_file("agent.db", OpenFlags::Create, false).unwrap();
    let mut bytes = vec![1; BLOCK_SIZE];
    bytes.extend(vec![2; BLOCK_SIZE]);
    let completion = file
        .pwrite(
            0,
            Arc::new(Buffer::new(bytes)),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    assert!(completion.finished());
    assert!(
        file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .is_err()
    );
    assert!(file.size().is_err());
    assert_eq!(
        store.metrics_snapshot().puts,
        0,
        "failed batch must never publish a manifest"
    );
    worker.join().unwrap();
}
#[test]
fn http_malformed_batch_acknowledgements_are_rejected() {
    for kind in ["missing", "path", "revision"] {
        let (store, worker) = server(vec![Box::new(move |_, body| {
            let bytes = ack(&body, &[(200, 1), (200, 1)]);
            let mut response: Value = serde_json::from_slice(&bytes).unwrap();
            match kind {
                "missing" => {
                    response["results"].as_array_mut().unwrap().pop();
                }
                "path" => response["results"][1]["path"] = response["results"][0]["path"].clone(),
                _ => response["results"][0]["revision"] = json!(0),
            }
            (200, serde_json::to_vec(&response).unwrap())
        })]);
        assert!(store.put_blocks(&blocks()).is_err());
        worker.join().unwrap();
    }
}

#[test]
fn http_block_batches_respect_item_limits() {
    let (store, worker) = server(vec![
        Box::new(|_, body| {
            assert_eq!(body["items"].as_array().unwrap().len(), 128);
            (200, ack(&body, &vec![(200, 1); 128]))
        }),
        Box::new(|_, body| {
            assert_eq!(body["items"].as_array().unwrap().len(), 1);
            (200, ack(&body, &[(200, 1)]))
        }),
    ]);
    let blocks = (0u32..129)
        .map(|i| {
            let data = i.to_be_bytes().to_vec();
            (digest(&data), data)
        })
        .collect();
    store.put_blocks(&blocks).unwrap();
    assert_eq!(store.metrics_snapshot().batch_requests, 2);
    worker.join().unwrap();
}

#[test]
fn http_writer_claim_and_release_share_the_manifest() {
    let (store, worker) = server(vec![
        Box::new(|request, _| {
            assert!(request.starts_with("GET /v1/fs/test/manifest.json "));
            (404, b"{}".to_vec())
        }),
        Box::new(|request, body| {
            assert!(request.starts_with("PUT /v1/fs/test/manifest.json "));
            assert_eq!(body["format"], MANIFEST_FORMAT);
            assert_eq!(body["lease"]["ttl_ms"], 30000);
            assert_eq!(body["lease"]["sequence"], 0);
            assert_eq!(body["lease"]["owner"].as_str().unwrap().len(), 64);
            (200, br#"{"revision":1}"#.to_vec())
        }),
        Box::new(|request, body| {
            assert!(request.starts_with("PUT /v1/fs/test/manifest.json "));
            assert!(
                body.get("lease").is_none(),
                "release must clear lease in the same object"
            );
            (200, br#"{"revision":2}"#.to_vec())
        }),
    ]);
    let io = FsIo::new(Arc::new(store)).unwrap();
    io.close().unwrap();
    assert!(io.ensure_healthy().is_err());
    worker.join().unwrap();
}

#[test]
fn http_manifest_conflict_is_distinct_from_uncertain_failure() {
    let (store, worker) = server(vec![Box::new(|request, _| {
        assert!(request.starts_with("PUT /v1/fs/test/manifest.json "));
        (409, b"{}".to_vec())
    })]);
    assert!(matches!(
        store.put_manifest(b"{}", 1, Instant::now() + Duration::from_secs(5)),
        Err(LimboError::LockingError(_))
    ));
    worker.join().unwrap();
}
