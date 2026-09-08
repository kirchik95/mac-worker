#![allow(dead_code)]
//! A scripted herdr server for tests: listens on a Unix socket, answers
//! one JSON line per connection from a queue of canned replies, and records
//! every request so tests can assert exactly what crossed the socket.

use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use serde_json::{Value, json};

/// How the fake answers the next request of a method.
pub enum Reply {
    /// `{"id": <request id>, "result": value}`.
    Result(Value),
    /// `{"id": <request id>, "error": {"code", "message"}}`.
    Error { code: String, message: String },
    /// Sent verbatim, id and all; for answers that must not match.
    Raw(Value),
    /// Waits, then answers with the value.
    Stall(Duration, Value),
    /// Never answers; holds the connection open for ten seconds.
    Silence,
}

type Replies = Arc<Mutex<HashMap<String, VecDeque<Reply>>>>;

pub struct FakeHerdr {
    path: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    replies: Replies,
    closed_after_reply: Arc<Mutex<Vec<bool>>>,
    stop: Arc<AtomicBool>,
}

impl FakeHerdr {
    /// Listen at the default session socket under `home`.
    pub fn start_in_home(home: &Path) -> Self {
        let dir = home.join(".config/herdr");
        fs::create_dir_all(&dir).expect("herdr config dir");
        Self::start_at(dir.join("herdr.sock"))
    }

    pub fn start_at(path: PathBuf) -> Self {
        let _ = fs::remove_file(&path);
        let listener = UnixListener::bind(&path).expect("bind fake herdr socket");
        listener
            .set_nonblocking(true)
            .expect("nonblocking fake herdr listener");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let replies: Replies = Arc::new(Mutex::new(HashMap::new()));
        let closed_after_reply = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        {
            let requests = Arc::clone(&requests);
            let replies = Arc::clone(&replies);
            let closed_after_reply = Arc::clone(&closed_after_reply);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let requests = Arc::clone(&requests);
                            let replies = Arc::clone(&replies);
                            let closed_after_reply = Arc::clone(&closed_after_reply);
                            thread::spawn(move || {
                                serve_one(stream, &requests, &replies, &closed_after_reply)
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
            });
        }
        Self {
            path,
            requests,
            replies,
            closed_after_reply,
            stop,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Queue the answer to the next request of `method`.  Methods with no
    /// queued reply are answered with `{"type": "ok"}`.
    pub fn reply(&self, method: &str, reply: Reply) {
        self.replies
            .lock()
            .unwrap()
            .entry(method.to_owned())
            .or_default()
            .push_back(reply);
    }

    /// Every request line received so far, as parsed JSON, in order.
    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }

    /// Requests of one method, in order.
    pub fn requests_for(&self, method: &str) -> Vec<Value> {
        self.requests()
            .into_iter()
            .filter(|request| request.get("method").and_then(Value::as_str) == Some(method))
            .collect()
    }

    /// For each answered connection, whether the client closed it right
    /// after reading the answer.
    pub fn connections_closed_after_reply(&self) -> Vec<bool> {
        self.closed_after_reply.lock().unwrap().clone()
    }
}

impl Drop for FakeHerdr {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = fs::remove_file(&self.path);
    }
}

fn serve_one(
    mut stream: UnixStream,
    requests: &Mutex<Vec<Value>>,
    replies: &Replies,
    closed_after_reply: &Mutex<Vec<bool>>,
) {
    // A socket accepted from a non-blocking listener inherits that mode on
    // macOS; the per-connection reads below must block up to their timeout.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let request: Value = serde_json::from_str(line.trim_end()).unwrap_or(Value::Null);
    requests.lock().unwrap().push(request.clone());
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let reply = replies
        .lock()
        .unwrap()
        .get_mut(&method)
        .and_then(VecDeque::pop_front)
        .unwrap_or(Reply::Result(json!({ "type": "ok" })));
    let answer = match reply {
        Reply::Result(value) => Some(json!({ "id": id, "result": value })),
        Reply::Error { code, message } => {
            Some(json!({ "id": id, "error": { "code": code, "message": message } }))
        }
        Reply::Raw(value) => Some(value),
        Reply::Stall(delay, value) => {
            thread::sleep(delay);
            Some(json!({ "id": id, "result": value }))
        }
        Reply::Silence => {
            thread::sleep(Duration::from_secs(10));
            None
        }
    };
    if let Some(answer) = answer {
        let _ = writeln!(stream, "{answer}");
        let _ = stream.flush();
        let mut byte = [0u8; 1];
        let closed = matches!(stream.read(&mut byte), Ok(0));
        closed_after_reply.lock().unwrap().push(closed);
    }
}
