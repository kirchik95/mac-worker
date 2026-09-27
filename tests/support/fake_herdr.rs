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
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use serde_json::{Value, json};

/// Gate a reply until the test releases it. The server signals `entered`
/// when the request has been recorded, then waits. Drop releases waiters.
pub struct Hold {
    entered: Mutex<bool>,
    go: Mutex<bool>,
    entered_cv: Condvar,
    go_cv: Condvar,
}

impl Hold {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Mutex::new(false),
            go: Mutex::new(false),
            entered_cv: Condvar::new(),
            go_cv: Condvar::new(),
        })
    }

    /// `true` once a request has entered this gate.
    pub fn wait_entered(&self, timeout: Duration) -> bool {
        let guard = self.entered.lock().expect("hold entered lock");
        let (guard, _) = self
            .entered_cv
            .wait_timeout_while(guard, timeout, |entered| !*entered)
            .expect("hold entered wait");
        *guard
    }

    pub fn release(&self) {
        let mut go = self.go.lock().expect("hold go lock");
        *go = true;
        self.go_cv.notify_all();
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.release();
    }
}

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
    /// Signals the gate, waits until [`Hold::release`], then answers.
    Hold(Arc<Hold>, Value),
}

type RequestHook = Arc<dyn Fn(&Value) + Send + Sync>;

type Replies = Arc<Mutex<HashMap<String, VecDeque<Reply>>>>;

pub struct FakeHerdr {
    path: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    replies: Replies,
    closed_after_reply: Arc<Mutex<Vec<bool>>>,
    on_request: Arc<Mutex<Option<RequestHook>>>,
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
        let on_request = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        {
            let requests = Arc::clone(&requests);
            let replies = Arc::clone(&replies);
            let closed_after_reply = Arc::clone(&closed_after_reply);
            let on_request = Arc::clone(&on_request);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let requests = Arc::clone(&requests);
                            let replies = Arc::clone(&replies);
                            let closed_after_reply = Arc::clone(&closed_after_reply);
                            let on_request = Arc::clone(&on_request);
                            thread::spawn(move || {
                                serve_one(
                                    stream,
                                    &requests,
                                    &replies,
                                    &closed_after_reply,
                                    &on_request,
                                )
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
            on_request,
            stop,
        }
    }

    /// Called after a request is recorded and before its reply is sent.
    /// Tests advance a manual clock here so a budget expires without sleeping.
    pub fn on_request(&self, hook: impl Fn(&Value) + Send + Sync + 'static) {
        *self.on_request.lock().expect("on_request lock") = Some(Arc::new(hook));
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
    on_request: &Mutex<Option<RequestHook>>,
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
    if let Some(hook) = on_request.lock().expect("on_request lock").clone() {
        hook(&request);
    }
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
        Reply::Hold(hold, value) => {
            {
                let mut entered = hold.entered.lock().expect("hold entered lock");
                *entered = true;
                hold.entered_cv.notify_all();
            }
            let go = hold.go.lock().expect("hold go lock");
            let _go = hold.go_cv.wait_while(go, |go| !*go).expect("hold go wait");
            Some(json!({ "id": id, "result": value }))
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
