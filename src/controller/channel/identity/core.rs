//! Existing-only identity reads and an interruptible authenticated stdio exchange.
use crate::{
    controller::{ControllerReadReply, ControllerRequest, decode_frame, encode_json_frame},
    error::{ProcessError, WorkerError},
    inputs::RelativePath,
    job::ClientId,
    paths::PathLayout,
    process::{ProcessRequest, ProcessRunner},
    rooted_fs::RootedDir,
};
use serde_json::Value;
use std::{io, time::Duration};

fn invalid_selector() -> WorkerError {
    WorkerError::Protocol("CONTROLLER_TRANSPORT: invalid socket identity selector".into())
}

pub(crate) fn selector_route(request: &ControllerRequest) -> Result<Option<String>, WorkerError> {
    if request.command() != "task.list" || request.body().get("controller_socket").is_none() {
        return Ok(None);
    }
    if request
        .body()
        .as_object()
        .is_none_or(|body| body.len() != 1)
    {
        return Err(invalid_selector());
    }
    let selector = request.body()["controller_socket"]
        .as_object()
        .ok_or_else(invalid_selector)?;
    let route = selector
        .get("route_sha256")
        .and_then(Value::as_str)
        .ok_or_else(invalid_selector)?;
    if selector.len() != 2
        || selector.get("op").and_then(Value::as_str) != Some("identity")
        || route.len() != 64
        || !route
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid_selector());
    }
    Ok(Some(route.to_owned()))
}

fn existing_private_root(path: &std::path::Path) -> io::Result<Option<RootedDir>> {
    let root = match RootedDir::open_anchored_absolute(path) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let metadata = root.root_metadata()?;
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o7777 != 0o700 {
        return Err(io::Error::from_raw_os_error(libc::EACCES));
    }
    Ok(Some(root))
}

pub(crate) fn existing_client_id(paths: &PathLayout) -> io::Result<Option<ClientId>> {
    let Some(root) = existing_private_root(&paths.state)? else {
        return Ok(None);
    };
    let bytes = match root.read_private_regular("client-id", 33) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if bytes.len() != 33 || bytes[32] != b'\n' {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    let id = std::str::from_utf8(&bytes[..32])
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?
        .parse()
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    Ok(Some(id))
}

pub(crate) fn existing_service_record(paths: &PathLayout) -> io::Result<Option<Vec<u8>>> {
    let Some(root) = existing_private_root(&paths.controller_state_root())? else {
        return Ok(None);
    };
    let rpc = match root.open_child_directory(
        &RelativePath::parse(b"rpc").map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?,
        false,
    ) {
        Ok(rpc) => rpc,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let binding = match rpc.private_entry_identity("service.json") {
        Ok(binding) => binding,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let bytes = rpc.read_private_regular("service.json", 8192)?;
    if rpc.private_entry_identity("service.json")? != binding {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    Ok(Some(bytes))
}

pub(crate) fn decode_unique(bytes: &[u8]) -> Result<Value, WorkerError> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value =
        crate::task::deserialize_unique_json(&mut decoder).map_err(|_| invalid_selector())?;
    decoder.end().map_err(|_| invalid_selector())?;
    Ok(value)
}

pub(crate) fn raw_exchange(
    raw: &dyn ProcessRunner,
    transport: &ProcessRequest,
    query: &ControllerRequest,
    remaining: Duration,
    should_stop: &dyn Fn() -> bool,
) -> Result<Value, WorkerError> {
    if should_stop() {
        return Err(ProcessError::Cancelled.into());
    }
    if remaining.is_zero() {
        return Err(ProcessError::DeadlineExceeded {
            deadline: remaining,
        }
        .into());
    }
    if selector_route(query)?.is_none() {
        return Err(invalid_selector());
    }
    let mut request = transport.clone();
    request.policy.deadline = request
        .policy
        .deadline
        .min(remaining)
        .min(Duration::from_secs(5));
    request.policy.stdout_limit = request.policy.stdout_limit.min(8196);
    request.stdin = Some(encode_json_frame(
        &serde_json::json!({"protocol_version":query.protocol_version(),"request_id":query.request_id(),"command":query.command(),"payload_sha256":query.payload_sha256(),"body":query.body()}),
    )?);
    let result = raw.run_interruptible(&request, should_stop)?;
    if should_stop() {
        return Err(ProcessError::Cancelled.into());
    }
    if !result.status.success() || result.stdout.len() > 8196 {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: socket identity is unavailable".into(),
        ));
    }
    let payload = decode_frame(&result.stdout)?;
    let reply: ControllerReadReply<Value> =
        serde_json::from_value(decode_unique(payload)?).map_err(|_| invalid_selector())?;
    reply.verify_envelope(query)?;
    Ok(reply.into_result())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::{ControllerReadReply, encode_json_frame, parse_request},
        error::ProcessError,
        process::{ProcessPolicy, ProcessResult},
        rooted_fs::RootedDir,
    };
    use std::{
        cell::Cell,
        fs,
        os::unix::fs::PermissionsExt,
        os::unix::process::ExitStatusExt,
        rc::Rc,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };
    fn paths(temp: &tempfile::TempDir) -> PathLayout {
        let p = temp.path();
        PathLayout {
            config: p.join("config"),
            state: p.join("state"),
            cache: p.join("cache"),
            data: p.join("data"),
        }
    }
    fn query(body: Value) -> ControllerRequest {
        parse_request(&serde_json::to_vec(&serde_json::json!({"protocol_version":7,"request_id":"11111111111141118111111111111111","command":"task.list","body":body})).unwrap()).unwrap()
    }
    fn identity_query() -> ControllerRequest {
        query(
            serde_json::json!({"controller_socket":{"op":"identity","route_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}),
        )
    }
    fn transport() -> ProcessRequest {
        ProcessRequest {
            program: "ssh".into(),
            args: vec![
                "-F".into(),
                "/original/config".into(),
                "-S".into(),
                "/literal/master".into(),
                "controller".into(),
            ],
            environment: vec![("HOME".into(), "/original/home".into())],
            environment_remove: vec![],
            stdin: None,
            policy: ProcessPolicy {
                stdout_limit: 1024 * 1024 + 4,
                stderr_limit: 256 * 1024,
                deadline: Duration::from_secs(30),
            },
            isolate_parent_environment: false,
        }
    }
    struct ReplyRunner {
        calls: Mutex<Vec<ProcessRequest>>,
        reply: Mutex<Option<ProcessResult>>,
    }
    impl ProcessRunner for ReplyRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(self.reply.lock().unwrap().take().unwrap())
        }
    }
    fn runner(query: &ControllerRequest, result: Value) -> ReplyRunner {
        ReplyRunner {
            calls: Mutex::new(vec![]),
            reply: Mutex::new(Some(ProcessResult {
                status: std::process::ExitStatus::from_raw(0),
                stdout: encode_json_frame(&ControllerReadReply::from_request(query, result))
                    .unwrap(),
                stderr: vec![],
            })),
        }
    }

    #[test]
    fn selector_grammar_rejects_mixed_and_unknown_fields_without_dispatch() {
        assert_eq!(
            selector_route(&identity_query()).unwrap(),
            Some("a".repeat(64))
        );
        assert_eq!(
            selector_route(&query(serde_json::json!({"controller_health":true}))).unwrap(),
            None
        );
        for body in [
            serde_json::json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64)},"controller_health":true}),
            serde_json::json!({"controller_socket":{"op":"other","route_sha256":"a".repeat(64)}}),
            serde_json::json!({"controller_socket":{"op":"identity","route_sha256":"A".repeat(64)}}),
            serde_json::json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64),"unknown":true}}),
            serde_json::json!({"controller_socket":true}),
        ] {
            assert!(selector_route(&query(body)).is_err());
        }
    }

    #[test]
    fn existing_client_id_comes_only_from_state_and_missing_never_creates() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        assert_eq!(existing_client_id(&paths).unwrap(), None);
        assert!(!paths.state.exists());
        assert!(!paths.controller_state_root().exists());
        let state = RootedDir::create(&paths.state).unwrap();
        state
            .write_private_atomic_no_replace("client-id", b"11111111111141118111111111111111\n")
            .unwrap();
        let controller = RootedDir::create(&paths.controller_state_root()).unwrap();
        controller
            .write_private_atomic_no_replace("client-id", b"22222222222242228222222222222222\n")
            .unwrap();
        assert_eq!(
            existing_client_id(&paths).unwrap().unwrap().to_string(),
            "11111111111141118111111111111111"
        );
        fs::remove_file(paths.state.join("client-id")).unwrap();
        assert_eq!(existing_client_id(&paths).unwrap(), None);
        assert!(state.list_names().unwrap().is_empty());
        assert!(!paths.controller_state_root().join("requests").exists());
        assert!(!paths.controller_state_root().join("active").exists());
    }

    #[test]
    fn existing_record_reader_is_private_bounded_and_independent_of_journal() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        assert_eq!(existing_service_record(&paths).unwrap(), None);
        assert!(!paths.controller_state_root().exists());
        let controller = RootedDir::create(&paths.controller_state_root()).unwrap();
        let rpc = controller.create_new_child_directory("rpc").unwrap();
        rpc.write_private_atomic_no_replace("service.json", br#"{"journal_id":null}"#)
            .unwrap();
        assert_eq!(
            existing_service_record(&paths).unwrap().unwrap(),
            br#"{"journal_id":null}"#
        );
        assert!(!paths.controller_state_root().join("events").exists());
        assert!(!paths.state.exists());
        assert!(!paths.controller_state_root().join("requests").exists());
        fs::set_permissions(
            rpc.path().join("service.json"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(existing_service_record(&paths).is_err());
        fs::set_permissions(
            rpc.path().join("service.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        fs::write(rpc.path().join("service.json"), vec![b' '; 8193]).unwrap();
        assert!(existing_service_record(&paths).is_err());
    }

    #[test]
    fn raw_bootstrap_preserves_captured_config_master_and_exact_envelope() {
        let query = identity_query();
        let result = serde_json::json!({"available":false});
        let raw = runner(&query, result.clone());
        let request = transport();
        assert_eq!(
            raw_exchange(&raw, &request, &query, Duration::from_secs(3), &|| false).unwrap(),
            result
        );
        let calls = raw.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].args, request.args);
        assert_eq!(calls[0].environment, request.environment);
        let sent = crate::controller::decode_request(calls[0].stdin.as_ref().unwrap()).unwrap();
        assert_eq!(sent.request_id(), "11111111111141118111111111111111");
        assert_eq!(sent.command(), "task.list");
        assert_eq!(sent.body(), query.body());
        assert_eq!(sent.payload_sha256(), query.payload_sha256());
        assert!(calls[0].policy.deadline <= Duration::from_secs(3));
    }

    #[test]
    fn cancelled_or_expired_bootstrap_has_no_transmission() {
        let query = identity_query();
        let raw = runner(&query, Value::Null);
        assert!(matches!(
            raw_exchange(&raw, &transport(), &query, Duration::from_secs(30), &|| {
                true
            }),
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        assert!(raw_exchange(&raw, &transport(), &query, Duration::ZERO, &|| false).is_err());
        assert!(raw.calls.lock().unwrap().is_empty());
    }

    struct GatedRunner {
        entered: std::sync::mpsc::Sender<()>,
        calls: Mutex<usize>,
    }
    impl ProcessRunner for GatedRunner {
        fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("must use live interruptible runner")
        }
        fn run_interruptible(
            &self,
            _request: &ProcessRequest,
            stop: &dyn Fn() -> bool,
        ) -> Result<ProcessResult, WorkerError> {
            *self.calls.lock().unwrap() += 1;
            self.entered.send(()).unwrap();
            let guard = std::time::Instant::now();
            loop {
                if stop() {
                    return Err(ProcessError::Cancelled.into());
                }
                if guard.elapsed() >= Duration::from_secs(30) {
                    return Err(ProcessError::DeadlineExceeded {
                        deadline: Duration::from_secs(30),
                    }
                    .into());
                }
                std::thread::yield_now();
            }
        }
    }

    #[test]
    fn borrowed_non_send_stop_interrupts_blocked_bootstrap_after_entry() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let raw = GatedRunner {
            entered: entered_tx,
            calls: Mutex::new(0),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let calls = Rc::new(Cell::new(0));
        let canceller = std::thread::spawn(move || {
            entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            signal.store(true, Ordering::Release);
        });
        let should_stop = || {
            calls.set(calls.get() + 1);
            stop.load(Ordering::Acquire)
        };
        let result = raw_exchange(
            &raw,
            &transport(),
            &identity_query(),
            Duration::from_secs(30),
            &should_stop,
        );
        assert!(matches!(
            result,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        canceller.join().unwrap();
        assert_eq!(*raw.calls.lock().unwrap(), 1);
        assert!(calls.get() > 1);
    }

    #[test]
    fn raw_identity_rejects_wrong_envelope_and_trailing_or_oversize_frames() {
        let query = identity_query();
        let wrong=parse_request(&serde_json::to_vec(&serde_json::json!({"protocol_version":7,"request_id":"22222222222242228222222222222222","command":"task.list","body":query.body()})).unwrap()).unwrap();
        let raw = runner(&wrong, Value::Null);
        assert!(
            raw_exchange(&raw, &transport(), &query, Duration::from_secs(30), &|| {
                false
            })
            .is_err()
        );
        for mode in 0..2 {
            let raw = runner(&query, Value::Null);
            let mut reply = raw.reply.lock().unwrap();
            if mode == 0 {
                reply.as_mut().unwrap().stdout.push(0);
            } else {
                reply.as_mut().unwrap().stdout = vec![0; 8197];
            }
            drop(reply);
            assert!(
                raw_exchange(&raw, &transport(), &query, Duration::from_secs(30), &|| {
                    false
                })
                .is_err()
            );
        }
    }
}
