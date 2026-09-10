//! Shared submit/runner admission: bound cache before SSH, one 3-wide pipeline.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    agent_facts::FACTS_TTL,
    client_state::{ClientStateStore, OBSERVATION_TTL_MILLIS},
    config::{WorkerEntry, valid_ssh_destination},
    error::WorkerError,
    job::AdmissionObservation,
    lease::MAX_HOST_SLOTS,
    process::ProcessRunner,
    protocol::{HealthStatus, WorkerHealth},
    scheduler::{CandidateObservation, CandidateSlot, WorkerPreference},
    scheduler_adapter::SchedulerProbeAdapter,
    transport::{SshTransport, WorkersService},
};

const ADMISSION_ROUND_BUDGET: Duration = Duration::from_secs(60);
const PROBE_CAP: Duration = Duration::from_secs(15);
const REFRESH_CAP: Duration = Duration::from_secs(30);

enum Pipeline {
    Observation {
        record: AdmissionObservation,
        probed_at: Option<Instant>,
    },
    Unavailable,
    Failed(WorkerError),
}

pub(crate) fn observe_admission(
    runner: &dyn ProcessRunner,
    config: &crate::config::Config,
    client_state: &ClientStateStore,
    preference: &WorkerPreference,
) -> Result<Vec<CandidateObservation>, WorkerError> {
    let selected = config
        .workers
        .iter()
        .filter(|worker| match preference {
            WorkerPreference::Automatic => true,
            WorkerPreference::Pinned { worker: pinned } => worker.name == *pinned,
        })
        .collect::<Vec<_>>();
    observe_workers(runner, config, client_state, &selected)
}

pub(crate) fn observe_workers(
    runner: &dyn ProcessRunner,
    config: &crate::config::Config,
    client_state: &ClientStateStore,
    workers: &[&WorkerEntry],
) -> Result<Vec<CandidateObservation>, WorkerError> {
    for worker in workers {
        require_admission_worker(worker)?;
    }
    if workers.is_empty() {
        return Ok(Vec::new());
    }
    let round_deadline = Instant::now() + ADMISSION_ROUND_BUDGET;
    let now = now_millis()?;
    let mut slots = (0..workers.len()).map(|_| None).collect::<Vec<_>>();
    let mut miss_indices = Vec::new();
    let mut hit_indices = Vec::new();
    for (index, worker) in workers.iter().enumerate() {
        if let Some(observation) = client_state.peek_admission_observation(&worker.name)?
            && reusable_for_admission(&observation, worker, now)
        {
            hit_indices.push(index);
            slots[index] = Some(Pipeline::Observation {
                record: observation,
                probed_at: None,
            });
        } else {
            miss_indices.push(index);
        }
    }

    if let Some(error) = run_pipelines(
        runner,
        config,
        client_state,
        workers,
        &miss_indices,
        round_deadline,
        &mut slots,
    )? {
        return Err(error);
    }

    if !miss_indices.is_empty() {
        let mut reprobe = Vec::new();
        let now = now_millis()?;
        for index in hit_indices {
            let worker = workers[index];
            match client_state.peek_admission_observation(&worker.name)? {
                Some(current) if reusable_for_admission(&current, worker, now) => {
                    slots[index] = Some(Pipeline::Observation {
                        record: current,
                        probed_at: None,
                    });
                }
                _ if remaining_until(round_deadline).is_zero() => {
                    slots[index] = Some(Pipeline::Unavailable);
                }
                _ => reprobe.push(index),
            }
        }
        if let Some(error) = run_pipelines(
            runner,
            config,
            client_state,
            workers,
            &reprobe,
            round_deadline,
            &mut slots,
        )? {
            return Err(error);
        }
    }

    let now = now_millis()?;
    slots
        .into_iter()
        .zip(workers)
        .map(|(slot, worker)| match slot {
            Some(Pipeline::Observation {
                record: observation,
                probed_at: Some(probed_at),
            }) => candidate_at_return(&observation, now, Some(probed_at)),
            Some(Pipeline::Observation {
                record: _,
                probed_at: None,
            }) => match client_state.peek_admission_observation(&worker.name)? {
                Some(current) if reusable_for_admission(&current, worker, now) => {
                    candidate_at_return(&current, now, None)
                }
                _ => unavailable_candidate(&worker.name),
            },
            Some(Pipeline::Failed(error)) => Err(error),
            Some(Pipeline::Unavailable) | None => unavailable_candidate(&worker.name),
        })
        .collect()
}

fn run_pipelines(
    runner: &dyn ProcessRunner,
    config: &crate::config::Config,
    client_state: &ClientStateStore,
    workers: &[&WorkerEntry],
    indices: &[usize],
    round_deadline: Instant,
    slots: &mut [Option<Pipeline>],
) -> Result<Option<WorkerError>, WorkerError> {
    if indices.is_empty() {
        return Ok(None);
    }
    let budget = remaining_until(round_deadline);
    if budget.is_zero() {
        for &index in indices {
            slots[index] = Some(Pipeline::Unavailable);
        }
        return Ok(None);
    }
    let miss_workers = indices
        .iter()
        .map(|&index| workers[index].clone())
        .collect::<Vec<_>>();
    let outcomes = WorkersService::new(SshTransport::new(runner)).map_workers_budgeted(
        &miss_workers,
        budget,
        |worker, remaining| {
            worker_pipeline(
                runner,
                config,
                client_state,
                worker,
                remaining,
                round_deadline,
            )
        },
    );
    let mut caller_error = None;
    for (&index, outcome) in indices.iter().zip(outcomes) {
        match outcome {
            Some(Pipeline::Failed(error)) => {
                if caller_error.is_none() {
                    caller_error = Some(error);
                }
            }
            Some(outcome) => slots[index] = Some(outcome),
            None => slots[index] = Some(Pipeline::Unavailable),
        }
    }
    Ok(caller_error)
}

fn worker_pipeline(
    runner: &dyn ProcessRunner,
    config: &crate::config::Config,
    client_state: &ClientStateStore,
    worker: &WorkerEntry,
    remaining: Duration,
    round_deadline: Instant,
) -> Pipeline {
    match worker_pipeline_inner(
        runner,
        config,
        client_state,
        worker,
        remaining,
        round_deadline,
    ) {
        Ok(Some((observation, probed_at))) => Pipeline::Observation {
            record: observation,
            probed_at,
        },
        Ok(None) => Pipeline::Unavailable,
        Err(error) if is_caller_error(&error) => Pipeline::Failed(error),
        Err(_) => Pipeline::Unavailable,
    }
}

fn worker_pipeline_inner(
    runner: &dyn ProcessRunner,
    config: &crate::config::Config,
    client_state: &ClientStateStore,
    worker: &WorkerEntry,
    remaining: Duration,
    round_deadline: Instant,
) -> Result<Option<(AdmissionObservation, Option<Instant>)>, WorkerError> {
    if remaining.is_zero() {
        return Ok(None);
    }
    let Some(_refresh) = client_state.acquire_observation_refresh(&worker.name, round_deadline)?
    else {
        return Ok(None);
    };
    let now = now_millis()?;
    let expected = client_state.peek_admission_observation(&worker.name)?;
    if let Some(observation) = expected.as_ref()
        && reusable_for_admission(observation, worker, now)
    {
        return Ok(Some((observation.clone(), None)));
    }
    if remaining_until(round_deadline).is_zero() {
        return Ok(None);
    }
    let probed = catch_unwind(AssertUnwindSafe(|| {
        probe_bound_observation(runner, config, worker, round_deadline)
    }));
    let (observation, probe_started) = match probed {
        Ok(Ok(Some(sampled))) => sampled,
        Ok(Ok(None)) => return Ok(None),
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            if let Ok(negative) = negative_bound_observation(worker) {
                let _ = client_state.commit_admission_observation(
                    &worker.name,
                    expected.as_ref(),
                    negative.clone(),
                );
                return Ok(Some((negative, Some(Instant::now()))));
            }
            return Ok(None);
        }
    };
    let winner = match client_state.commit_admission_observation(
        &worker.name,
        expected.as_ref(),
        observation.clone(),
    ) {
        Ok(winner) => winner,
        Err(WorkerError::Queue {
            code: "OBSERVATION_REPLACED",
            ..
        }) => return Ok(None),
        Err(error) => return Err(error),
    };
    rank_committed(worker, &observation, winner, probe_started)
}

fn probe_bound_observation(
    runner: &dyn ProcessRunner,
    config: &crate::config::Config,
    worker: &WorkerEntry,
    deadline: Instant,
) -> Result<Option<(AdmissionObservation, Instant)>, WorkerError> {
    let transport = SshTransport::new(runner);
    let one = single_worker_config(config, worker);
    let probe_budget = request_budget(deadline, PROBE_CAP);
    if probe_budget.is_zero() {
        return Ok(None);
    }
    let (mut health, mut probe_started, mut started_millis) =
        probe_once(&transport, worker, probe_budget)?;
    if health.status == HealthStatus::Ready && facts_need_refresh(&health, probe_started.elapsed())
    {
        let refresh_budget = request_budget(deadline, REFRESH_CAP);
        if refresh_budget.is_zero() {
            return Ok(None);
        }
        match transport.refresh_facts_with_deadline(worker, refresh_budget, false) {
            Ok(()) => {
                let reprobe_budget = request_budget(deadline, PROBE_CAP);
                if reprobe_budget.is_zero() {
                    return Ok(None);
                }
                let refreshed = probe_once(&transport, worker, reprobe_budget)?;
                health = refreshed.0;
                probe_started = refreshed.1;
                started_millis = refreshed.2;
            }
            Err(
                error @ WorkerError::Transport {
                    code: "REFRESH_FACTS_FAILED",
                    ..
                },
            ) => {
                health.status = HealthStatus::Unavailable;
                health.error_code = Some(error.public_code());
                health.error_message = Some(error.public_message());
            }
            Err(error) => return Err(error),
        }
    }
    if health.status == HealthStatus::Ready
        && facts_need_refresh(&health, probe_started.elapsed())
        && let Some(probe) = health.probe.as_mut()
    {
        probe.agent_facts = None;
    }
    let facts_age = health
        .probe
        .as_ref()
        .and_then(|probe| probe.agent_facts.as_ref().and(probe.facts_age_millis));
    Ok(Some((
        admission_from_health(&one, &health, started_millis)?.with_local_binding(
            worker.ssh.clone(),
            worker.remote_binary.clone(),
            worker.capabilities.clone(),
            worker.slots,
            facts_age,
            started_millis,
        ),
        probe_started,
    )))
}

fn probe_once(
    transport: &SshTransport<&dyn ProcessRunner>,
    worker: &WorkerEntry,
    deadline: Duration,
) -> Result<(WorkerHealth, Instant, u64), WorkerError> {
    let started_millis = now_millis()?;
    let started = Instant::now();
    let health = transport.probe_with_deadline(worker, deadline);
    Ok((health, started, started_millis))
}

fn facts_need_refresh(health: &WorkerHealth, elapsed: Duration) -> bool {
    if health.status != HealthStatus::Ready {
        return false;
    }
    let Some(probe) = health.probe.as_ref() else {
        return true;
    };
    if probe.agent_facts.is_none() {
        return true;
    }
    let Some(age) = probe.facts_age_millis else {
        return true;
    };
    age.saturating_add(duration_millis(elapsed)) > FACTS_TTL
}

fn reusable_for_admission(
    observation: &AdmissionObservation,
    worker: &WorkerEntry,
    now_millis: u64,
) -> bool {
    if observation.worker_name() != worker.name {
        return false;
    }
    if !observation.binding_complete() || !observation.matches_worker(worker) {
        return false;
    }
    let Some(started) = observation.final_probe_started_at_millis() else {
        return false;
    };
    if started > now_millis || observation.observed_at_millis() > now_millis {
        return false;
    }
    let probe_age = now_millis - started;
    let observed_age = now_millis - observation.observed_at_millis();
    if probe_age > OBSERVATION_TTL_MILLIS || observed_age > OBSERVATION_TTL_MILLIS {
        return false;
    }
    if !observation.ready() {
        return true;
    }
    let Some(facts_age) = observation.facts_age_millis() else {
        return false;
    };
    facts_age.saturating_add(probe_age) <= FACTS_TTL
}

fn inner_facts_expired(
    observation: &AdmissionObservation,
    now_millis: u64,
    probed_at: Option<Instant>,
) -> bool {
    let Some(facts_age) = observation.facts_age_millis() else {
        return true;
    };
    let elapsed = if let Some(probed_at) = probed_at {
        duration_millis(probed_at.elapsed())
    } else {
        let Some(started) = observation.final_probe_started_at_millis() else {
            return true;
        };
        if started > now_millis {
            return true;
        }
        now_millis - started
    };
    facts_age.saturating_add(elapsed) > FACTS_TTL
}

fn candidate_at_return(
    observation: &AdmissionObservation,
    now_millis: u64,
    probed_at: Option<Instant>,
) -> Result<CandidateObservation, WorkerError> {
    let mut capabilities = observation.capabilities().to_vec();
    if observation.ready() && inner_facts_expired(observation, now_millis, probed_at) {
        capabilities.retain(|capability| !capability.starts_with("agent:"));
    }
    CandidateObservation::new(
        observation.worker_name().to_owned(),
        observation.ready(),
        observation.slot(),
        capabilities,
        observation.available_memory_bytes(),
        observation.free_disk_bytes(),
    )
    .map(|candidate| candidate.with_interactive_agents(observation.interactive_agents()))
    .map_err(|_| WorkerError::Protocol("cached scheduler observation is invalid".into()))
}

fn rank_committed(
    worker: &WorkerEntry,
    incoming: &AdmissionObservation,
    winner: AdmissionObservation,
    probe_started: Instant,
) -> Result<Option<(AdmissionObservation, Option<Instant>)>, WorkerError> {
    if &winner == incoming {
        return Ok(Some((winner, Some(probe_started))));
    }
    let now = now_millis()?;
    if reusable_for_admission(&winner, worker, now) {
        Ok(Some((winner, None)))
    } else {
        Ok(None)
    }
}

fn require_admission_worker(worker: &WorkerEntry) -> Result<(), WorkerError> {
    if worker.slots == 0 || worker.slots > MAX_HOST_SLOTS {
        return Err(WorkerError::Config(format!(
            "worker {:?} slots must be between 1 and {MAX_HOST_SLOTS}",
            worker.name
        )));
    }
    if !valid_ssh_destination(&worker.ssh) || worker.remote_binary != "~/.local/bin/worker" {
        return Err(WorkerError::Transport {
            code: "INVALID_REQUEST",
            message: "worker transport configuration is invalid".into(),
        });
    }
    Ok(())
}

fn is_caller_error(error: &WorkerError) -> bool {
    matches!(
        error,
        WorkerError::Config(_)
            | WorkerError::Transport {
                code: "INVALID_REQUEST",
                ..
            }
            | WorkerError::Io(_)
            | WorkerError::Protocol(_)
            | WorkerError::Queue { .. }
    )
}

fn admission_from_health(
    config: &crate::config::Config,
    health: &WorkerHealth,
    observed_at: u64,
) -> Result<AdmissionObservation, WorkerError> {
    let candidate = SchedulerProbeAdapter::observations(config, std::slice::from_ref(health))?
        .into_iter()
        .next()
        .ok_or_else(|| WorkerError::Protocol("worker probe was empty".into()))?;
    Ok(AdmissionObservation::new(
        candidate.worker_name().to_owned(),
        candidate.ready(),
        candidate.slot(),
        candidate.capabilities().to_vec(),
        candidate.available_memory_bytes(),
        candidate.free_disk_bytes(),
        observed_at,
    )?
    .with_interactive_agents(candidate.interactive_agents()))
}

fn negative_bound_observation(worker: &WorkerEntry) -> Result<AdmissionObservation, WorkerError> {
    let started = now_millis()?;
    Ok(AdmissionObservation::new(
        worker.name.clone(),
        false,
        CandidateSlot::Busy,
        Vec::new(),
        None,
        0,
        started,
    )?
    .with_local_binding(
        worker.ssh.clone(),
        worker.remote_binary.clone(),
        worker.capabilities.clone(),
        worker.slots,
        None,
        started,
    ))
}

fn unavailable_candidate(name: &str) -> Result<CandidateObservation, WorkerError> {
    CandidateObservation::new(
        name.to_owned(),
        false,
        CandidateSlot::Busy,
        Vec::new(),
        None,
        0,
    )
    .map_err(|_| WorkerError::Protocol("admission unavailable projection is invalid".into()))
}

fn single_worker_config(
    config: &crate::config::Config,
    worker: &WorkerEntry,
) -> crate::config::Config {
    config.with_workers(vec![worker.clone()])
}

fn request_budget(deadline: Instant, cap: Duration) -> Duration {
    remaining_until(deadline).min(cap)
}

fn remaining_until(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn duration_millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

fn now_millis() -> Result<u64, WorkerError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| WorkerError::Io(std::io::Error::other(error)))?
        .as_millis();
    u64::try_from(millis).map_err(|_| WorkerError::Protocol("clock is beyond u64 millis".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::{HashMap, HashSet},
        os::unix::process::ExitStatusExt,
        process::ExitStatus,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        thread,
    };

    use crate::{
        agent_facts::{
            AgentAuth, AgentFacts, AgentProbe, HerdrFactState, HerdrFacts, ProfileProbe,
        },
        client_state::ClientStateStore,
        job::AdmissionObservation,
        lease::SlotState,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
        scheduler::WorkerPreference,
        transfer::HostOperation,
    };

    #[derive(Clone, Copy)]
    enum FactsKind {
        Authenticated,
        Missing,
        Unauthenticated,
    }

    struct ScriptedRunner {
        probes: Mutex<Vec<String>>,
        refreshes: Mutex<Vec<String>>,
        probe_deadlines: Mutex<Vec<Duration>>,
        refresh_deadlines: Mutex<Vec<Duration>>,
        delay: Mutex<HashMap<String, Duration>>,
        fail: Mutex<HashMap<String, bool>>,
        facts_age: Mutex<HashMap<String, u64>>,
        facts_kind: Mutex<HashMap<String, FactsKind>>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        probe_entered: Mutex<Option<mpsc::Sender<()>>>,
        probe_release: Mutex<Option<mpsc::Receiver<()>>>,
        wait_before: Mutex<HashMap<String, mpsc::Receiver<()>>>,
        wait_before_timeout: Mutex<HashMap<String, Duration>>,
        notify_entered: Mutex<HashMap<String, mpsc::Sender<()>>>,
        notify_after: Mutex<HashMap<String, mpsc::Sender<()>>>,
        observation_gate: Mutex<HashMap<String, ObservationGate>>,
        refresh_delay: Mutex<HashMap<String, Duration>>,
        herdr_interactive: Mutex<HashMap<String, u32>>,
    }

    impl ScriptedRunner {
        fn new() -> Self {
            Self {
                probes: Mutex::new(Vec::new()),
                refreshes: Mutex::new(Vec::new()),
                probe_deadlines: Mutex::new(Vec::new()),
                refresh_deadlines: Mutex::new(Vec::new()),
                delay: Mutex::new(HashMap::new()),
                fail: Mutex::new(HashMap::new()),
                facts_age: Mutex::new(HashMap::new()),
                facts_kind: Mutex::new(HashMap::new()),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                probe_entered: Mutex::new(None),
                probe_release: Mutex::new(None),
                wait_before: Mutex::new(HashMap::new()),
                wait_before_timeout: Mutex::new(HashMap::new()),
                notify_entered: Mutex::new(HashMap::new()),
                notify_after: Mutex::new(HashMap::new()),
                observation_gate: Mutex::new(HashMap::new()),
                refresh_delay: Mutex::new(HashMap::new()),
                herdr_interactive: Mutex::new(HashMap::new()),
            }
        }

        fn delay(&self, ssh: &str, delay: Duration) {
            self.delay.lock().unwrap().insert(ssh.to_owned(), delay);
        }

        fn delay_refresh(&self, ssh: &str, delay: Duration) {
            self.refresh_delay
                .lock()
                .unwrap()
                .insert(ssh.to_owned(), delay);
        }

        fn herdr_interactive(&self, ssh: &str, count: u32) {
            self.herdr_interactive
                .lock()
                .unwrap()
                .insert(ssh.to_owned(), count);
        }

        fn fail(&self, ssh: &str) {
            self.fail.lock().unwrap().insert(ssh.to_owned(), true);
        }

        fn facts_age(&self, ssh: &str, age: u64) {
            self.facts_age.lock().unwrap().insert(ssh.to_owned(), age);
        }

        fn facts_kind(&self, ssh: &str, kind: FactsKind) {
            self.facts_kind.lock().unwrap().insert(ssh.to_owned(), kind);
        }

        fn hold_probe(&self, entered: mpsc::Sender<()>, release: mpsc::Receiver<()>) {
            *self.probe_entered.lock().unwrap() = Some(entered);
            *self.probe_release.lock().unwrap() = Some(release);
        }

        fn wait_before(&self, ssh: &str, release: mpsc::Receiver<()>) {
            self.wait_before_for(ssh, release, Duration::from_secs(2));
        }

        fn wait_before_for(&self, ssh: &str, release: mpsc::Receiver<()>, timeout: Duration) {
            self.wait_before
                .lock()
                .unwrap()
                .insert(ssh.to_owned(), release);
            self.wait_before_timeout
                .lock()
                .unwrap()
                .insert(ssh.to_owned(), timeout);
        }

        fn notify_entered(&self, ssh: &str, entered: mpsc::Sender<()>) {
            self.notify_entered
                .lock()
                .unwrap()
                .insert(ssh.to_owned(), entered);
        }

        #[allow(dead_code)]
        fn notify_after(&self, ssh: &str, entered: mpsc::Sender<()>) {
            self.notify_after
                .lock()
                .unwrap()
                .insert(ssh.to_owned(), entered);
        }

        fn wait_for_published_observation(
            &self,
            ssh: &str,
            store: ClientStateStore,
            worker_name: &str,
            timeout: Duration,
        ) -> ObservationPublishReceipt {
            let observed = Arc::new(AtomicBool::new(false));
            let diagnostic = Arc::new(Mutex::new(None));
            self.observation_gate.lock().unwrap().insert(
                ssh.to_owned(),
                ObservationGate {
                    store,
                    worker_name: worker_name.to_owned(),
                    timeout,
                    observed: observed.clone(),
                    diagnostic: diagnostic.clone(),
                },
            );
            ObservationPublishReceipt {
                observed,
                diagnostic,
            }
        }
    }

    impl ProcessRunner for ScriptedRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let ssh = ssh_destination(request);
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(current, Ordering::SeqCst);
            let _guard = InFlightGuard(&self.in_flight);
            if is_refresh(request) {
                self.refreshes.lock().unwrap().push(ssh.clone());
                self.refresh_deadlines
                    .lock()
                    .unwrap()
                    .push(request.policy.deadline);
                let refresh_delay = self.refresh_delay.lock().unwrap().get(&ssh).copied();
                if let Some(delay) = refresh_delay {
                    thread::sleep(delay);
                }
                return Ok(success(Vec::new()));
            }
            self.probes.lock().unwrap().push(ssh.clone());
            self.probe_deadlines
                .lock()
                .unwrap()
                .push(request.policy.deadline);
            let entered = self.notify_entered.lock().unwrap().remove(&ssh);
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            let entered = self.probe_entered.lock().unwrap().take();
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            let release = self.probe_release.lock().unwrap().take();
            if let Some(release) = release {
                let _ = release.recv();
            }
            let wait_timeout = self
                .wait_before_timeout
                .lock()
                .unwrap()
                .remove(&ssh)
                .unwrap_or(Duration::from_secs(2));
            let observation_gate = self.observation_gate.lock().unwrap().remove(&ssh);
            if let Some(gate) = observation_gate {
                wait_until_published(&gate);
            }
            let wait = self.wait_before.lock().unwrap().remove(&ssh);
            if let Some(wait) = wait {
                let _ = wait.recv_timeout(wait_timeout);
            }
            let delay = self.delay.lock().unwrap().get(&ssh).copied();
            if let Some(delay) = delay {
                thread::sleep(delay);
            }
            let notify = self.notify_after.lock().unwrap().remove(&ssh);
            if let Some(notify) = notify {
                let _ = notify.send(());
            }
            if self
                .fail
                .lock()
                .unwrap()
                .get(&ssh)
                .copied()
                .unwrap_or(false)
            {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(255 << 8),
                    stdout: Vec::new(),
                    stderr: b"offline".to_vec(),
                });
            }
            let age = self.facts_age.lock().unwrap().get(&ssh).copied();
            let kind = self
                .facts_kind
                .lock()
                .unwrap()
                .get(&ssh)
                .copied()
                .unwrap_or(FactsKind::Authenticated);
            let interactive = self.herdr_interactive.lock().unwrap().get(&ssh).copied();
            Ok(success(probe_bytes(age, kind, interactive)))
        }
    }

    struct ObservationPublishReceipt {
        observed: Arc<AtomicBool>,
        diagnostic: Arc<Mutex<Option<String>>>,
    }

    struct ObservationGate {
        store: ClientStateStore,
        worker_name: String,
        timeout: Duration,
        observed: Arc<AtomicBool>,
        diagnostic: Arc<Mutex<Option<String>>>,
    }

    fn wait_until_published(gate: &ObservationGate) {
        let deadline = Instant::now() + gate.timeout;
        loop {
            match gate.store.peek_admission_observation(&gate.worker_name) {
                Ok(Some(_)) => {
                    gate.observed.store(true, Ordering::SeqCst);
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    *gate.diagnostic.lock().unwrap() = Some(format!(
                        "peek {} failed before publication: {error}",
                        gate.worker_name
                    ));
                    return;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                *gate.diagnostic.lock().unwrap() = Some(format!(
                    "timed out after {:?} waiting for {} publication",
                    gate.timeout, gate.worker_name
                ));
                return;
            }
            thread::sleep(remaining.min(Duration::from_millis(5)));
        }
    }

    struct InFlightGuard<'a>(&'a AtomicUsize);

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn ssh_destination(request: &ProcessRequest) -> String {
        request
            .args
            .windows(2)
            .find(|window| window[0] == "--")
            .map(|window| window[1].to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    fn is_refresh(request: &ProcessRequest) -> bool {
        request
            .args
            .last()
            .is_some_and(|argument| argument == HostOperation::RefreshFacts.command())
    }

    fn success(stdout: Vec<u8>) -> ProcessResult {
        ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        }
    }

    fn probe_bytes(
        facts_age: Option<u64>,
        kind: FactsKind,
        interactive_agents: Option<u32>,
    ) -> Vec<u8> {
        let herdr = interactive_agents.map(|count| HerdrFacts {
            state: HerdrFactState::Available,
            version: Some("1.0.0".into()),
            interactive_agents: Some(count),
        });
        let agent_facts = match kind {
            FactsKind::Missing => None,
            FactsKind::Authenticated => Some(AgentFacts {
                agents: vec![AgentProbe {
                    name: "codex".into(),
                    version: Some("0.1.0".into()),
                    auth: AgentAuth::Authenticated,
                    auth_by_profile: vec![("secure".into(), AgentAuth::Authenticated)],
                }],
                env_profiles: vec![ProfileProbe {
                    name: "secure".into(),
                    secure: true,
                }],
                git_identity: true,
                collected_at_millis: 1,
                herdr: herdr.clone(),
            }),
            FactsKind::Unauthenticated => Some(AgentFacts {
                agents: vec![AgentProbe {
                    name: "codex".into(),
                    version: Some("0.1.0".into()),
                    auth: AgentAuth::Unauthenticated,
                    auth_by_profile: Vec::new(),
                }],
                env_profiles: vec![ProfileProbe {
                    name: "secure".into(),
                    secure: true,
                }],
                git_identity: true,
                collected_at_millis: 1,
                herdr,
            }),
        };
        let probe = ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            supervision_version: SUPERVISION_VERSION,
            hostname: "mini-1.local".into(),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 100,
            total_disk_bytes: 200,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: Some(0),
            available_memory_bytes: Some(10),
            cpu_counters: None,
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities: vec!["darwin-arm64".into()],
            agent_facts,
            facts_age_millis: facts_age,
            configured_slots: 0,
            busy_slots: 0,
        };
        serde_json::to_vec(&probe).unwrap()
    }

    fn worker(name: &str, ssh: &str) -> WorkerEntry {
        WorkerEntry {
            name: name.into(),
            ssh: ssh.into(),
            slots: 1,
            capabilities: vec!["darwin-arm64".into()],
            remote_binary: "~/.local/bin/worker".into(),
            herdr: false,
        }
    }

    fn config(workers: Vec<WorkerEntry>) -> crate::config::Config {
        crate::config::Config {
            version: 1,
            notifications: crate::config::NotificationsConfig::default(),
            controller: crate::config::ControllerConfig::default(),
            workers,
        }
    }

    fn temp_store() -> (tempfile::TempDir, ClientStateStore) {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().canonicalize().unwrap().join("state");
        let store = ClientStateStore::open(&state).unwrap();
        (dir, store)
    }

    fn bound_ready(entry: &WorkerEntry, at: u64, facts_age: Option<u64>) -> AdmissionObservation {
        AdmissionObservation::new(
            entry.name.clone(),
            true,
            CandidateSlot::Idle,
            vec!["darwin-arm64".into(), "agent:codex".into()],
            Some(10),
            20,
            at,
        )
        .unwrap()
        .with_local_binding(
            entry.ssh.clone(),
            entry.remote_binary.clone(),
            entry.capabilities.clone(),
            entry.slots,
            facts_age,
            at,
        )
    }

    fn bound_negative(entry: &WorkerEntry, at: u64) -> AdmissionObservation {
        AdmissionObservation::new(
            entry.name.clone(),
            false,
            CandidateSlot::Busy,
            Vec::new(),
            None,
            0,
            at,
        )
        .unwrap()
        .with_local_binding(
            entry.ssh.clone(),
            entry.remote_binary.clone(),
            entry.capabilities.clone(),
            entry.slots,
            None,
            at,
        )
    }

    fn now() -> u64 {
        now_millis().unwrap()
    }

    #[test]
    fn slow_offline_peer_does_not_erase_a_near_ttl_warm_hit() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let mini2 = worker("mini-2", "mac2");
        let config = config(vec![mini1.clone(), mini2.clone()]);
        let at = now().saturating_sub(1_900);
        store
            .publish_admission_observation(bound_ready(&mini1, at, Some(0)))
            .unwrap();
        let runner = ScriptedRunner::new();
        runner.delay("mac2", Duration::from_millis(250));
        runner.fail("mac2");
        runner.facts_age("mac1", 0);
        let ranked =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert!(ranked[0].ready(), "slow offline peer erased healthy mini-1");
        assert!(
            runner
                .probes
                .lock()
                .unwrap()
                .iter()
                .any(|ssh| ssh == "mac1"),
            "expired warm hit must re-enter the same round pool, probes={:?}",
            runner.probes.lock().unwrap()
        );
        assert!(!ranked[1].ready());
    }

    #[test]
    fn near_ttl_cold_observation_strips_agent_caps_after_a_slow_peer() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let mini2 = worker("mini-2", "mac2");
        let config = config(vec![mini1, mini2]);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", FACTS_TTL.saturating_sub(100));
        runner.delay("mac1", Duration::from_millis(80));
        runner.delay("mac2", Duration::from_millis(60));
        let published = runner.wait_for_published_observation(
            "mac2",
            store.clone(),
            "mini-1",
            Duration::from_secs(2),
        );
        runner.fail("mac2");
        let ranked =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        let diagnostic = published.diagnostic.lock().unwrap().clone();
        assert!(
            published.observed.load(Ordering::SeqCst),
            "mac2 60ms wait must follow mini-1's published observation; {diagnostic:?}"
        );
        let capabilities = ranked[0].capabilities().to_vec();
        assert!(ranked[0].ready());
        assert!(
            capabilities
                .iter()
                .all(|capability| !capability.starts_with("agent:")),
            "facts remaining life was 100ms; 80ms probe transit plus 60ms peer wait after final publish must strip agent caps, got {capabilities:?}"
        );
    }

    #[test]
    fn refresh_uses_elapsed_time_not_the_policy_cap() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", FACTS_TTL + 1);
        let remaining = Duration::from_secs(5);
        let ranked = worker_pipeline_inner(
            &runner,
            &config,
            &store,
            &mini1,
            remaining,
            Instant::now() + remaining,
        )
        .unwrap()
        .expect("small remaining budget must still finish probe/refresh");
        assert!(ranked.0.ready());
        assert_eq!(runner.refreshes.lock().unwrap().len(), 1);
        assert_eq!(runner.probes.lock().unwrap().len(), 2);
        let refresh_deadline = runner.refresh_deadlines.lock().unwrap()[0];
        assert!(
            refresh_deadline > Duration::ZERO && refresh_deadline <= remaining,
            "refresh must clamp to remaining round time, not burn the 30s cap; got {refresh_deadline:?}"
        );
        for deadline in runner.probe_deadlines.lock().unwrap().iter() {
            assert!(
                *deadline <= remaining,
                "probe deadline {deadline:?} exceeded remaining {remaining:?}"
            );
        }
    }

    #[test]
    fn bound_negative_is_reusable_without_facts_age() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        store
            .publish_admission_observation(bound_negative(&mini1, now()))
            .unwrap();
        let runner = ScriptedRunner::new();
        let ranked =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert!(runner.probes.lock().unwrap().is_empty());
        assert!(!ranked[0].ready());
    }

    #[test]
    fn zero_remaining_does_not_publish_over_a_ready_row() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let at = now();
        store
            .publish_admission_observation(bound_ready(&mini1, at, Some(0)))
            .unwrap();
        let runner = ScriptedRunner::new();
        let outcome = worker_pipeline_inner(
            &runner,
            &config,
            &store,
            &mini1,
            Duration::ZERO,
            Instant::now() + ADMISSION_ROUND_BUDGET,
        );
        assert!(outcome.unwrap().is_none());
        assert!(runner.probes.lock().unwrap().is_empty());
        let peeked = store.peek_admission_observation("mini-1").unwrap().unwrap();
        assert!(peeked.ready());
        assert_eq!(peeked.observed_at_millis(), at);
    }

    fn plant_stale_ready(store: &ClientStateStore, entry: &WorkerEntry) -> AdmissionObservation {
        let planted = bound_ready(entry, now(), Some(FACTS_TTL + 1));
        store
            .publish_admission_observation(planted.clone())
            .unwrap();
        planted
    }

    #[test]
    fn exhausted_refresh_budget_does_not_cache_a_synthetic_negative() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let planted = plant_stale_ready(&store, &mini1);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", FACTS_TTL + 1);
        runner.delay("mac1", Duration::from_millis(80));
        let remaining = Duration::from_millis(80);
        let outcome = worker_pipeline_inner(
            &runner,
            &config,
            &store,
            &mini1,
            remaining,
            Instant::now() + remaining,
        )
        .unwrap();
        assert!(
            outcome.is_none(),
            "a budget skip after a Ready probe must stay ephemeral"
        );
        assert_eq!(runner.probes.lock().unwrap().len(), 1);
        assert!(runner.refreshes.lock().unwrap().is_empty());
        let peeked = store.peek_admission_observation("mini-1").unwrap().unwrap();
        assert_eq!(peeked, planted);
        let retry = worker_pipeline_inner(
            &runner,
            &config,
            &store,
            &mini1,
            Duration::from_secs(5),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap()
        .expect("fresh remaining budget must retry probe/refresh");
        assert!(retry.0.ready());
        assert_eq!(runner.refreshes.lock().unwrap().len(), 1);
        assert_eq!(runner.probes.lock().unwrap().len(), 3);
    }

    #[test]
    fn exhausted_reprobe_budget_does_not_cache_a_synthetic_negative() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let planted = plant_stale_ready(&store, &mini1);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", FACTS_TTL + 1);
        runner.delay("mac1", Duration::from_millis(40));
        runner.delay_refresh("mac1", Duration::from_millis(80));
        let remaining = Duration::from_millis(120);
        let outcome = worker_pipeline_inner(
            &runner,
            &config,
            &store,
            &mini1,
            remaining,
            Instant::now() + remaining,
        )
        .unwrap();
        assert!(
            outcome.is_none(),
            "a budget skip after a successful refresh must stay ephemeral"
        );
        assert_eq!(runner.probes.lock().unwrap().len(), 1);
        assert_eq!(runner.refreshes.lock().unwrap().len(), 1);
        let peeked = store.peek_admission_observation("mini-1").unwrap().unwrap();
        assert_eq!(peeked, planted);
        let retry = worker_pipeline_inner(
            &runner,
            &config,
            &store,
            &mini1,
            Duration::from_secs(5),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap()
        .expect("fresh remaining budget must retry after a skipped re-probe");
        assert!(retry.0.ready());
        assert_eq!(runner.refreshes.lock().unwrap().len(), 2);
        assert_eq!(runner.probes.lock().unwrap().len(), 3);
    }

    #[test]
    fn refresh_lock_wait_timeout_does_not_poison_a_published_row() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let at = now();
        let original = bound_ready(&mini1, at, Some(0));
        store
            .publish_admission_observation(original.clone())
            .unwrap();
        let held = store
            .acquire_observation_refresh(&mini1.name, Instant::now() + Duration::from_secs(2))
            .unwrap()
            .expect("test holds the refresh lock");
        let newer = bound_ready(&mini1, at.saturating_add(10), Some(0));
        let runner = Arc::new(ScriptedRunner::new());
        let remaining = Duration::from_millis(80);
        let store_for_wait = store.clone();
        let config_for_wait = config.clone();
        let worker_for_wait = mini1.clone();
        let runner_for_wait = Arc::clone(&runner);
        let waiter = thread::spawn(move || {
            worker_pipeline_inner(
                runner_for_wait.as_ref(),
                &config_for_wait,
                &store_for_wait,
                &worker_for_wait,
                remaining,
                Instant::now() + remaining,
            )
        });
        thread::sleep(Duration::from_millis(15));
        store.publish_admission_observation(newer.clone()).unwrap();
        let outcome = waiter.join().unwrap().unwrap();
        drop(held);
        assert!(
            outcome.is_none(),
            "expired refresh wait must not SSH or publish"
        );
        assert!(runner.probes.lock().unwrap().is_empty());
        let peeked = store.peek_admission_observation("mini-1").unwrap().unwrap();
        assert_eq!(peeked, newer);
        assert_ne!(peeked.observed_at_millis(), original.observed_at_millis());
    }

    #[test]
    fn cas_replacement_winner_must_be_strictly_valid() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1]);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", 0);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        runner.hold_probe(entered_tx, release_rx);
        let store_for_probe = store.clone();
        let probing = thread::spawn(move || {
            observe_admission(
                &runner,
                &config,
                &store_for_probe,
                &WorkerPreference::Automatic,
            )
        });
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("probe snapshot must run before the CAS replacement");
        store
            .publish_admission_observation(
                AdmissionObservation::new(
                    "mini-1".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["darwin-arm64".into()],
                    Some(10),
                    20,
                    now(),
                )
                .unwrap(),
            )
            .unwrap();
        release_tx.send(()).unwrap();
        let ranked = probing.join().unwrap().unwrap();
        assert!(
            !ranked[0].ready(),
            "unbound CAS winner must not be ranked ready"
        );
    }

    #[test]
    fn missing_facts_do_not_become_a_ready_skip_ssh_hit() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", 0);
        runner.facts_kind("mac1", FactsKind::Missing);
        let first =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert!(first[0].ready());
        let stored = store.peek_admission_observation("mini-1").unwrap().unwrap();
        assert!(
            stored.facts_age_millis().is_none(),
            "missing agent_facts must not store a usable facts_age"
        );
        let probes_after_first = runner.probes.lock().unwrap().len();
        let second =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert!(second[0].ready());
        assert!(
            runner.probes.lock().unwrap().len() > probes_after_first,
            "ready row without facts must miss and refresh, not skip SSH"
        );
        assert!(!runner.refreshes.lock().unwrap().is_empty());
    }

    #[test]
    fn herdr_interactive_count_survives_probe_disk_and_warm_reuse() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", 0);
        runner.herdr_interactive("mac1", 3);
        let first =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert_eq!(first[0].interactive_agents(), Some(3));
        let stored = store.peek_admission_observation("mini-1").unwrap().unwrap();
        assert_eq!(stored.interactive_agents(), Some(3));
        let text = serde_json::to_string(&stored).unwrap();
        assert!(text.contains(r#""interactive_agents":3"#), "{text}");
        assert!(text.contains(r#""ssh":"mac1""#), "{text}");
        let back: AdmissionObservation = serde_json::from_str(&text).unwrap();
        assert_eq!(back, stored);
        let probes = runner.probes.lock().unwrap().len();
        assert_eq!(probes, 1);
        let second =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert_eq!(second[0].interactive_agents(), Some(3));
        assert_eq!(
            runner.probes.lock().unwrap().len(),
            probes,
            "bound Herdr count must skip SSH inside the outer TTL"
        );
    }

    #[test]
    fn current_unauthenticated_facts_remain_a_negative_capability_hit() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let config = config(vec![mini1.clone()]);
        let runner = ScriptedRunner::new();
        runner.facts_age("mac1", 0);
        runner.facts_kind("mac1", FactsKind::Unauthenticated);
        let first =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert!(first[0].ready());
        assert!(
            first[0]
                .capabilities()
                .iter()
                .all(|capability| !capability.starts_with("agent:"))
        );
        let probes = runner.probes.lock().unwrap().len();
        let second =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        assert!(second[0].ready());
        assert_eq!(
            runner.probes.lock().unwrap().len(),
            probes,
            "current no-agent facts must skip SSH inside the outer TTL"
        );
    }

    #[test]
    fn reused_warm_hit_expired_during_revisit_is_not_ranked() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let mini2 = worker("mini-2", "mac2");
        let mini3 = worker("mini-3", "mac3");
        let config = config(vec![mini1.clone(), mini2.clone(), mini3.clone()]);
        let planted_millis = now();
        store
            .publish_admission_observation(bound_ready(&mini1, planted_millis, Some(0)))
            .unwrap();
        store
            .publish_admission_observation(bound_ready(&mini2, planted_millis, Some(0)))
            .unwrap();
        let runner = ScriptedRunner::new();
        runner.fail("mac3");
        runner.facts_age("mac2", 0);
        let (mac3_entered_tx, mac3_entered_rx) = mpsc::channel();
        let (mac3_release_tx, mac3_release_rx) = mpsc::channel();
        let (mac2_entered_tx, mac2_entered_rx) = mpsc::channel();
        let (mac2_release_tx, mac2_release_rx) = mpsc::channel();
        runner.notify_entered("mac3", mac3_entered_tx);
        runner.wait_before("mac3", mac3_release_rx);
        runner.notify_entered("mac2", mac2_entered_tx);
        runner.wait_before_for(
            "mac2",
            mac2_release_rx,
            Duration::from_millis(OBSERVATION_TTL_MILLIS + 500),
        );
        let store_for_stale = store.clone();
        let mini2_for_stale = mini2.clone();
        let coordinator = thread::spawn(move || {
            mac3_entered_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("mini-3 miss wave must start before sibling timestamps are forced stale");
            let current = store_for_stale
                .peek_admission_observation("mini-2")
                .unwrap()
                .expect("mini-2 must still be the initial warm hit");
            store_for_stale
                .commit_admission_observation(
                    "mini-2",
                    Some(&current),
                    bound_ready(
                        &mini2_for_stale,
                        now().saturating_sub(OBSERVATION_TTL_MILLIS + 1),
                        Some(0),
                    ),
                )
                .expect("CAS must force mini-2 stale; publish would drop an older timestamp");
            mac3_release_tx
                .send(())
                .expect("release mini-3 after mini-2 is stale");
            mac2_entered_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("expired mini-2 must re-enter the original round pool");
            let expire_at = planted_millis + OBSERVATION_TTL_MILLIS;
            if let Some(remaining) = expire_at.saturating_add(1).checked_sub(now()) {
                thread::sleep(Duration::from_millis(remaining));
            }
            mac2_release_tx
                .send(())
                .expect("release mini-2 after mini-1's real outer TTL");
        });
        let ranked =
            observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
        coordinator
            .join()
            .expect("revisit fixture coordinator must finish");
        let probes = runner.probes.lock().unwrap().clone();
        let ranked_ready: Vec<_> = ranked
            .iter()
            .map(|candidate| (candidate.worker_name().to_owned(), candidate.ready()))
            .collect();
        assert_eq!(
            probes,
            ["mac3".to_owned(), "mac2".to_owned()],
            "first wave is the forced miss; revisit probes only the staled sibling; probes={probes:?} ranked={ranked_ready:?}"
        );
        assert!(
            !ranked[0].ready(),
            "a cached hit that aged past the outer TTL during the revisit wave must not be reused; probes={probes:?} ranked={ranked_ready:?}"
        );
        assert!(
            ranked[1].ready(),
            "the revisited sibling must publish a fresh ready observation; probes={probes:?} ranked={ranked_ready:?}"
        );
        assert!(
            !ranked[2].ready(),
            "offline miss must stay not ready; ranked={ranked_ready:?}"
        );
    }

    #[test]
    fn reversed_inventory_keeps_the_healthy_peer() {
        let (_dir, store) = temp_store();
        let offline = worker("mini-1", "mac1");
        let healthy = worker("mini-2", "mac2");
        for workers in [
            vec![offline.clone(), healthy.clone()],
            vec![healthy.clone(), offline.clone()],
        ] {
            let config = config(workers);
            let runner = ScriptedRunner::new();
            runner.fail("mac1");
            runner.delay("mac1", Duration::from_millis(80));
            runner.facts_age("mac2", 0);
            let ranked =
                observe_admission(&runner, &config, &store, &WorkerPreference::Automatic).unwrap();
            let ready = ranked
                .iter()
                .find(|candidate| candidate.worker_name() == "mini-2")
                .expect("healthy peer present");
            assert!(
                ready.ready(),
                "offline peer must not drop the healthy worker; order {:?}",
                ranked
                    .iter()
                    .map(|candidate| candidate.worker_name())
                    .collect::<Vec<_>>()
            );
        }
    }

    struct OverlapGate {
        in_probe: Mutex<HashSet<u8>>,
        overlapped: AtomicBool,
        release: Mutex<bool>,
        changed: Condvar,
    }

    impl OverlapGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                in_probe: Mutex::new(HashSet::new()),
                overlapped: AtomicBool::new(false),
                release: Mutex::new(false),
                changed: Condvar::new(),
            })
        }

        fn enter(&self, round: u8) {
            {
                let mut in_probe = self.in_probe.lock().unwrap();
                in_probe.insert(round);
                if in_probe.len() == 2 {
                    self.overlapped.store(true, Ordering::SeqCst);
                }
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut release = self.release.lock().unwrap();
            if self.overlapped.load(Ordering::SeqCst) {
                self.changed.notify_all();
            }
            while !*release {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    *release = true;
                    self.changed.notify_all();
                    break;
                }
                let (next, timeout) = self.changed.wait_timeout(release, remaining).unwrap();
                release = next;
                if timeout.timed_out() {
                    *release = true;
                    self.changed.notify_all();
                    break;
                }
            }
        }

        fn leave(&self, round: u8) {
            self.in_probe.lock().unwrap().remove(&round);
        }

        fn wait_for_overlap(&self) {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut release = self.release.lock().unwrap();
            while !self.overlapped.load(Ordering::SeqCst) && !*release {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let (next, timeout) = self.changed.wait_timeout(release, remaining).unwrap();
                release = next;
                if timeout.timed_out() {
                    break;
                }
            }
            *release = true;
            self.changed.notify_all();
        }
    }

    impl Drop for OverlapGate {
        fn drop(&mut self) {
            *self.release.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    struct RoundRunner {
        inner: Arc<ScriptedRunner>,
        round: u8,
        gate: Arc<OverlapGate>,
    }

    impl ProcessRunner for RoundRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if !is_refresh(request) {
                self.gate.enter(self.round);
                let result = self.inner.run(request);
                self.gate.leave(self.round);
                return result;
            }
            self.inner.run(request)
        }
    }

    #[test]
    fn simultaneous_reversed_inventories_single_flight_and_preserve_order() {
        let (_dir, store) = temp_store();
        let store = Arc::new(store);
        let workers: Vec<_> = (1..=4)
            .map(|index| worker(&format!("mini-{index}"), &format!("mac{index}")))
            .collect();
        let forward = config(workers.clone());
        let reversed = config(workers.iter().rev().cloned().collect());
        let inner = Arc::new(ScriptedRunner::new());
        for index in 1..=4 {
            inner.facts_age(&format!("mac{index}"), 0);
        }
        let gate = OverlapGate::new();
        let forward_runner = RoundRunner {
            inner: Arc::clone(&inner),
            round: 1,
            gate: Arc::clone(&gate),
        };
        let reversed_runner = RoundRunner {
            inner: Arc::clone(&inner),
            round: 2,
            gate: Arc::clone(&gate),
        };
        let forward_store = Arc::clone(&store);
        let reversed_store = Arc::clone(&store);
        let forward_config = forward.clone();
        let reversed_config = reversed.clone();
        thread::scope(|scope| {
            let forward_handle = scope.spawn(|| {
                let selected: Vec<_> = forward_config.workers.iter().collect();
                observe_workers(
                    &forward_runner,
                    &forward_config,
                    forward_store.as_ref(),
                    &selected,
                )
            });
            let reversed_handle = scope.spawn(|| {
                let selected: Vec<_> = reversed_config.workers.iter().collect();
                observe_workers(
                    &reversed_runner,
                    &reversed_config,
                    reversed_store.as_ref(),
                    &selected,
                )
            });
            gate.wait_for_overlap();
            let forward_ranked = forward_handle.join().unwrap().unwrap();
            let reversed_ranked = reversed_handle.join().unwrap().unwrap();
            assert!(
                gate.overlapped.load(Ordering::SeqCst),
                "both reversed inventories must overlap while refresh locks are held"
            );
            assert_eq!(
                forward_ranked
                    .iter()
                    .map(|candidate| candidate.worker_name())
                    .collect::<Vec<_>>(),
                vec!["mini-1", "mini-2", "mini-3", "mini-4"]
            );
            assert_eq!(
                reversed_ranked
                    .iter()
                    .map(|candidate| candidate.worker_name())
                    .collect::<Vec<_>>(),
                vec!["mini-4", "mini-3", "mini-2", "mini-1"]
            );
            let probes = inner.probes.lock().unwrap().clone();
            let mut unique = probes.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(
                probes.len(),
                unique.len(),
                "common workers must single-flight, probes={probes:?}"
            );
            assert_eq!(unique.len(), 4);
            assert!(forward_ranked.iter().all(|candidate| candidate.ready()));
            assert!(reversed_ranked.iter().all(|candidate| candidate.ready()));
        });
    }

    #[test]
    fn whole_pipeline_peak_stays_at_three_including_refresh() {
        let (_dir, store) = temp_store();
        let workers: Vec<_> = (1..=4)
            .map(|index| worker(&format!("mini-{index}"), &format!("mac{index}")))
            .collect();
        let config = config(workers);
        let runner = Arc::new(ScriptedRunner::new());
        for index in 1..=4 {
            runner.facts_age(&format!("mac{index}"), FACTS_TTL + 1);
            runner.delay(&format!("mac{index}"), Duration::from_millis(40));
        }
        let selected: Vec<_> = config.workers.iter().collect();
        let ranked = observe_workers(runner.as_ref(), &config, &store, &selected).unwrap();
        assert_eq!(ranked.len(), 4);
        assert!(
            runner.max_in_flight.load(Ordering::SeqCst) <= 3,
            "probe+refresh must stay inside the existing 3-wide pool, max={}",
            runner.max_in_flight.load(Ordering::SeqCst)
        );
        assert_eq!(runner.refreshes.lock().unwrap().len(), 4);
        assert!(
            ranked.iter().all(|candidate| candidate.ready()),
            "healthy refresh fixture must rank ready workers, got {ranked:?}"
        );
    }

    #[test]
    fn same_ms_capability_change_cas_keeps_the_disk_winner() {
        let (_dir, store) = temp_store();
        let mini1 = worker("mini-1", "mac1");
        let at = now();
        let expected = bound_ready(&mini1, at, Some(0));
        store
            .publish_admission_observation(expected.clone())
            .unwrap();
        let incoming = bound_ready(&mini1, at, Some(0));
        let disk = AdmissionObservation::new(
            mini1.name.clone(),
            true,
            CandidateSlot::Idle,
            vec!["darwin-arm64".into(), "agent:claude".into()],
            Some(10),
            20,
            at,
        )
        .unwrap()
        .with_local_binding(
            mini1.ssh.clone(),
            mini1.remote_binary.clone(),
            mini1.capabilities.clone(),
            mini1.slots,
            Some(0),
            at,
        );
        store.publish_admission_observation(disk.clone()).unwrap();
        let winner = store
            .commit_admission_observation("mini-1", Some(&expected), incoming.clone())
            .unwrap();
        assert_eq!(winner.capabilities(), disk.capabilities());
        let ranked = rank_committed(&mini1, &incoming, winner, Instant::now())
            .unwrap()
            .expect("strict reusable disk winner");
        assert!(
            ranked.1.is_none(),
            "replacement winner is not this pipeline's sample"
        );
        assert_eq!(ranked.0.capabilities(), disk.capabilities());
    }
}
