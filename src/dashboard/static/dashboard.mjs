const TERMINAL_STATES = new Set(['succeeded', 'failed', 'cancelled', 'timed_out', 'lost']);
const SNAPSHOT_INTERVAL_MILLIS = 2_000;
const LOG_INTERVAL_MILLIS = 1_000;
const LOG_LIMIT_BYTES = 65_536;

export function createDashboardClient({ document, fetch, timers }) {
  const nodes = Object.fromEntries(
    [
      'refresh-status',
      'rail-state-indicator',
      'last-updated',
      'worker-grid',
      'queue-list',
      'active-jobs',
      'recent-jobs',
      'job-detail',
      'stdout-log',
      'stderr-log',
      'task-filter-run',
      'task-filter-state',
      'task-filter-worker',
      'task-filter-agent',
      'run-progress',
      'task-list',
      'task-detail',
      'task-timeline',
      'task-stdout-log',
      'task-stderr-log',
    ].map((id) => [id, requiredNode(document, id)]),
  );

  let currentJob = null;
  let currentTask = null;
  let taskSelectionId = null;
  let taskDetailPending = null;
  let currentSnapshot = null;
  let stdoutOffset = 0;
  let stderrOffset = 0;
  let taskStdoutOffset = 0;
  let taskStderrOffset = 0;
  let stdoutDecoder = new TextDecoder();
  let stderrDecoder = new TextDecoder();
  let taskStdoutDecoder = new TextDecoder();
  let taskStderrDecoder = new TextDecoder();
  let stdoutDecoderFlushed = false;
  let stderrDecoderFlushed = false;
  let taskStdoutDecoderFlushed = false;
  let taskStderrDecoderFlushed = false;
  let taskFinalLengths = { stdout: null, stderr: null };
  let snapshotTimer = null;
  let legacyLogTimer = null;
  let taskLogTimer = null;
  let logRefreshPromise = null;
  let taskLogRefreshPromise = null;
  let started = false;
  let selectionGeneration = 0;
  let lastAppliedSnapshotRevision = null;
  const taskFilters = {
    run: '',
    state: '',
    worker: '',
    agent: '',
  };

  const element = (tagName, className, text) => {
    const node = document.createElement(tagName);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text == null ? '—' : String(text);
    return node;
  };

  const api = async (path, options = {}) => {
    const response = await fetch(path, { cache: 'no-store', ...options });
    const body = await response.json();
    if (!response.ok) {
      const error = new Error(body?.error?.message ?? `Request failed (${response.status})`);
      error.code = body?.error?.code;
      error.status = response.status;
      throw error;
    }
    return body;
  };

  const settingsOrigin = document.defaultView?.location?.origin;
  const fetchAgentSettings = (workerName) => api(
    `/api/v1/workers/${encodeURIComponent(workerName)}/agent-settings`,
  );
  const saveAgentSettings = (workerName, payload) => api(
    `/api/v1/workers/${encodeURIComponent(workerName)}/agent-settings`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Mac-Worker-Settings': '1',
        ...(settingsOrigin ? { Origin: settingsOrigin } : {}),
      },
      body: JSON.stringify(payload),
    },
  );

  const view = createObservatoryView(document, element, {
    fetchAgentSettings,
    saveAgentSettings,
  });

  const launchMeta = (task) => {
    const meta = element('div', 'launch-meta');
    for (const [label, value] of [['Agent', task?.agent], ['Model', task?.model], ['Effort', task?.effort]]) {
      const field = element('div');
      field.append(element('span', '', label), element('strong', '', value ?? 'Not reported'));
      meta.append(field);
    }
    return meta;
  };

  const renderWorker = (worker) => {
    const card = element('article', 'machine-card');
    const slot = worker.slot ?? {};
    const current = worker.freshness === 'current';
    const busy = slot.state === 'busy';
    card.setAttribute('data-state', worker.freshness);
    card.setAttribute('data-busy', String(busy));
    const heading = element('header', 'machine-card__header');
    const icon = element('span', 'machine-icon');
    icon.setAttribute('aria-hidden', 'true');
    const state = element('div', 'machine-card__state');
    const led = element('span', 'state-led');
    led.setAttribute('aria-hidden', 'true');
    state.append(led, element('span', '', current ? (busy ? 'Working' : 'Available') : humanize(worker.freshness)));
    heading.append(icon, element('h3', 'machine-card__name', worker.name), state);
    const load = element('div', 'machine-load');
    const prefix = worker.freshness === 'stale' ? 'Last ' : '';
    load.append(
      metric(`${prefix}CPU`, formatPercent(worker.system?.cpu_busy_percent), worker.system?.cpu_busy_percent == null ? 'metric--unavailable' : 'metric--hero'),
      metric(`${prefix}Memory`, worker.system?.memory_pressure ? humanize(worker.system.memory_pressure) : '—'),
      metric(`${prefix}Disk free`, formatBytes(worker.system?.free_disk_bytes)),
    );
    const bottom = element('div', 'machine-bottom');
    const line = element('div', 'machine-task-line');
    if (worker.active_task) {
      const task = worker.active_task;
      const title = element('button', 'machine-task-title', task.title);
      title.setAttribute('type', 'button');
      title.addEventListener('click', () => openTask(task.task_id));
      line.append(title, element('span', 'machine-task-age', `Turn ${task.turn_number ?? '—'}`));
      bottom.append(line, launchMeta(task));
    } else {
      const label = !current ? 'Observation is out of date' : busy ? 'Job in progress' : 'Ready for next task';
      const title = element(slot.active_job_id ? 'button' : 'p', 'machine-task-title', label);
      if (slot.active_job_id) {
        title.setAttribute('type', 'button');
        title.addEventListener('click', () => openJob(slot.active_job_id));
      }
      line.append(title);
      bottom.append(line, element('p', 'machine-note', current
        ? `${busy ? 1 : 0} / ${slot.capacity ?? 1} slots occupied · Current observation`
        : `Last seen ${formatAge(worker.observed_at_millis)} · ${worker.freshness === 'stale' ? 'Cached metrics' : 'Worker unavailable'}`));
    }
    if (worker.active_task && !current) bottom.append(element('p', 'machine-note', 'Cached task · worker observation is stale'));
    card.append(heading, load, bottom);
    return card;
  };

  const metric = (label, value, extraClass = '') => {
    const item = element('div', `metric ${extraClass}`.trim());
    item.append(element('span', 'metric__label', label), element('strong', 'metric__value', value));
    return item;
  };

  const appendFact = (list, term, description) => {
    list.append(element('dt', '', term), element('dd', '', description));
  };

  const renderQueueEntry = (entry) => {
    const ticket = element('article', 'ticket ticket--queue');
    const position = element('div', 'ticket__number', String(entry.position).padStart(2, '0'));
    const body = element('div', 'ticket__body');
    const entryLabel = entry.entry_kind === 'task_turn' ? 'Task turn' : 'Batch';
    const identity = entry.task_id && isSafeIdentifier(entry.task_id)
      ? `Task ${shortId(entry.task_id)}`
      : `Job ${shortId(entry.job_id)}`;
    const scheduling = [
      entry.run_id ? `run ${shortId(entry.run_id)}` : null,
      entry.run_max_parallel != null ? `cap ${entry.run_max_parallel}` : null,
      entry.pinned_worker ? `pinned ${entry.pinned_worker}` : null,
    ].filter(Boolean).join(' · ');
    const task = currentSnapshot?.tasks?.find((value) => value.task_id === entry.task_id);
    const title = element('button', 'ticket__title', task?.title ?? projectLabel(entry));
    title.setAttribute('type', 'button');
    title.addEventListener('click', () => entry.task_id ? openTask(entry.task_id) : openJob(entry.job_id));
    body.append(
      element('p', 'ticket__id', `${entryLabel} · ${identity}`),
      title,
      element('p', 'ticket__meta', `${commandSummary(entry.command_summary)} · queued ${formatAge(entry.created_at_millis)}${scheduling ? ` · ${scheduling}` : ''}`),
      element('p', 'ticket__note', `${humanize(entry.blocking_code)} · ${formatList(entry.requirements, 'No special requirements')}`),
    );
    ticket.append(position, body);
    return ticket;
  };

  const renderJob = (job, recent = false) => {
    const button = element('button', 'ticket ticket--job', `Inspect ${projectLabel(job)}`);
    button.setAttribute('type', 'button');
    button.setAttribute('data-state', job.state);
    button.addEventListener('click', () => openJob(job.job_id));

    const summary = element('span', 'ticket__summary');
    summary.append(
      element('span', 'ticket__id', `Job ${shortId(job.job_id)}`),
      element('strong', 'ticket__title', projectLabel(job)),
      element(
        'span',
        'ticket__meta',
        `${humanize(job.state)} · ${job.worker_name || 'Unassigned'} · ${commandSummary(job.command_summary)}`,
      ),
      element(
        'span',
        'ticket__note',
        recent ? resultSummary(job) : `Updated ${formatAge(job.updated_at_millis)}`,
      ),
    );
    button.textContent = '';
    button.append(element('span', 'ticket__inspect', 'Inspect'), summary, element('span', 'ticket__arrow', '↗'));
    return button;
  };

  const renderCollection = (region, values, renderer, emptyMessage) => {
    if (!values?.length) {
      region.replaceChildren(element('p', 'empty-state', emptyMessage));
      return;
    }
    region.replaceChildren(...values.map(renderer));
  };

  const renderRunProgress = (progress, runs) => {
    const counts = progress ?? {};
    const summary = element('div', 'run-progress__summary');
    for (const [label, key] of [
      ['Total', 'total'],
      ['Queued', 'queued'],
      ['Active', 'active'],
      ['Open', 'open'],
      ['Closed', 'closed'],
      ['Failed-like', 'failed_like'],
    ]) {
      const metricNode = element('div', 'run-progress__metric');
      metricNode.append(
        element('span', '', label),
        element('strong', '', counts[key] ?? 0),
      );
      summary.append(metricNode);
    }

    const runCards = element('div', 'run-progress__runs');
    if (!runs?.length) {
      runCards.append(element('p', 'empty-state', 'No named task runs recorded.'));
    } else {
      for (const run of runs) {
        const card = element('article', 'run-card');
        const runProgress = run.progress ?? {};
        card.append(
          element('p', 'run-card__id', `Run ${shortId(run.run_id)}`),
          element('h4', 'run-card__name', run.name ?? 'Unnamed run'),
          element('p', 'run-card__meta', `Max parallel ${run.max_parallel ?? '—'} · created ${formatTimestamp(run.created_at_millis)}`),
          element(
            'p',
            'run-card__progress',
            progressSummary(runProgress),
          ),
        );
        runCards.append(card);
      }
    }
    nodes['run-progress'].replaceChildren(summary, runCards);
  };

  const renderTaskFilters = (tasks) => {
    const definitions = [
      ['run', 'task-filter-run', 'All runs', (task) => task.run_id],
      ['state', 'task-filter-state', 'All states', (task) => task.state],
      ['worker', 'task-filter-worker', 'All workers', (task) => task.worker],
      ['agent', 'task-filter-agent', 'All agents', (task) => task.agent],
    ];
    for (const [key, id, allLabel, valueFor] of definitions) {
      const values = [...new Set((tasks ?? [])
        .map(valueFor)
        .filter((value) => value != null && value !== '')
        .map((value) => String(value)))]
        .sort((left, right) => left.localeCompare(right));
      const selected = taskFilters[key];
      const select = nodes[id];
      select.replaceChildren(element('option', '', allLabel));
      select.children[0].setAttribute('value', '');
      for (const value of values) {
        const option = element('option', '', filterLabel(key, value));
        option.setAttribute('value', value);
        select.append(option);
      }
      select.value = values.includes(selected) ? selected : '';
      taskFilters[key] = select.value;
    }
  };

  const renderTaskTable = (tasks) => {
    const rows = (tasks ?? [])
      .filter((task) => taskMatchesFilters(task, taskFilters))
      .filter((task) => isSafeIdentifier(task.task_id))
      .map(renderTaskRow);
    nodes['task-list'].replaceChildren(
      ...(rows.length
        ? rows
        : [element('p', 'empty-state', tasks?.length ? 'No tasks match the current filters.' : 'No durable tasks recorded.')]),
    );
  };

  const renderTaskRow = (task) => {
    const taskId = String(task.task_id);
    const button = element('button', 'task-row');
    button.setAttribute('type', 'button');
    button.setAttribute('data-task-id', taskId);
    button.setAttribute('data-state', task.state ?? 'unknown');
    button.addEventListener('click', () => openTask(taskId));

    const identity = element('span', 'task-row__identity');
    identity.append(
      element('span', 'task-row__inspect', 'Inspect'),
      element('span', 'task-row__id', `Task ${shortId(taskId)}`),
      element('strong', 'task-row__title', task.title ?? 'Untitled task'),
      element('span', 'task-row__meta', `${task.agent ?? 'Unknown agent'} · ${humanize(task.state)} · ${task.worker ?? 'Unassigned'}`),
    );

    const facts = element('span', 'task-row__facts');
    for (const value of [
      `Runner ${humanize(task.runner)}`,
      `Freshness ${humanize(task.freshness)}`,
      `${task.turn_count ?? 0} turns`,
      `Run position ${task.run_position ?? '—'}`,
      `Outcome ${outcomeSummary(task.last_outcome)}`,
      `Age ${formatAge(task.updated_at_millis)}`,
    ]) {
      facts.append(element('span', 'task-row__fact', value));
    }

    button.textContent = '';
    button.append(
      identity,
      facts,
      element('span', 'task-row__arrow', '↗'),
    );
    return button;
  };

  const appendResultList = (container, label, values, className) => {
    if (!values?.length) return;
    const wrapper = element('div', className);
    wrapper.append(element('strong', '', label));
    const list = element('ul');
    for (const value of values) list.append(element('li', '', value));
    wrapper.append(list);
    container.append(wrapper);
  };

  const renderTaskDetail = (detail) => {
    const task = detail.task;
    const header = element('div', 'detail-heading');
    header.append(
      element('p', 'eyebrow', `${humanize(task.state)} · ${humanize(task.freshness)}`),
      element('h3', 'detail-title', task.title ?? 'Untitled task'),
      element('p', 'detail-command', `${task.agent ?? 'Unknown agent'} · ${task.worker ?? 'Unassigned'} · runner ${humanize(task.runner)}`),
    );

    const facts = element('dl', 'detail-facts');
    appendFact(facts, 'Task ID', task.task_id);
    appendFact(facts, 'Run position', task.run_position ?? '—');
    appendFact(facts, 'Branch', task.branch ?? '—');
    appendFact(facts, 'Project', detail.project_id ?? '—');
    appendFact(facts, 'Worktree', detail.worktree_id ?? '—');
    appendFact(facts, 'Base', detail.base_oid ?? '—');
    appendFact(facts, 'Head', detail.head_oid ?? '—');
    appendFact(facts, 'Session present', Boolean(detail.session_present));
    appendFact(facts, 'Created', formatTimestamp(task.created_at_millis));
    appendFact(facts, 'Updated', formatTimestamp(task.updated_at_millis));

    const result = element('article', 'task-result-card');
    result.append(
      element('p', 'task-result-card__label', `Result · ${outcomeSummary(task.last_outcome)}`),
      element('p', 'task-result-card__summary', detail.summary ?? 'No summary reported.'),
    );
    appendResultList(result, 'Questions', detail.questions, 'task-result-card__questions');
    appendResultList(result, 'Changed files', detail.files_changed, 'task-result-card__files');
    if (detail.diff_stat != null) {
      result.append(element('p', 'task-result-card__files', `Diff stat · ${detail.diff_stat}`));
    }
    result.append(element('code', 'task-fetch-command', detail.fetch_command ?? `worker task fetch ${task.task_id}`));

    nodes['task-detail'].replaceChildren(header, facts, result);
    view.renderTask(detail);
  };

  const renderTaskTimeline = (detail) => {
    const events = Array.isArray(detail.timeline) ? detail.timeline : detail.turns;
    if (!events?.length) {
      nodes['task-timeline'].replaceChildren(element('p', 'empty-state', 'This task has no recorded turns.'));
      return;
    }
    const rendered = events.map((event) => {
      const item = element('article', 'timeline-event');
      item.setAttribute('data-terminal', String(event.terminal != null));
      const heading = element('div', 'timeline-event__heading');
      heading.append(
        element('strong', '', `Turn ${event.turn_number ?? '—'}`),
        element('span', 'timeline-event__id', `ID ${shortId(event.turn_id)}`),
      );
      const meta = element('div', 'timeline-event__meta');
      meta.append(
        element('span', '', `Started ${formatTimestamp(event.started_at_millis)}`),
        element('span', '', `Ended ${formatTimestamp(event.ended_at_millis)}`),
        element('span', '', `Terminal ${humanize(event.terminal)}`),
        element('span', '', `Outcome ${outcomeSummary(event.outcome)}`),
      );
      const committed = event.agent_committed == null ? 'Not recorded' : event.agent_committed ? 'Yes' : 'No';
      const flags = element('p', 'timeline-event__flags', `Committed ${committed} · Log ${event.log_truncated ? 'truncated' : 'complete'}`);
      item.append(heading, meta, flags);
      return item;
    });
    nodes['task-timeline'].replaceChildren(...rendered);
  };

  for (const [key, id] of [
    ['run', 'task-filter-run'],
    ['state', 'task-filter-state'],
    ['worker', 'task-filter-worker'],
    ['agent', 'task-filter-agent'],
  ]) {
    nodes[id].addEventListener('change', () => {
      taskFilters[key] = nodes[id].value;
      renderTaskTable(currentSnapshot?.tasks);
    });
  }

  const renderSnapshot = (snapshot) => {
    currentSnapshot = snapshot;
    renderCollection(nodes['worker-grid'], snapshot.workers, renderWorker, 'No workers configured.');
    renderCollection(nodes['queue-list'], snapshot.queue, renderQueueEntry, 'Queue clear — no jobs waiting.');
    renderCollection(nodes['active-jobs'], snapshot.active_jobs, (job) => renderJob(job, false), 'No jobs in flight.');
    renderCollection(nodes['recent-jobs'], snapshot.recent_jobs, (job) => renderJob(job, true), 'No retained job history.');
    renderRunProgress(snapshot.progress, snapshot.runs);
    renderTaskFilters(snapshot.tasks);
    renderTaskTable(snapshot.tasks);
    view.renderSnapshot(snapshot);

    const refreshedCurrentJob = currentJob
      ? [...(snapshot.active_jobs ?? []), ...(snapshot.recent_jobs ?? [])]
        .find((job) => job.job_id === currentJob.job_id)
      : null;
    if (refreshedCurrentJob) {
      currentJob = refreshedCurrentJob;
      renderJobDetail(currentJob);
    }

    if (currentTask) {
      const refreshedTask = (snapshot.tasks ?? [])
        .find((task) => task.task_id === currentTask.task.task_id);
      if (refreshedTask) {
        currentTask = { ...currentTask, task: refreshedTask };
        renderTaskDetail(currentTask);
      }
    }

    const freshness = snapshot.collection?.freshness ?? 'offline';
    const errorCount = snapshot.collection?.errors?.length ?? 0;
    nodes['refresh-status'].textContent = errorCount
      ? `${humanize(freshness)} · ${errorCount} collection ${errorCount === 1 ? 'issue' : 'issues'}`
      : `${humanize(freshness)} · revision ${snapshot.revision}`;
    nodes['refresh-status'].setAttribute('data-state', freshness);
    nodes['rail-state-indicator'].setAttribute('data-state', freshness);
    nodes['last-updated'].textContent = `Snapshot ${new Date(snapshot.generated_at_millis).toLocaleTimeString([], { hour12: false })}`;
  };

  const renderJobDetail = (job) => {
    const header = element('div', 'detail-heading');
    header.append(
      element('p', 'eyebrow', `${humanize(job.state)} · ${job.worker_name || 'Unassigned'}`),
      element('h3', 'detail-title', projectLabel(job)),
      element('p', 'detail-command', commandSummary(job.command_summary)),
    );

    const facts = element('dl', 'detail-facts');
    appendFact(facts, 'Job ID', job.job_id);
    appendFact(facts, 'Manifest', job.manifest_digest ?? '—');
    appendFact(facts, 'Resource class', job.resource_class ?? '—');
    appendFact(facts, 'Created', formatTimestamp(job.created_at_millis));
    appendFact(facts, 'Updated', formatTimestamp(job.updated_at_millis));
    appendFact(facts, 'Exit', exitSummary(job));
    appendFact(facts, 'Artifact', humanize(job.artifact_status ?? 'not_reported'));
    if (job.remote_uncertainty) appendFact(facts, 'Remote status', humanize(job.remote_uncertainty));
    nodes['job-detail'].replaceChildren(header, facts);
  };

  const refreshSnapshot = async () => {
    nodes['refresh-status'].textContent = 'Refreshing local observations…';
    try {
      const snapshot = await api('/api/v1/snapshot');
      if (!Number.isSafeInteger(snapshot?.revision) || snapshot.revision < 0) {
        throw new Error('Invalid dashboard snapshot revision');
      }
      if (
        lastAppliedSnapshotRevision != null
        && snapshot.revision < lastAppliedSnapshotRevision
      ) return;
      const selectedUpdate = currentTask && snapshot.tasks?.find((task) => task.task_id === currentTask.task.task_id);
      const reloadTask = selectedUpdate && (
        selectedUpdate.active_turn_id !== currentTask.task.active_turn_id
        || selectedUpdate.turn_count !== currentTask.task.turn_count
        || selectedUpdate.state !== currentTask.task.state
      );
      const retryTask = !currentTask && taskSelectionId && taskDetailPending == null
        && snapshot.tasks?.some((task) => task.task_id === taskSelectionId);
      renderSnapshot(snapshot);
      lastAppliedSnapshotRevision = snapshot.revision;
      if (reloadTask || retryTask) {
        await openTask(selectedUpdate?.task_id ?? taskSelectionId, { navigate: false });
      }
      if (started && selectionGeneration === 0 && view.page() === 'overview') {
        const active = snapshot.tasks?.find((task) => task.state === 'active' && task.freshness === 'current');
        if (active) await openTask(active.task_id);
      }
    } catch (error) {
      if (currentSnapshot) {
        const staleWorkers = (currentSnapshot.workers ?? []).map((worker) => ({
          ...worker, freshness: worker.freshness === 'offline' ? 'offline' : 'stale',
        }));
        renderCollection(nodes['worker-grid'], staleWorkers, renderWorker, 'No workers configured.');
        view.renderSnapshot({ ...currentSnapshot, workers: staleWorkers });
      }
      nodes['refresh-status'].textContent = `Dashboard unavailable — ${errorMessage(error)}`;
      nodes['refresh-status'].setAttribute('data-state', 'offline');
      nodes['rail-state-indicator'].setAttribute('data-state', 'offline');
    }
  };

  const resetLogs = () => {
    stdoutOffset = 0;
    stderrOffset = 0;
    stdoutDecoder = new TextDecoder();
    stderrDecoder = new TextDecoder();
    stdoutDecoderFlushed = false;
    stderrDecoderFlushed = false;
    nodes['stdout-log'].textContent = '';
    nodes['stderr-log'].textContent = '';
  };

  const openJob = async (jobId) => {
    view.selectPage('history');
    view.clearTask();
    taskSelectionId = null;
    taskDetailPending = null;
    const requestGeneration = ++selectionGeneration;
    logRefreshPromise = null;
    stopTaskLogTimer();
    taskLogRefreshPromise = null;
    currentTask = null;
    resetTaskLogs();
    currentJob = null;
    resetLogs();
    nodes['job-detail'].replaceChildren(element('p', 'empty-state', 'Loading job record…'));
    try {
      const selectedJob = await api(`/api/v1/jobs/${encodeURIComponent(jobId)}`);
      if (requestGeneration !== selectionGeneration) return;
      currentJob = selectedJob;
      renderJobDetail(currentJob);
      if (started && legacyLogTimer == null) {
        legacyLogTimer = timers.setInterval(refreshLogs, LOG_INTERVAL_MILLIS);
      }
    } catch (error) {
      if (requestGeneration !== selectionGeneration) return;
      nodes['job-detail'].replaceChildren(
        element('p', 'empty-state empty-state--error', `Job unavailable — ${errorMessage(error)}`),
      );
    }
  };

  const resetTaskLogs = () => {
    taskStdoutOffset = 0;
    taskStderrOffset = 0;
    taskStdoutDecoder = new TextDecoder();
    taskStderrDecoder = new TextDecoder();
    taskStdoutDecoderFlushed = false;
    taskStderrDecoderFlushed = false;
    taskFinalLengths = { stdout: null, stderr: null };
    nodes['task-stdout-log'].textContent = '';
    nodes['task-stderr-log'].textContent = '';
  };

  const stopTaskLogTimer = () => {
    if (taskLogTimer != null) timers.clearInterval(taskLogTimer);
    taskLogTimer = null;
  };

  const startTaskLogTimer = () => {
    if (started && taskLogTimer == null) {
      taskLogTimer = timers.setInterval(refreshTaskLogs, LOG_INTERVAL_MILLIS);
    }
  };

  const taskTurnForDetail = (detail) => {
    const turns = Array.isArray(detail?.turns) ? detail.turns : [];
    const activeTurnId = detail?.task?.active_turn_id;
    if (activeTurnId != null) {
      const activeTurn = turns.find((turn) => String(turn.turn_id) === String(activeTurnId));
      if (activeTurn) return activeTurn;
    }
    return turns.find((turn) => turn.terminal == null) ?? turns.at(-1) ?? null;
  };

  const taskTurnIsActive = (detail) => {
    const turn = taskTurnForDetail(detail);
    return detail?.task?.state === 'active' && turn?.terminal == null;
  };

  const taskFinalLength = (stream, detail = currentTask) => {
    const turn = taskTurnForDetail(detail);
    const field = `final_${stream}_bytes`;
    return taskFinalLengths[stream]
      ?? turn?.[field]
      ?? detail?.[field]
      ?? null;
  };

  const taskStreamComplete = (stream, offset) => {
    if (!currentTask) return false;
    const finalLength = taskFinalLength(stream);
    if (finalLength != null) return offset === finalLength;
    return false;
  };

  const rememberTaskFinalLength = (stream, chunk) => {
    const finalLength = chunk.final_length
      ?? chunk.final_bytes
      ?? chunk[`final_${stream}_bytes`]
      ?? null;
    if (Number.isSafeInteger(finalLength) && finalLength >= 0) {
      taskFinalLengths[stream] = finalLength;
    }
  };

  const refreshTaskStream = async (stream, offset, refreshGeneration, selectedTaskId, selectedTurnId) => {
    if (
      refreshGeneration !== selectionGeneration
      || currentTask?.task?.task_id !== selectedTaskId
      || taskTurnForDetail(currentTask)?.turn_id !== selectedTurnId
    ) return offset;
    if (taskStreamComplete(stream, offset)) return offset;
    const chunk = await api(
      `/api/v1/tasks/${encodeURIComponent(selectedTaskId)}/turns/${encodeURIComponent(selectedTurnId)}/logs?stream=${stream}&offset=${offset}&limit=${LOG_LIMIT_BYTES}`,
    );
    if (
      refreshGeneration !== selectionGeneration
      || currentTask?.task?.task_id !== selectedTaskId
      || taskTurnForDetail(currentTask)?.turn_id !== selectedTurnId
    ) return offset;
    if (
      chunk.stream !== stream
      || chunk.offset !== offset
      || !Number.isSafeInteger(chunk.next_offset)
      || chunk.next_offset < offset
    ) throw new Error(`Invalid ${stream} task log cursor`);
    const bytes = decodeBase64(chunk.data);
    if (
      chunk.next_offset !== offset + bytes.length
      || (taskFinalLength(stream) != null && chunk.next_offset > taskFinalLength(stream))
    ) throw new Error(`Invalid ${stream} task log length`);
    rememberTaskFinalLength(stream, chunk);
    // Terminal turn logs are immutable. An empty read confirms their final
    // length when the detail projection does not include byte counts.
    if (!taskTurnIsActive(currentTask) && bytes.length === 0) {
      taskFinalLengths[stream] = chunk.next_offset;
    }
    const decoder = stream === 'stdout' ? taskStdoutDecoder : taskStderrDecoder;
    const pane = stream === 'stdout' ? nodes['task-stdout-log'] : nodes['task-stderr-log'];
    pane.textContent += decoder.decode(bytes, { stream: true });
    view.updateLogEmpty(currentTask);
    return chunk.next_offset;
  };

  const flushTaskDecoders = () => {
    if (!taskStdoutDecoderFlushed) {
      nodes['task-stdout-log'].textContent += taskStdoutDecoder.decode();
      taskStdoutDecoderFlushed = true;
    }
    if (!taskStderrDecoderFlushed) {
      nodes['task-stderr-log'].textContent += taskStderrDecoder.decode();
      taskStderrDecoderFlushed = true;
    }
  };

  const performTaskLogRefresh = async () => {
    if (!currentTask) return;
    const refreshGeneration = selectionGeneration;
    const selectedTaskId = currentTask.task.task_id;
    const selectedTurn = taskTurnForDetail(currentTask);
    if (!selectedTurn || !isSafeIdentifier(selectedTurn.turn_id)) return;
    const selectedTurnId = String(selectedTurn.turn_id);
    try {
      const nextStdoutOffset = await refreshTaskStream(
        'stdout',
        taskStdoutOffset,
        refreshGeneration,
        selectedTaskId,
        selectedTurnId,
      );
      if (
        refreshGeneration !== selectionGeneration
        || currentTask?.task?.task_id !== selectedTaskId
        || taskTurnForDetail(currentTask)?.turn_id !== selectedTurnId
      ) return;
      taskStdoutOffset = nextStdoutOffset;
      const nextStderrOffset = await refreshTaskStream(
        'stderr',
        taskStderrOffset,
        refreshGeneration,
        selectedTaskId,
        selectedTurnId,
      );
      if (
        refreshGeneration !== selectionGeneration
        || currentTask?.task?.task_id !== selectedTaskId
        || taskTurnForDetail(currentTask)?.turn_id !== selectedTurnId
      ) return;
      taskStderrOffset = nextStderrOffset;
      const bothStreamsComplete = taskStreamComplete('stdout', taskStdoutOffset)
        && taskStreamComplete('stderr', taskStderrOffset);
      if (bothStreamsComplete) {
        flushTaskDecoders();
        stopTaskLogTimer();
      }
    } catch (error) {
      if (
        refreshGeneration !== selectionGeneration
        || currentTask?.task?.task_id !== selectedTaskId
        || taskTurnForDetail(currentTask)?.turn_id !== selectedTurnId
      ) return;
      const message = `Task log refresh paused — ${errorMessage(error)}`;
      nodes['task-stderr-log'].textContent += `${nodes['task-stderr-log'].textContent ? '\n' : ''}${message}`;
    }
  };

  const refreshTaskLogs = () => {
    if (taskLogRefreshPromise) return taskLogRefreshPromise;
    if (!currentTask) return Promise.resolve();
    let pendingRefresh;
    pendingRefresh = performTaskLogRefresh().finally(() => {
      if (taskLogRefreshPromise === pendingRefresh) taskLogRefreshPromise = null;
    });
    taskLogRefreshPromise = pendingRefresh;
    return pendingRefresh;
  };

  const openTask = async (taskId, { navigate = true } = {}) => {
    if (navigate) view.selectPage('overview');
    view.loading();
    const requestGeneration = ++selectionGeneration;
    taskDetailPending = null;
    taskSelectionId = null;
    stopTaskLogTimer();
    taskLogRefreshPromise = null;
    if (legacyLogTimer != null) timers.clearInterval(legacyLogTimer);
    legacyLogTimer = null;
    currentJob = null;
    resetLogs();
    currentTask = null;
    resetTaskLogs();
    nodes['job-detail'].replaceChildren(element('p', 'empty-state', 'Select a legacy ticket to inspect its record.'));
    nodes['task-detail'].replaceChildren(element('p', 'empty-state', 'Loading task record…'));
    nodes['task-timeline'].replaceChildren(element('p', 'empty-state', 'Loading turn history…'));
    if (!isSafeIdentifier(taskId)) {
      nodes['task-detail'].replaceChildren(
        element('p', 'empty-state empty-state--error', 'Task unavailable — invalid task identifier.'),
      );
      nodes['task-timeline'].replaceChildren(
        element('p', 'empty-state empty-state--error', 'Turn history unavailable.'),
      );
      view.error('Invalid task identifier.');
      return;
    }
    const selectedTaskId = String(taskId);
    taskSelectionId = selectedTaskId;
    taskDetailPending = requestGeneration;
    try {
      const selectedTask = await api(`/api/v1/tasks/${encodeURIComponent(selectedTaskId)}`);
      if (requestGeneration !== selectionGeneration) return;
      if (!selectedTask?.task || String(selectedTask.task.task_id) !== selectedTaskId) {
        throw new Error('Invalid task detail');
      }
      currentTask = selectedTask;
      renderTaskDetail(currentTask);
      renderTaskTimeline(currentTask);
      if (taskTurnForDetail(currentTask)) startTaskLogTimer();
    } catch (error) {
      if (requestGeneration !== selectionGeneration) return;
      nodes['task-detail'].replaceChildren(
        element('p', 'empty-state empty-state--error', `Task unavailable — ${errorMessage(error)}`),
      );
      nodes['task-timeline'].replaceChildren(
        element('p', 'empty-state empty-state--error', `Turn history unavailable — ${errorMessage(error)}`),
      );
      view.error(errorMessage(error));
    } finally {
      if (taskDetailPending === requestGeneration) taskDetailPending = null;
    }
  };

  const streamComplete = (stream, offset) => {
    if (!currentJob || !isTerminal(currentJob.state)) return false;
    const finalLength = stream === 'stdout'
      ? currentJob.final_stdout_bytes
      : currentJob.final_stderr_bytes;
    return finalLength != null && offset === finalLength;
  };

  const refreshStream = async (stream, offset, refreshGeneration, selectedJobId) => {
    if (refreshGeneration !== selectionGeneration || currentJob?.job_id !== selectedJobId) {
      return offset;
    }
    if (streamComplete(stream, offset)) return offset;
    const chunk = await api(
      `/api/v1/jobs/${encodeURIComponent(selectedJobId)}/logs?stream=${stream}&offset=${offset}&limit=${LOG_LIMIT_BYTES}`,
    );
    if (refreshGeneration !== selectionGeneration || currentJob?.job_id !== selectedJobId) {
      return offset;
    }
    if (chunk.stream !== stream || chunk.offset !== offset || chunk.next_offset < offset) {
      throw new Error(`Invalid ${stream} log cursor`);
    }
    const decoder = stream === 'stdout' ? stdoutDecoder : stderrDecoder;
    const pane = stream === 'stdout' ? nodes['stdout-log'] : nodes['stderr-log'];
    pane.textContent += decoder.decode(decodeBase64(chunk.data), { stream: true });
    return chunk.next_offset;
  };

  const performLogRefresh = async () => {
    if (!currentJob) return;
    const refreshGeneration = selectionGeneration;
    const selectedJobId = currentJob.job_id;
    try {
      const nextStdoutOffset = await refreshStream(
        'stdout',
        stdoutOffset,
        refreshGeneration,
        selectedJobId,
      );
      if (refreshGeneration !== selectionGeneration || currentJob?.job_id !== selectedJobId) return;
      stdoutOffset = nextStdoutOffset;
      const nextStderrOffset = await refreshStream(
        'stderr',
        stderrOffset,
        refreshGeneration,
        selectedJobId,
      );
      if (refreshGeneration !== selectionGeneration || currentJob?.job_id !== selectedJobId) return;
      stderrOffset = nextStderrOffset;
      if (streamComplete('stdout', stdoutOffset) && streamComplete('stderr', stderrOffset)) {
        if (!stdoutDecoderFlushed) {
          nodes['stdout-log'].textContent += stdoutDecoder.decode();
          stdoutDecoderFlushed = true;
        }
        if (!stderrDecoderFlushed) {
          nodes['stderr-log'].textContent += stderrDecoder.decode();
          stderrDecoderFlushed = true;
        }
        if (legacyLogTimer != null) timers.clearInterval(legacyLogTimer);
        legacyLogTimer = null;
      }
    } catch (error) {
      if (refreshGeneration !== selectionGeneration || currentJob?.job_id !== selectedJobId) return;
      const message = `Log refresh paused — ${errorMessage(error)}`;
      nodes['stderr-log'].textContent += `${nodes['stderr-log'].textContent ? '\n' : ''}${message}`;
    }
  };

  const refreshLogs = () => {
    if (logRefreshPromise) return logRefreshPromise;
    let pendingRefresh;
    pendingRefresh = performLogRefresh().finally(() => {
      if (logRefreshPromise === pendingRefresh) logRefreshPromise = null;
    });
    logRefreshPromise = pendingRefresh;
    return pendingRefresh;
  };

  const start = () => {
    if (started) return;
    started = true;
    snapshotTimer = timers.setInterval(refreshSnapshot, SNAPSHOT_INTERVAL_MILLIS);
    legacyLogTimer = timers.setInterval(refreshLogs, LOG_INTERVAL_MILLIS);
    if (currentTask && taskTurnForDetail(currentTask)) startTaskLogTimer();
  };

  return {
    refreshSnapshot,
    openJob,
    refreshLogs,
    openTask,
    refreshTaskLogs,
    start,
    offsets: () => ({ stdout: stdoutOffset, stderr: stderrOffset }),
    taskOffsets: () => ({ stdout: taskStdoutOffset, stderr: taskStderrOffset }),
  };
}

function requiredNode(document, id) {
  const node = document.getElementById(id);
  if (!node) throw new Error(`Dashboard shell is missing #${id}`);
  return node;
}

function isTerminal(state) {
  return TERMINAL_STATES.has(state);
}

function shortId(value) {
  return String(value ?? '').slice(0, 12);
}

function isSafeIdentifier(value) {
  return typeof value === 'string'
    && value.length > 0
    && value.length <= 256
    && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value);
}

function filterLabel(filter, value) {
  if (filter === 'run') return `Run ${shortId(value)}`;
  return humanize(value);
}

function taskMatchesFilters(task, filters) {
  if (!task || typeof task !== 'object') return false;
  return (!filters.run || String(task.run_id ?? '') === filters.run)
    && (!filters.state || String(task.state ?? '') === filters.state)
    && (!filters.worker || String(task.worker ?? '') === filters.worker)
    && (!filters.agent || String(task.agent ?? '') === filters.agent);
}

function progressSummary(progress) {
  return `Total ${progress.total ?? 0} · queued ${progress.queued ?? 0} · active ${progress.active ?? 0} · open ${progress.open ?? 0} · closed ${progress.closed ?? 0} · failed-like ${progress.failed_like ?? 0}`;
}

function outcomeSummary(outcome) {
  if (outcome == null) return '—';
  if (typeof outcome === 'string') return humanize(outcome);
  if (typeof outcome === 'object') {
    const label = humanize(outcome.kind);
    return outcome.reason == null ? label : `${label} · ${outcome.reason}`;
  }
  return String(outcome);
}

function projectLabel(record) {
  return record.project_label ?? `project-${shortId(record.project_id)}/worktree-${shortId(record.worktree_id)}`;
}

function commandSummary(summary) {
  if (summary?.mode === 'argv') return `argv (${summary.arg_count ?? 0} arguments)`;
  return 'shell';
}

function humanize(value) {
  return String(value ?? 'unknown')
    .replaceAll('_', ' ')
    .toLowerCase()
    .replace(/^./, (character) => character.toUpperCase());
}

function formatList(values, empty = '—') {
  return values?.length ? values.join(' · ') : empty;
}

function formatPercent(value) {
  return value == null ? 'Warming up' : `${Number(value).toFixed(1)}%`;
}

function formatBytes(value) {
  if (value == null) return '—';
  if (value === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const exponent = Math.min(Math.floor(Math.log(value) / Math.log(1_000)), units.length - 1);
  return `${(value / (1_000 ** exponent)).toFixed(exponent > 1 ? 1 : 0)} ${units[exponent]}`;
}

function formatDisk(system) {
  if (system?.free_disk_bytes == null) return '—';
  if (system.total_disk_bytes == null) return formatBytes(system.free_disk_bytes);
  return `${formatBytes(system.free_disk_bytes)} / ${formatBytes(system.total_disk_bytes)}`;
}

function formatTimestamp(value) {
  if (value == null) return 'Never';
  return new Date(value).toLocaleString([], { dateStyle: 'medium', timeStyle: 'medium' });
}

function formatAge(value) {
  if (value == null) return '—';
  const seconds = Math.max(0, Math.round((Date.now() - value) / 1_000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

function resultSummary(job) {
  const result = exitSummary(job);
  const artifact = job.artifact_status ? ` · artifact ${humanize(job.artifact_status).toLowerCase()}` : '';
  return `${result}${artifact}`;
}

function exitSummary(job) {
  if (job.exit_code != null) return `Exit ${job.exit_code}`;
  if (job.terminating_signal != null) return `Signal ${job.terminating_signal}`;
  return isTerminal(job.state) ? 'No exit result' : 'In progress';
}

function decodeBase64(value) {
  const binary = globalThis.atob(String(value ?? ''));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

const AGENTS = [
  { id: 'codex', name: 'Codex', initials: 'CX', cli: 'codex' },
  { id: 'cursor', name: 'Cursor', initials: 'CU', cli: 'cursor-agent' },
  { id: 'opencode', name: 'OpenCode', initials: 'OC', cli: 'opencode' },
  { id: 'claude', name: 'Claude Code', initials: 'CL', cli: 'claude' },
];

const AGENT_SETTINGS_FIELDS = [
  'agent',
  'model',
  'effort',
  'effort_options',
  'source',
  'revision',
  'writable',
  'message',
];

const hasExactKeys = (value, keys) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const actual = Object.keys(value).sort();
  return actual.length === keys.length && actual.every((key, index) => key === [...keys].sort()[index]);
};

const validSettingsText = (value, maxLength) => typeof value === 'string'
  && value.length > 0
  && value.length <= maxLength
  && !/[\r\n]/.test(value);

const validateAgentSettingsEntry = (entry, expectedAgent = null, { requireRevision = false } = {}) => {
  if (!hasExactKeys(entry, AGENT_SETTINGS_FIELDS)) {
    throw new Error('The native settings response was invalid.');
  }
  const knownAgent = AGENTS.some((agent) => agent.id === entry.agent);
  if (!knownAgent || (expectedAgent && entry.agent !== expectedAgent)) {
    throw new Error('The native settings response was invalid.');
  }
  if (entry.model != null && !validSettingsText(entry.model, 256)) {
    throw new Error('The native settings response was invalid.');
  }
  if (entry.effort != null && !validSettingsText(entry.effort, 64)) {
    throw new Error('The native settings response was invalid.');
  }
  if (!Array.isArray(entry.effort_options)
    || entry.effort_options.length > 64
    || entry.effort_options.some((value) => !validSettingsText(value, 64))) {
    throw new Error('The native settings response was invalid.');
  }
  if (!validSettingsText(entry.source, 128)
    || (entry.revision != null && !validSettingsText(entry.revision, 256))
    || (requireRevision && entry.revision == null)
    || typeof entry.writable !== 'boolean'
    || (entry.message != null && !validSettingsText(entry.message, 512))) {
    throw new Error('The native settings response was invalid.');
  }
  return entry;
};

const validateAgentSettingsList = (data) => {
  if (!hasExactKeys(data, ['agents']) || !Array.isArray(data.agents)) {
    throw new Error('The native settings response was invalid.');
  }
  const seen = new Set();
  for (const entry of data.agents) {
    validateAgentSettingsEntry(entry);
    if (seen.has(entry.agent)) throw new Error('The native settings response was invalid.');
    seen.add(entry.agent);
  }
  return data;
};

export function agentConnection(worker, agentName, profile) {
  const facts = worker?.agent_facts;
  const agent = facts?.agents?.find((entry) => entry.name === agentName);
  if (!worker || worker.freshness !== 'current' || facts?.freshness !== 'current' || !agent) {
    return { state: 'unknown', label: 'Unknown', auth: 'Unknown' };
  }
  const auth = profile == null
    ? agent.auth
    : agent.auth_by_profile?.find((entry) => entry.profile === profile)?.auth;
  if (auth === 'authenticated') return { state: 'connected', label: 'Connected', auth: 'Authenticated' };
  if (auth === 'unauthenticated') return { state: 'unauthenticated', label: 'Sign-in needed', auth: 'Unauthenticated' };
  return { state: 'unknown', label: 'Unknown', auth: 'Unknown' };
}

function createObservatoryView(document, element, {
  fetchAgentSettings = async () => { throw new Error('Native settings are unavailable.'); },
  saveAgentSettings = async () => { throw new Error('Native settings are unavailable.'); },
} = {}) {
  const node = (id) => requiredNode(document, id);
  const browser = document.defaultView;
  const pages = ['overview', 'tasks', 'history', 'settings'];
  const tabs = ['stdout', 'stderr', 'details'];
  let page = 'overview';
  let snapshot = null;
  let workerName = null;
  let agentId = 'codex';
  let renderSettings = () => {};
  let ensureSettingsForSelection = () => Promise.resolve();

  const settingsCache = new Map();
  const settingsDrafts = new Map();
  const settingsRequestTokens = new Map();
  const settingsSaveTokens = new Map();
  const settingsEpochs = new Map();
  const settingsSaveCounts = new Map();
  const settingsMutations = new Map();
  const settingsNotices = new Map();
  let detailKey = null;
  let detailRefs = null;

  const selectPage = (value, updateUrl = true) => {
    page = pages.includes(value) ? value : 'overview';
    for (const name of pages) {
      node(`page-${name}`).hidden = name !== page;
      node(`nav-${name}`).setAttribute('aria-current', name === page ? 'page' : 'false');
    }
    if (browser && updateUrl && browser.location.hash !== `#${page}`) {
      browser.history.pushState(null, '', `#${page}`);
    }
    if (page === 'settings' && snapshot) {
      renderSettings();
      return ensureSettingsForSelection();
    }
    return Promise.resolve();
  };
  for (const name of pages) {
    node(`nav-${name}`).addEventListener('click', async (event) => {
      event.preventDefault();
      await selectPage(name);
    });
  }
  const syncHash = () => selectPage(browser?.location.hash.slice(1), false);
  browser?.addEventListener('hashchange', syncHash);
  browser?.addEventListener('popstate', syncHash);
  syncHash();

  const selectTab = (selected) => {
    for (const tab of tabs) {
      node(`console-${tab}`).hidden = tab !== selected;
      node(`tab-${tab}`).setAttribute('aria-selected', String(tab === selected));
      node(`tab-${tab}`).setAttribute('tabindex', tab === selected ? '0' : '-1');
    }
  };
  for (const [index, tab] of tabs.entries()) {
    node(`tab-${tab}`).addEventListener('click', () => selectTab(tab));
    node(`tab-${tab}`).addEventListener('keydown', (event) => {
      const next = event.key === 'ArrowRight' ? (index + 1) % tabs.length
        : event.key === 'ArrowLeft' ? (index + tabs.length - 1) % tabs.length
          : event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : null;
      if (next != null) {
        event.preventDefault();
        selectTab(tabs[next]);
        node(`tab-${tabs[next]}`).focus();
      }
    });
  }

  const latestTask = (worker, agent) => (snapshot?.tasks ?? [])
    .filter((task) => task.worker === worker?.name && task.agent === agent)
    .sort((left, right) => right.updated_at_millis - left.updated_at_millis)[0] ?? null;
  const effectiveProfile = (task) => task ? task.env_profile ?? null : snapshot?.project_defaults?.env_profile ?? null;
  const connectionNode = (connection) => {
    const status = element('span', 'connection', connection.label);
    status.setAttribute('data-state', connection.state);
    return status;
  };
  const appendFactRef = (list, label, value = 'Not reported') => {
    const term = element('dt', '', label);
    const description = element('dd', '', value ?? 'Not reported');
    list.append(term, description);
    return description;
  };
  const settingsKeyFor = (worker, agent) => `${worker ?? ''}::${agent}`;
  const sourceLabel = (source) => ({
    'native-codex': 'Codex user settings',
    'native-cursor': 'Cursor user settings',
    'native-opencode': 'OpenCode user settings',
    'native-claude': 'Claude Code user settings',
  }[source] ?? 'Native user settings');
  const settingsErrorText = (error) => error instanceof Error && error.message
    ? error.message
    : 'Native settings could not be loaded.';
  const settingsEntry = (worker, agent) => settingsCache.get(worker)?.data?.agents
    ?.find((entry) => entry.agent === agent) ?? null;
  const settingsDraft = (worker, agent, entry = settingsEntry(worker, agent)) => {
    const key = settingsKeyFor(worker, agent);
    if (!settingsDrafts.has(key)) {
      settingsDrafts.set(key, {
        model: entry?.model ?? null,
        effort: entry?.effort ?? null,
        dirty: false,
      });
    } else if (!settingsDrafts.get(key).dirty && entry) {
      settingsDrafts.set(key, {
        model: entry.model ?? null,
        effort: entry.effort ?? null,
        dirty: false,
      });
    }
    return settingsDrafts.get(key);
  };
  const settingsValueLabel = (value) => value == null ? 'CLI default' : String(value);
  const latestValueLabel = (value) => value == null ? 'Not reported' : String(value);
  const settingsState = (worker) => settingsCache.get(worker) ?? { status: 'idle' };
  const mutationState = (worker, agent) => settingsMutations.get(settingsKeyFor(worker, agent)) ?? { saving: false, error: null };
  const settingsNotice = (worker, agent) => settingsNotices.get(settingsKeyFor(worker, agent)) ?? null;
  const settingsSaveInFlight = (worker) => (settingsSaveCounts.get(worker) ?? 0) > 0;
  const bumpSettingsEpoch = (worker) => {
    const next = (settingsEpochs.get(worker) ?? 0) + 1;
    settingsEpochs.set(worker, next);
    return next;
  };
  const releaseSettingsSave = (worker) => {
    const next = Math.max(0, (settingsSaveCounts.get(worker) ?? 0) - 1);
    if (next) settingsSaveCounts.set(worker, next);
    else settingsSaveCounts.delete(worker);
  };
  const settingsEntryUnavailable = (entry) => Boolean(
    entry && !entry.writable && entry.model == null && entry.effort == null,
  );
  const settingsEffortState = (agent, entry) => {
    if (!entry || settingsEntryUnavailable(entry)) {
      return { value: 'Unavailable', hint: null, kind: 'unavailable' };
    }
    const options = Array.isArray(entry.effort_options) ? entry.effort_options : [];
    if (options.length) {
      return { value: settingsValueLabel(entry.effort), hint: null, kind: 'supported' };
    }
    if (agent === 'opencode' && entry.effort == null) {
      return {
        value: 'Not supported',
        hint: 'This agent does not publish a global effort setting.',
        kind: 'unsupported',
      };
    }
    return {
      value: settingsValueLabel(entry.effort),
      hint: 'Choices unavailable',
      kind: 'unverified',
    };
  };

  const updateEditorControls = () => {
    if (!detailRefs?.editorControls) return;
    const { save, cancel, model, effort, effortReset, entry, draft, worker, agent } = detailRefs.editorControls;
    const mutation = mutationState(worker, agent);
    const canWrite = Boolean(entry?.writable && entry.revision);
    save.disabled = !canWrite || !draft.dirty || mutation.saving;
    cancel.disabled = !draft.dirty || mutation.saving;
    model.disabled = !canWrite || mutation.saving;
    if (effort) effort.disabled = effort.getAttribute('data-editable') !== 'true' || !canWrite || mutation.saving;
    if (effort && effort.getAttribute('data-editable') !== 'true') {
      effort.value = draft.effort == null ? 'CLI default' : draft.effort;
    }
    if (effortReset) effortReset.disabled = !canWrite || mutation.saving || draft.effort == null;
    if (detailRefs.editorMessage) {
      detailRefs.editorMessage.textContent = settingsNotice(worker, agent)
        ?? mutation.error
        ?? (mutation.saving
          ? 'Saving native defaults…'
          : detailRefs.editorRefreshing ? 'Refreshing native defaults…' : '');
    }
  };

  const renderNativeEditor = () => {
    if (!detailRefs) return;
    const { editor, worker, agent } = detailRefs;
    const cache = settingsState(worker);
    const entry = settingsEntry(worker, agent);
    const draft = settingsDraft(worker, agent, entry);
    const mutation = mutationState(worker, agent);
    detailRefs.editorRefreshing = cache.status === 'loading' && Boolean(entry);
    const signature = JSON.stringify([
      worker,
      agent,
      cache.status === 'error' ? 'error' : entry ? 'cached-entry' : cache.status,
      entry,
      mutation.saving,
      mutation.error,
      settingsNotice(worker, agent),
    ]);
    if (detailRefs.editorSignature === signature) {
      updateEditorControls();
      return;
    }
    detailRefs.editorSignature = signature;
    const heading = element('div', 'settings-editor__heading');
    heading.append(
      element('h3', '', 'Native defaults'),
      element('span', 'eyebrow', entry ? sourceLabel(entry.source) : 'Native user settings'),
    );
    const help = element('p', 'settings-editor__help', 'These values affect future CLI launches. Task, environment, project, or model-specific settings may override them.');
    const sourceMessage = entry?.message ? element('p', 'settings-editor__source-message', entry.message) : null;
    const message = element('p', 'settings-editor__message');
    message.setAttribute('aria-live', 'polite');
    const body = element('div', 'settings-editor__body');

    if (cache.status === 'loading' && !entry) {
      body.append(element('p', 'empty-state', 'Loading native defaults…'));
      editor.replaceChildren(heading, help, body, message);
      detailRefs.editorMessage = message;
      detailRefs.editorControls = null;
      updateEditorControls();
      return;
    }
    if (cache.status === 'error') {
      body.append(element('p', 'empty-state empty-state--error', `Load failed · ${settingsErrorText(cache.error)}`));
      editor.replaceChildren(heading, help, body, message);
      detailRefs.editorMessage = message;
      detailRefs.editorControls = null;
      updateEditorControls();
      return;
    }
    if (!entry) {
      body.append(element('p', 'empty-state', worker ? 'Native defaults were not reported for this agent.' : 'Select a worker to load native defaults.'));
      editor.replaceChildren(heading, help, body, message);
      detailRefs.editorMessage = message;
      detailRefs.editorControls = null;
      updateEditorControls();
      return;
    }
    if (settingsEntryUnavailable(entry)) {
      body.append(element('p', 'empty-state empty-state--error', `Unavailable · ${entry.message ?? 'Native defaults could not be read.'}`));
      editor.replaceChildren(heading, help, body, message);
      detailRefs.editorMessage = message;
      detailRefs.editorControls = null;
      updateEditorControls();
      return;
    }

    const canWrite = Boolean(entry.writable && entry.revision);
    const modelLabel = element('label', 'settings-field');
    modelLabel.append(element('span', '', 'Default model'));
    const model = element('input');
    model.setAttribute('type', 'text');
    model.setAttribute('maxlength', '256');
    model.setAttribute('autocomplete', 'off');
    model.setAttribute('data-testid', 'agent-settings-model');
    model.setAttribute('aria-label', 'Native default model');
    model.setAttribute('placeholder', 'CLI default');
    model.value = draft.model ?? '';
    model.addEventListener('input', () => {
      draft.model = model.value.trim() || null;
      draft.dirty = draft.model !== (entry.model ?? null) || draft.effort !== (entry.effort ?? null);
      updateEditorControls();
    });
    modelLabel.append(model);

    const effortLabel = element('label', 'settings-field');
    effortLabel.append(element('span', '', 'Default effort'));
    let effortControl = null;
    let effortReset = null;
    const effortOptions = Array.isArray(entry.effort_options)
      ? entry.effort_options.filter((value) => typeof value === 'string' && value.length > 0)
      : [];
    if (effortOptions.length > 0) {
      const effort = element('select');
      effort.setAttribute('data-testid', 'agent-settings-effort');
      effort.setAttribute('aria-label', 'Native default effort');
      effort.setAttribute('data-editable', 'true');
      const defaultOption = element('option', '', 'CLI default');
      defaultOption.setAttribute('value', '');
      effort.append(defaultOption);
      const values = [...effortOptions];
      if (entry.effort && !values.includes(entry.effort)) values.push(entry.effort);
      for (const value of values) {
        const option = element('option', '', value);
        option.setAttribute('value', value);
        effort.append(option);
      }
      effort.value = draft.effort ?? '';
      effort.addEventListener('change', () => {
        draft.effort = effort.value || null;
        draft.dirty = draft.model !== (entry.model ?? null) || draft.effort !== (entry.effort ?? null);
        updateEditorControls();
      });
      effortControl = effort;
      effortLabel.append(effort);
    } else if (entry.effort != null) {
      const effort = element('input');
      effort.setAttribute('type', 'text');
      effort.setAttribute('data-testid', 'agent-settings-effort');
      effort.setAttribute('aria-label', 'Native default effort');
      effort.value = draft.effort ?? 'CLI default';
      effort.readOnly = true;
      effort.setAttribute('data-editable', 'false');
      const effortHint = element('small', 'settings-field__hint', 'Choices unavailable');
      effortReset = element('button', 'button button--quiet settings-field__reset', 'Use CLI default');
      effortReset.setAttribute('type', 'button');
      effortReset.setAttribute('data-testid', 'agent-settings-effort-reset');
      effortReset.addEventListener('click', () => {
        draft.effort = null;
        draft.dirty = draft.model !== (entry.model ?? null) || draft.effort !== (entry.effort ?? null);
        updateEditorControls();
      });
      effortLabel.append(effort, effortHint, effortReset);
      effortControl = effort;
    } else if (agent !== 'opencode') {
      const defaultEffort = element('span', 'settings-field__value', 'CLI default');
      defaultEffort.setAttribute('data-testid', 'agent-settings-effort');
      effortLabel.append(
        defaultEffort,
        element('small', 'settings-field__hint', 'Choices unavailable'),
      );
    } else {
      const unsupported = element('span', 'settings-field__value', 'Not supported');
      unsupported.setAttribute('data-testid', 'agent-settings-effort');
      effortLabel.append(unsupported, element('small', 'settings-field__hint', 'This agent does not publish a global effort setting.'));
    }

    const fields = element('div', 'settings-editor__fields');
    fields.append(modelLabel, effortLabel);
    const actions = element('div', 'settings-editor__actions');
    const save = element('button', 'button button--primary', 'Save changes');
    save.setAttribute('type', 'button');
    save.setAttribute('data-testid', 'agent-settings-save');
    const cancel = element('button', 'button button--quiet', 'Cancel');
    cancel.setAttribute('type', 'button');
    cancel.setAttribute('data-testid', 'agent-settings-cancel');
    actions.append(save, cancel);
    const effortHelp = element(
      'p',
      'settings-editor__effort-help',
      'Effort choices describe the loaded model. If you change the model, review the effort or choose CLI default before saving; the worker validates the pair.',
    );
    body.append(fields, effortHelp, actions);
    editor.replaceChildren(heading, help, ...(sourceMessage ? [sourceMessage] : []), body, message);
    detailRefs.editorMessage = message;
    detailRefs.editorControls = {
      save,
      cancel,
      model,
      effort: effortControl,
      effortReset,
      entry,
      draft,
      worker,
      agent,
    };
    model.disabled = !canWrite || mutation.saving;
    save.addEventListener('click', () => saveCurrentSettings(worker, agent));
    cancel.addEventListener('click', () => {
      settingsDrafts.delete(settingsKeyFor(worker, agent));
      settingsNotices.delete(settingsKeyFor(worker, agent));
      settingsMutations.delete(settingsKeyFor(worker, agent));
      if (detailRefs) detailRefs.editorSignature = null;
      renderSettings();
    });
    updateEditorControls();
  };

  const renderDetailStatic = (worker, agent) => {
    const task = latestTask(worker, agent.id);
    const profile = effectiveProfile(task);
    const status = agentConnection(worker, agent.id, profile);
    const probe = worker?.agent_facts?.agents?.find((value) => value.name === agent.id);
    const destinationKey = settingsKeyFor(workerName, agent.id);
    if (detailKey !== destinationKey || !detailRefs) {
      const heading = element('div', 'agent-detail-heading');
      const name = element('h2', '', agent.name);
      const connection = connectionNode(status);
      heading.append(name, connection);
      const facts = element('dl');
      const latestModel = appendFactRef(facts, 'Latest task model');
      const latestEffort = appendFactRef(facts, 'Latest task effort');
      const permissions = appendFactRef(facts, 'Permissions');
      const environment = appendFactRef(facts, 'Environment profile');
      const authentication = appendFactRef(facts, 'Authentication');
      const cliVersion = appendFactRef(facts, 'CLI version');
      const editor = element('section', 'agent-settings-editor');
      const note = element('p', 'agent-detail-note');
      const noteStrong = element('strong');
      const noteBody = element('span');
      note.append(noteStrong, noteBody);
      detailRefs = {
        heading: name,
        connection,
        subtitle: element('p', 'agent-detail-subtitle'),
        latestModel,
        latestEffort,
        permissions,
        environment,
        authentication,
        cliVersion,
        editor,
        noteStrong,
        noteBody,
        worker: workerName,
        agent: agent.id,
        editorSignature: null,
        editorControls: null,
        editorMessage: null,
        editorRefreshing: false,
      };
      node('agent-detail').replaceChildren(heading, detailRefs.subtitle, editor, facts, note);
      detailKey = destinationKey;
    }
    detailRefs.worker = workerName;
    detailRefs.agent = agent.id;
    detailRefs.heading.textContent = agent.name;
    detailRefs.connection.textContent = status.label;
    detailRefs.connection.setAttribute('data-state', status.state);
    detailRefs.subtitle.textContent = `${workerName ?? 'No worker'} / ${agent.cli}`;
    detailRefs.latestModel.textContent = latestValueLabel(task?.model);
    detailRefs.latestEffort.textContent = latestValueLabel(task?.effort);
    detailRefs.permissions.textContent = task ? latestValueLabel(task.permissions) : latestValueLabel(snapshot.project_defaults?.permissions?.[agent.id]);
    detailRefs.environment.textContent = task || snapshot.project_defaults ? profile ?? 'None' : 'Not reported';
    detailRefs.authentication.textContent = status.auth;
    detailRefs.cliVersion.textContent = latestValueLabel(probe?.version);
    detailRefs.noteStrong.textContent = task
      ? `Latest task · ${shortId(task.task_id)}${task.freshness === 'stale' ? ' · cached' : ''}`
      : 'No task settings recorded';
    detailRefs.noteBody.textContent = worker?.freshness !== 'current' || worker?.agent_facts?.freshness !== 'current'
      ? 'Agent observation is unavailable or stale. Connection cannot be confirmed.'
      : 'Latest task values are shown separately from native defaults.';
    renderNativeEditor();
  };

  const rebuildSettingsList = (workers) => {
    const focusedAgent = document.activeElement?.getAttribute('data-agent');
    const select = node('settings-worker');
    const workerValues = workers.map((worker) => worker.name);
    const oldValues = Array.from(select.children).map((option) => option.value ?? option.getAttribute('value')).join('|');
    if (oldValues !== workerValues.join('|') || !select.children.length) {
      const options = workers.length ? workers.map((worker) => {
        const option = element('option', '', worker.name);
        option.setAttribute('value', worker.name);
        return option;
      }) : [element('option', '', 'No workers')];
      select.replaceChildren(...options);
    }
    select.value = workerName ?? '';
    select.disabled = !workers.length;
    const worker = workers.find((entry) => entry.name === workerName);
    let connected = 0;
    const rows = AGENTS.map((agent) => {
      const task = latestTask(worker, agent.id);
      const status = agentConnection(worker, agent.id, effectiveProfile(task));
      if (status.state === 'connected') connected += 1;
      const row = element('button', 'agent-row');
      row.setAttribute('type', 'button');
      row.setAttribute('data-agent', agent.id);
      row.setAttribute('aria-pressed', String(agent.id === agentId));
      const identity = element('span', 'agent-identity');
      const label = element('span');
      const latest = task
        ? `Latest task · ${formatAge(task.updated_at_millis)} · ${latestValueLabel(task.model)} / ${latestValueLabel(task.effort)}`
        : 'No recorded tasks';
      label.append(element('strong', '', agent.name), element('small', '', latest));
      identity.append(element('span', 'agent-monogram', agent.initials), label);
      row.append(identity, connectionNode(status));
      const cache = settingsState(workerName);
      const entry = settingsEntry(workerName, agent.id);
      const loadingValue = { value: 'Loading…', hint: null, missing: true };
      const errorValue = { value: 'Load failed', hint: null, missing: true };
      const unloadedValue = { value: 'Not loaded', hint: null, missing: true };
      const modelValue = cache.status === 'ready'
        ? entry
          ? settingsEntryUnavailable(entry)
            ? { value: 'Unavailable', hint: null, missing: true }
            : { value: settingsValueLabel(entry.model), hint: null, missing: false }
          : { value: 'Unavailable', hint: null, missing: true }
        : cache.status === 'loading' ? loadingValue : cache.status === 'error' ? errorValue : unloadedValue;
      const effortState = settingsEffortState(agent.id, entry);
      const effortValue = cache.status === 'ready'
        ? {
          value: effortState.kind === 'unavailable' || effortState.kind === 'unsupported'
            ? effortState.value
            : effortState.value,
          hint: effortState.hint,
          missing: effortState.kind === 'unavailable' || effortState.kind === 'unsupported',
        }
        : cache.status === 'loading' ? loadingValue : cache.status === 'error' ? errorValue : unloadedValue;
      const values = [
        ['Default model', modelValue],
        ['Default effort', effortValue],
      ];
      for (const [labelText, state] of values) {
        const field = element('span', state.missing ? 'agent-value agent-value--missing' : 'agent-value');
        field.append(element('span', 'agent-field-label', `${labelText} `), element('span', '', state.value));
        if (state.hint) field.append(element('small', 'agent-value__hint', state.hint));
        row.append(field);
      }
      row.append(element('span', 'agent-arrow', '›'));
      row.addEventListener('click', () => { agentId = agent.id; renderSettings(); });
      return row;
    });
    node('agent-list').replaceChildren(...rows);
    if (focusedAgent) rows.find((row) => row.getAttribute('data-agent') === focusedAgent)?.focus?.();
    node('settings-count').textContent = String(AGENTS.length);
    node('settings-summary').textContent = `${connected} connected ${connected === 1 ? 'agent' : 'agents'} · ${AGENTS.length - connected} unconfirmed`;
    const refresh = node('settings-refresh');
    refresh.disabled = !workerName
      || settingsState(workerName).status === 'loading'
      || settingsSaveInFlight(workerName);
    const agent = AGENTS.find((value) => value.id === agentId) ?? AGENTS[0];
    renderDetailStatic(worker, agent);

    const defaults = snapshot.project_defaults;
    node('project-defaults').replaceChildren(...(defaults ? [
      ['Default agent', AGENTS.find((value) => value.id === defaults.default_agent)?.name ?? defaults.default_agent],
      ['Task timeout', `${defaults.timeout_seconds / 60} min`],
      ['Max follow-ups', defaults.max_followups],
      ['Source', humanize(defaults.source)],
      ['Publication', defaults.publish?.map(humanize).join(' · ') || 'None'],
      ['Environment profile', defaults.env_profile ?? 'None'],
    ].map(([label, value]) => {
      const field = element('div', 'default-value');
      field.append(element('span', '', label), element('strong', '', value));
      return field;
    }) : [element('p', 'empty-state', 'Unavailable · Project defaults could not be loaded from the dashboard launch directory.') ]));
    node('agent-worker-title').textContent = `${agent.name} across workers`;
    node('agent-workers').replaceChildren(...(workers.length ? workers.map((other) => {
      const row = element('button', 'agent-worker');
      row.setAttribute('type', 'button');
      row.setAttribute('aria-pressed', String(other.name === workerName));
      const otherTask = latestTask(other, agentId);
      row.append(element('span', '', other.name), connectionNode(agentConnection(other, agentId, effectiveProfile(otherTask))));
      row.addEventListener('click', () => {
        workerName = other.name;
        renderSettings();
        ensureSettingsForSelection();
      });
      return row;
    }) : [element('p', 'empty-state', 'No workers configured.') ]));
  };

  const loadAgentSettings = (targetWorker, { force = false } = {}) => {
    if (!targetWorker) return Promise.resolve();
    const current = settingsCache.get(targetWorker);
    if (settingsSaveInFlight(targetWorker)) return current?.promise ?? Promise.resolve(current?.data);
    if (!force && current?.status === 'ready') return Promise.resolve(current.data);
    if (!force && current?.status === 'loading') return current.promise ?? Promise.resolve();
    const epoch = settingsEpochs.get(targetWorker) ?? 0;
    const token = (settingsRequestTokens.get(targetWorker) ?? 0) + 1;
    settingsRequestTokens.set(targetWorker, token);
    settingsCache.set(targetWorker, {
      status: 'loading',
      data: current?.data ?? null,
      error: null,
    });
    if (workerName === targetWorker) renderSettings();
    const promise = (async () => {
      const stillCurrent = () => settingsRequestTokens.get(targetWorker) === token
        && (settingsEpochs.get(targetWorker) ?? 0) === epoch
        && !settingsSaveInFlight(targetWorker);
      try {
        const data = await fetchAgentSettings(targetWorker);
        if (!stillCurrent()) return data;
        validateAgentSettingsList(data);
        settingsCache.set(targetWorker, { status: 'ready', data, error: null });
        return data;
      } catch (error) {
        if (stillCurrent()) {
          settingsCache.set(targetWorker, {
            status: 'error',
            data: current?.data ?? null,
            error,
          });
        }
        return null;
      } finally {
        if (workerName === targetWorker && stillCurrent()) renderSettings();
      }
    })();
    const loading = settingsCache.get(targetWorker);
    if (loading) loading.promise = promise;
    return promise;
  };

  const saveCurrentSettings = async (targetWorker, targetAgent) => {
    const key = settingsKeyFor(targetWorker, targetAgent);
    const entry = settingsEntry(targetWorker, targetAgent);
    const draft = settingsDraft(targetWorker, targetAgent, entry);
    if (!entry?.writable || !entry.revision || !draft.dirty) return;
    const token = (settingsSaveTokens.get(key) ?? 0) + 1;
    settingsSaveTokens.set(key, token);
    bumpSettingsEpoch(targetWorker);
    settingsRequestTokens.set(targetWorker, (settingsRequestTokens.get(targetWorker) ?? 0) + 1);
    settingsSaveCounts.set(targetWorker, (settingsSaveCounts.get(targetWorker) ?? 0) + 1);
    settingsMutations.set(key, { saving: true, error: null });
    settingsNotices.delete(key);
    if (workerName === targetWorker && agentId === targetAgent) renderSettings();
    try {
      const saved = validateAgentSettingsEntry(await saveAgentSettings(targetWorker, {
        agent: targetAgent,
        model: draft.model,
        effort: draft.effort,
        revision: entry.revision,
      }), targetAgent, { requireRevision: true });
      if (settingsSaveTokens.get(key) !== token || saved?.agent !== targetAgent) return;
      const cache = settingsCache.get(targetWorker);
      const agents = [...(cache?.data?.agents ?? [])];
      const index = agents.findIndex((value) => value.agent === targetAgent);
      if (index >= 0) agents[index] = saved;
      else agents.push(saved);
      settingsCache.set(targetWorker, { status: 'ready', data: { agents }, error: null });
      settingsDrafts.delete(key);
      settingsMutations.delete(key);
      settingsNotices.delete(key);
      releaseSettingsSave(targetWorker);
    } catch (error) {
      if (settingsSaveTokens.get(key) !== token) return;
      const cache = settingsCache.get(targetWorker);
      if (cache?.data) settingsCache.set(targetWorker, { status: 'ready', data: cache.data, error: null });
      settingsMutations.set(key, { saving: false, error: settingsErrorText(error) });
      releaseSettingsSave(targetWorker);
      if (error?.code === 'SETTINGS_CONFLICT' || error?.status === 409) {
        settingsNotices.set(key, 'Native settings changed. Your draft is still here; fresh settings were loaded so you can retry.');
        await loadAgentSettings(targetWorker, { force: true });
      }
    }
    if (workerName === targetWorker && agentId === targetAgent) renderSettings();
  };

  renderSettings = () => {
    if (!snapshot) return;
    const workers = snapshot.workers ?? [];
    if (!workers.some((worker) => worker.name === workerName)) {
      const worker = workers.find((entry) => entry.active_task && entry.freshness === 'current') ?? workers[0];
      workerName = worker?.name ?? null;
      if (AGENTS.some((agent) => agent.id === worker?.active_task?.agent)) agentId = worker.active_task.agent;
    }
    rebuildSettingsList(workers);
  };
  ensureSettingsForSelection = () => {
    if (!snapshot || !workerName) return Promise.resolve();
    const current = settingsCache.get(workerName);
    if (current?.status === 'ready') return Promise.resolve(current.data);
    if (current?.status === 'loading') return current.promise ?? Promise.resolve();
    return loadAgentSettings(workerName);
  };
  node('settings-worker').addEventListener('change', () => {
    workerName = node('settings-worker').value;
    renderSettings();
    return ensureSettingsForSelection();
  });
  node('settings-refresh').addEventListener('click', () => loadAgentSettings(workerName, { force: true }));

  const renderSnapshot = (value) => {
    snapshot = value;
    const workers = snapshot.workers ?? [];
    const current = workers.filter((worker) => worker.freshness === 'current');
    const stale = workers.filter((worker) => worker.freshness === 'stale').length;
    const offline = workers.filter((worker) => worker.freshness === 'offline').length;
    node('pool-summary').textContent = `Your personal pool · ${current.length} current ${current.length === 1 ? 'worker' : 'workers'}${stale ? ` · ${stale} stale ${stale === 1 ? 'observation' : 'observations'}` : ''}${offline ? ` · ${offline} offline` : ''}`;
    const occupied = current.filter((worker) => worker.slot?.state === 'busy').length;
    const capacity = workers.reduce((total, worker) => total + (worker.slot?.capacity ?? 1), 0);
    node('slots-summary').textContent = `${String(occupied).padStart(2, '0')} / ${String(capacity).padStart(2, '0')}`;
    node('slots-summary').setAttribute('title', 'Occupied slots confirmed by current observations');
    const progress = snapshot.progress ?? {};
    node('tasks-summary').textContent = String(progress.total ?? snapshot.tasks?.length ?? 0).padStart(2, '0');
    node('queue-count').textContent = String(snapshot.queue?.length ?? 0);
    const waiting = snapshot.tasks?.filter((task) => task.state === 'open' || task.state === 'lost').length ?? 0;
    node('queue-attention').textContent = waiting ? `${waiting} ${waiting === 1 ? 'task needs' : 'tasks need'} attention · See Tasks` : 'No tasks need attention';
    const content = element('div', 'progress-content');
    const track = element('div', 'progress-bar');
    const meter = element('progress');
    meter.setAttribute('max', String(Math.max(1, progress.total ?? 0)));
    meter.setAttribute('value', String(progress.closed ?? 0));
    meter.setAttribute('aria-label', 'Closed tasks');
    track.append(meter);
    const legend = element('div', 'progress-legend');
    for (const [label, count] of [['closed', progress.closed], ['active', progress.active], ['queued', progress.queued], ['open', progress.open]]) {
      const item = element('span');
      item.append(element('strong', '', count ?? 0), element('span', '', ` ${label}`));
      legend.append(item);
    }
    content.append(track, legend);
    node('overview-progress').replaceChildren(content);
    renderSettings();
    if (page === 'settings') ensureSettingsForSelection();
  };
  const updateLogEmpty = (detail) => {
    const empty = node('inspector-empty');
    empty.hidden = node('task-stdout-log').textContent.length > 0;
    empty.textContent = detail?.task?.state === 'active' ? 'Waiting for output…' : 'No standard output recorded for this turn.';
  };
  const renderTask = (detail) => {
    const task = detail.task;
    node('inspector-label').textContent = `${humanize(task.state)} turn · ${task.worker ?? 'Unassigned'} · Turn ${task.turn_count ?? 0}`;
    node('inspector-title').textContent = task.title ?? 'Untitled task';
    node('inspector-meta').replaceChildren(...[['Agent', task.agent], ['Model', task.model], ['Effort', task.effort]].map(([label, value]) => {
      const field = element('div');
      field.append(element('span', '', label), element('strong', '', value ?? 'Not reported'));
      return field;
    }));
    node('inspector-runner').textContent = `Runner ${humanize(task.runner).toLowerCase()} · ${humanize(task.freshness).toLowerCase()}`;
    node('inspector-branch').textContent = task.branch ?? 'No branch recorded';
    node('inspector-run').textContent = task.run_id ? `Run ${shortId(task.run_id)} ↗` : 'View run history ↗';
    updateLogEmpty(detail);
  };
  return {
    renderSnapshot, renderTask, updateLogEmpty, selectPage, page: () => page,
    loading() {
      selectTab('stdout');
      node('inspector-label').textContent = 'Task output';
      node('inspector-title').textContent = 'Loading task…';
      node('inspector-meta').replaceChildren();
      node('inspector-runner').textContent = 'Loading';
      node('inspector-branch').textContent = 'Loading task record…';
      node('inspector-run').textContent = 'View run history ↗';
      node('inspector-empty').hidden = false;
      node('inspector-empty').textContent = 'Loading task output…';
    },
    error(message) {
      node('inspector-title').textContent = 'Task unavailable';
      node('inspector-runner').textContent = 'Unavailable';
      node('inspector-empty').textContent = message;
    },
    clearTask() {
      node('inspector-label').textContent = 'Task output';
      node('inspector-title').textContent = 'Select a task to inspect';
      node('inspector-meta').replaceChildren();
      node('inspector-runner').textContent = 'No task selected';
      node('inspector-branch').textContent = 'No branch selected';
      node('inspector-run').textContent = 'View run history ↗';
      node('inspector-empty').hidden = false;
      node('inspector-empty').textContent = 'Select an active worker or a task from the Tasks tab to view its output.';
      node('task-detail').replaceChildren(element('p', 'empty-state', 'Select a task to inspect its record.'));
      node('task-timeline').replaceChildren();
    },
  };
}

if (typeof window !== 'undefined' && typeof document !== 'undefined') {
  createDashboardClient({
    document,
    fetch: window.fetch.bind(window),
    timers: window,
  }).start();
}
