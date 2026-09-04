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

  const api = async (path) => {
    const response = await fetch(path, { cache: 'no-store' });
    const body = await response.json();
    if (!response.ok) {
      throw new Error(body?.error?.message ?? `Request failed (${response.status})`);
    }
    return body;
  };

  const renderWorker = (worker) => {
    const card = element('article', 'machine-card');
    card.setAttribute('data-state', worker.freshness);

    const heading = element('header', 'machine-card__header');
    const identity = element('div', 'machine-card__identity');
    const stateLine = element('div', 'machine-card__state');
    const led = element('span', 'state-led');
    led.setAttribute('aria-hidden', 'true');
    stateLine.append(led, element('span', 'state-label', humanize(worker.freshness)));
    identity.append(
      element('p', 'eyebrow', worker.hostname ?? 'Host unavailable'),
      element('h3', 'machine-card__name', worker.name),
    );
    heading.append(identity, stateLine);

    const slot = worker.slot ?? {};
    const load = element('div', 'machine-load');
    load.append(
      metric('Slot', `${slot.state === 'busy' ? 1 : 0} / ${slot.capacity ?? 1}`, 'metric--hero'),
      metric('CPU', formatPercent(worker.system?.cpu_busy_percent)),
      metric('Memory', humanize(worker.system?.memory_pressure ?? 'warming_up')),
    );

    const details = element('dl', 'machine-facts');
    appendFact(details, 'Disk free', formatDisk(worker.system));
    appendFact(details, 'Swap', formatBytes(worker.system?.swap_used_bytes));
    appendFact(details, 'Observed', formatTimestamp(worker.observed_at_millis));
    appendFact(details, 'Capabilities', formatList(worker.capabilities));
    appendFact(details, 'Missing', formatList(worker.missing_capabilities, 'None'));
    if (slot.active_job_id) appendFact(details, 'Active job', shortId(slot.active_job_id));
    if (worker.active_task) {
      appendFact(details, 'Active task', worker.active_task.title);
      appendFact(details, 'Task agent', worker.active_task.agent);
      appendFact(details, 'Task turn', worker.active_task.turn_number);
      appendFact(
        details,
        'Task started',
        worker.active_task.started_at_millis == null
          ? 'Not recorded'
          : `${formatTimestamp(worker.active_task.started_at_millis)} · ${formatAge(worker.active_task.started_at_millis)}`,
      );
    }
    if (worker.error) appendFact(details, worker.error.code, worker.error.message);

    card.append(heading, load, details);
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
    body.append(
      element('p', 'ticket__id', `${entryLabel} · ${identity}`),
      element('h3', 'ticket__title', projectLabel(entry)),
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
        if (!taskTurnIsActive(currentTask)) stopTaskLogTimer();
      }
    }

    const freshness = snapshot.collection?.freshness ?? 'offline';
    const errorCount = snapshot.collection?.errors?.length ?? 0;
    nodes['refresh-status'].textContent = errorCount
      ? `${humanize(freshness)} · ${errorCount} collection ${errorCount === 1 ? 'issue' : 'issues'}`
      : `${humanize(freshness)} · revision ${snapshot.revision}`;
    nodes['refresh-status'].setAttribute('data-state', freshness);
    nodes['rail-state-indicator'].setAttribute('data-state', freshness);
    nodes['last-updated'].textContent = `Snapshot ${formatTimestamp(snapshot.generated_at_millis)}`;
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
      renderSnapshot(snapshot);
      lastAppliedSnapshotRevision = snapshot.revision;
    } catch (error) {
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
    return !taskTurnIsActive(currentTask);
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
    const decoder = stream === 'stdout' ? taskStdoutDecoder : taskStderrDecoder;
    const pane = stream === 'stdout' ? nodes['task-stdout-log'] : nodes['task-stderr-log'];
    pane.textContent += decoder.decode(bytes, { stream: true });
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
      const inactiveWithoutFinalLengths = !taskTurnIsActive(currentTask)
        && taskFinalLength('stdout') == null
        && taskFinalLength('stderr') == null;
      if (bothStreamsComplete || inactiveWithoutFinalLengths) {
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

  const openTask = async (taskId) => {
    const requestGeneration = ++selectionGeneration;
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
      return;
    }
    const selectedTaskId = String(taskId);
    try {
      const selectedTask = await api(`/api/v1/tasks/${encodeURIComponent(selectedTaskId)}`);
      if (requestGeneration !== selectionGeneration) return;
      if (!selectedTask?.task || String(selectedTask.task.task_id) !== selectedTaskId) {
        throw new Error('Invalid task detail');
      }
      currentTask = selectedTask;
      renderTaskDetail(currentTask);
      renderTaskTimeline(currentTask);
      if (taskTurnIsActive(currentTask)) startTaskLogTimer();
    } catch (error) {
      if (requestGeneration !== selectionGeneration) return;
      nodes['task-detail'].replaceChildren(
        element('p', 'empty-state empty-state--error', `Task unavailable — ${errorMessage(error)}`),
      );
      nodes['task-timeline'].replaceChildren(
        element('p', 'empty-state empty-state--error', `Turn history unavailable — ${errorMessage(error)}`),
      );
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
    if (currentTask && taskTurnIsActive(currentTask)) startTaskLogTimer();
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

if (typeof window !== 'undefined' && typeof document !== 'undefined') {
  createDashboardClient({
    document,
    fetch: window.fetch.bind(window),
    timers: window,
  }).start();
}
