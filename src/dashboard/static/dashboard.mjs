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
    ].map((id) => [id, requiredNode(document, id)]),
  );

  let currentJob = null;
  let stdoutOffset = 0;
  let stderrOffset = 0;
  let stdoutDecoder = new TextDecoder();
  let stderrDecoder = new TextDecoder();
  let stdoutDecoderFlushed = false;
  let stderrDecoderFlushed = false;
  let snapshotTimer = null;
  let logTimer = null;
  let logRefreshPromise = null;
  let started = false;
  let selectionGeneration = 0;
  let lastAppliedSnapshotRevision = null;

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
    body.append(
      element('p', 'ticket__id', `Job ${shortId(entry.job_id)}`),
      element('h3', 'ticket__title', projectLabel(entry)),
      element('p', 'ticket__meta', `${commandSummary(entry.command_summary)} · queued ${formatAge(entry.created_at_millis)}`),
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

  const renderSnapshot = (snapshot) => {
    renderCollection(nodes['worker-grid'], snapshot.workers, renderWorker, 'No workers configured.');
    renderCollection(nodes['queue-list'], snapshot.queue, renderQueueEntry, 'Queue clear — no jobs waiting.');
    renderCollection(nodes['active-jobs'], snapshot.active_jobs, (job) => renderJob(job, false), 'No jobs in flight.');
    renderCollection(nodes['recent-jobs'], snapshot.recent_jobs, (job) => renderJob(job, true), 'No retained job history.');

    const refreshedCurrentJob = currentJob
      ? [...(snapshot.active_jobs ?? []), ...(snapshot.recent_jobs ?? [])]
        .find((job) => job.job_id === currentJob.job_id)
      : null;
    if (refreshedCurrentJob) {
      currentJob = refreshedCurrentJob;
      renderJobDetail(currentJob);
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
    currentJob = null;
    resetLogs();
    nodes['job-detail'].replaceChildren(element('p', 'empty-state', 'Loading job record…'));
    try {
      const selectedJob = await api(`/api/v1/jobs/${encodeURIComponent(jobId)}`);
      if (requestGeneration !== selectionGeneration) return;
      currentJob = selectedJob;
      renderJobDetail(currentJob);
      if (started && logTimer == null) {
        logTimer = timers.setInterval(refreshLogs, LOG_INTERVAL_MILLIS);
      }
    } catch (error) {
      if (requestGeneration !== selectionGeneration) return;
      nodes['job-detail'].replaceChildren(
        element('p', 'empty-state empty-state--error', `Job unavailable — ${errorMessage(error)}`),
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
        if (logTimer != null) timers.clearInterval(logTimer);
        logTimer = null;
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
    logTimer = timers.setInterval(refreshLogs, LOG_INTERVAL_MILLIS);
  };

  return {
    refreshSnapshot,
    openJob,
    refreshLogs,
    start,
    offsets: () => ({ stdout: stdoutOffset, stderr: stderrOffset }),
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
