import test from 'node:test';
import assert from 'node:assert/strict';

import { createDashboardClient, agentConnection } from '../src/dashboard/static/dashboard.mjs';

const JOB_A = '0123456789abcdef0123456789abcdef';
const JOB_B = 'fedcba9876543210fedcba9876543210';
const TASK_ACTIVE = 'task-active';
const TASK_QUEUED = 'task-queued';
const TASK_OPEN = 'task-open';
const TASK_CLOSED = 'task-closed';
const TASK_LOST = 'task-lost';
const RUN_ALPHA = 'run-alpha';
const RUN_BETA = 'run-beta';
const MALICIOUS_LABEL = '<img src=x onerror=1>';
const SETTINGS_FORGE = '/api/v1/workers/mini-forge/agent-settings';
const SETTINGS_ANVIL = '/api/v1/workers/mini-anvil/agent-settings';

test('agent connections respect worker freshness and authentication profile', () => {
  const worker = { freshness: 'current', agent_facts: { freshness: 'current', agents: [
    { name: 'cursor', version: '1.0', auth: 'authenticated', auth_by_profile: [
      { profile: 'agents', auth: 'unauthenticated' },
    ] },
  ] } };
  assert.equal(agentConnection(worker, 'cursor', null).label, 'Connected');
  assert.equal(agentConnection(worker, 'cursor', 'agents').label, 'Sign-in needed');
  assert.equal(agentConnection(worker, 'cursor', 'missing').label, 'Unknown');
  assert.equal(agentConnection({ ...worker, freshness: 'stale' }, 'cursor', null).label, 'Unknown');
  worker.agent_facts.freshness = 'stale';
  assert.equal(agentConnection(worker, 'cursor', null).label, 'Unknown');
  assert.equal(agentConnection(null, 'cursor', null).label, 'Unknown');
});

test('navigation and Settings load native defaults without inventing task values', async () => {
  const harness = createHarness(taskSnapshotFixture());
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();
  assert.equal(harness.document.node('page-settings').hidden, false);
  assert.equal(harness.document.node('page-overview').hidden, true);
  assert.match(harness.document.node('agent-list').textContent, /Codex/);
  assert.match(harness.document.node('agent-detail').textContent, /Not reported/);
  assert.match(harness.document.node('project-defaults').textContent, /Unavailable/);
  assert.ok(harness.fetchCalls.some(({ path }) => path === SETTINGS_FORGE));
  await harness.document.node('nav-tasks').click();
  assert.equal(harness.document.node('page-tasks').hidden, false);
  assert.equal(harness.document.node('nav-tasks').getAttribute('aria-current'), 'page');
});

test('Settings renders native defaults and keeps a draft through snapshot polls', async () => {
  const snapshot = taskSnapshotFixture();
  const settings = agentSettingsFixture({
    codex: { model: 'gpt-5.6-sol', effort: 'high' },
  });
  const harness = fakeEnvironment({ snapshot, settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  assert.match(harness.document.node('agent-list').textContent, /Default model gpt-5\.6-sol/);
  assert.match(harness.document.node('agent-detail').textContent, /Native default/);
  const modelInput = findByTestId(harness.document.node('agent-detail'), 'agent-settings-model');
  assert.equal(modelInput.value, 'gpt-5.6-sol');
  const originalInput = modelInput;
  modelInput.value = 'draft-model';
  modelInput.dispatchEvent(new Event('input'));
  assert.match(harness.document.node('agent-list').textContent, /gpt-5\.6-sol/);
  assert.doesNotMatch(harness.document.node('agent-list').textContent, /draft-model/);

  snapshot.revision += 1;
  await client.refreshSnapshot();

  assert.equal(findByTestId(harness.document.node('agent-detail'), 'agent-settings-model'), originalInput);
  assert.equal(modelInput.value, 'draft-model');
  assert.match(harness.document.node('agent-detail').textContent, /Latest task/);
});

test('Cancel restores the cached native defaults without saving', async () => {
  const settings = agentSettingsFixture({ codex: { model: 'gpt-5.6-sol', effort: 'high' } });
  const harness = fakeEnvironment({ settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  const modelInput = findByTestId(harness.document.node('agent-detail'), 'agent-settings-model');
  modelInput.value = 'draft-model';
  modelInput.dispatchEvent(new Event('input'));
  await findByTestId(harness.document.node('agent-detail'), 'agent-settings-cancel').click();

  assert.equal(findByTestId(harness.document.node('agent-detail'), 'agent-settings-model').value, 'gpt-5.6-sol');
  assert.equal(harness.fetchCalls.filter(({ options }) => options?.method === 'POST').length, 0);
});

test('Saving native defaults sends the explicit payload and protected headers', async () => {
  const settings = agentSettingsFixture({ codex: { model: 'gpt-5.6-sol', effort: 'high' } });
  const harness = fakeEnvironment({ settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  const effort = findByTestId(detail, 'agent-settings-effort');
  modelInput.value = 'gpt-5.6-pro';
  modelInput.dispatchEvent(new Event('input'));
  effort.value = 'max';
  effort.dispatchEvent(new Event('change'));
  await findByTestId(detail, 'agent-settings-save').click();

  const save = harness.fetchCalls.find(({ options }) => options?.method === 'POST');
  assert.ok(save);
  assert.equal(save.path, SETTINGS_FORGE);
  assert.deepEqual(save.options.headers, {
    'Content-Type': 'application/json',
    'X-Mac-Worker-Settings': '1',
    Origin: 'http://127.0.0.1:59623',
  });
  assert.deepEqual(JSON.parse(save.options.body), {
    agent: 'codex',
    model: 'gpt-5.6-pro',
    effort: 'max',
    revision: settings.agents[0].revision,
  });
  assert.equal(findByTestId(detail, 'agent-settings-model').value, 'gpt-5.6-pro');
});

test('Settings shows loading and failure states while keeping retry available', async () => {
  const pending = deferred();
  const harness = fakeEnvironment({ deferredResponses: { [SETTINGS_FORGE]: [pending.promise] } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  const opening = harness.document.node('nav-settings').click();
  assert.match(harness.document.node('agent-detail').textContent, /Loading native defaults/);
  pending.resolve(response(agentSettingsFixture(), true));
  await opening;

  const failed = fakeEnvironment({
    settingsRouteResponses: { [SETTINGS_FORGE]: [response({ error: { code: 'SETTINGS_UNAVAILABLE', message: 'worker settings unavailable' } }, false, 503)] },
  });
  const failedClient = createDashboardClient(failed);
  await failedClient.refreshSnapshot();
  await failed.document.node('nav-settings').click();
  assert.match(failed.document.node('agent-detail').textContent, /Load failed/);
  assert.match(failed.document.node('agent-detail').textContent, /worker settings unavailable/);
  assert.equal(failed.document.node('settings-refresh').disabled, false);
});

test('a failed refresh with cached data keeps the draft but disables the editor', async () => {
  const initial = agentSettingsFixture({ codex: { model: 'initial-model' } });
  const harness = fakeEnvironment({
    settingsByWorker: { 'mini-forge': initial },
    settingsRouteResponses: {
      [SETTINGS_FORGE]: [
        response(initial),
        response({ error: { code: 'SETTINGS_UNAVAILABLE', message: 'refresh unavailable' } }, false, 503),
        response(initial),
      ],
    },
  });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'draft-model';
  modelInput.dispatchEvent(new Event('input'));
  await harness.document.node('settings-refresh').click();

  assert.match(detail.textContent, /Load failed/);
  assert.match(detail.textContent, /refresh unavailable/);
  assert.equal(findByTestId(detail, 'agent-settings-model'), undefined);
  assert.equal(harness.document.node('settings-refresh').disabled, false);
  const codexRow = findByTag(harness.document.node('agent-list'), 'BUTTON')
    .find((row) => row.getAttribute('data-agent') === 'codex');
  assert.match(codexRow.textContent, /Load failed/);

  await harness.document.node('settings-refresh').click();
  assert.equal(findByTestId(detail, 'agent-settings-model').value, 'draft-model');
});

test('conflicts retain the draft and refresh its revision before retry', async () => {
  const initial = agentSettingsFixture({ codex: { model: 'gpt-5.6-sol', effort: 'high' } });
  const refreshed = agentSettingsFixture({ codex: { model: 'gpt-5.6-codex', effort: 'high' } });
  const harness = fakeEnvironment({
    settingsByWorker: { 'mini-forge': initial },
    settingsRouteResponses: {
      [SETTINGS_FORGE]: [response(initial), response(refreshed)],
      [`${SETTINGS_FORGE}#save`]: [response({ error: { code: 'SETTINGS_CONFLICT', message: 'native settings changed; refresh and retry' } }, false, 409)],
    },
  });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();
  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'draft-model';
  modelInput.dispatchEvent(new Event('input'));
  await findByTestId(detail, 'agent-settings-save').click();

  assert.equal(findByTestId(detail, 'agent-settings-model').value, 'draft-model');
  assert.match(detail.textContent, /native settings changed/i);
  const getCalls = harness.fetchCalls.filter(({ path, options }) => path === SETTINGS_FORGE && !options?.method);
  assert.equal(getCalls.length, 2);

  await findByTestId(detail, 'agent-settings-save').click();
  const saves = harness.fetchCalls.filter(({ options }) => options?.method === 'POST');
  assert.equal(saves.length, 2);
  assert.equal(JSON.parse(saves[1].options.body).revision, refreshed.agents[0].revision);
});

test('out-of-order worker settings responses cannot replace the current worker', async () => {
  const forge = deferred();
  const anvil = deferred();
  const harness = fakeEnvironment({
    deferredResponses: { [SETTINGS_FORGE]: [forge.promise], [SETTINGS_ANVIL]: [anvil.promise] },
  });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  const opening = harness.document.node('nav-settings').click();
  harness.nodes['settings-worker'].value = 'mini-anvil';
  harness.nodes['settings-worker'].dispatchEvent(new Event('change'));
  anvil.resolve(response(agentSettingsFixture({ codex: { model: 'anvil-model', effort: null } })));
  await Promise.resolve();
  forge.resolve(response(agentSettingsFixture({ codex: { model: 'forge-model', effort: null } })));
  await opening;
  await Promise.resolve();

  assert.equal(harness.nodes['settings-worker'].value, 'mini-anvil');
  assert.equal(findByTestId(harness.document.node('agent-detail'), 'agent-settings-model').value, 'anvil-model');
  assert.doesNotMatch(harness.document.node('agent-detail').textContent, /forge-model/);
});

test('a late save response cannot replace a newly selected agent', async () => {
  const saveResponse = deferred();
  const harness = fakeEnvironment({
    settingsByWorker: { 'mini-forge': agentSettingsFixture() },
    settingsRouteResponses: { [`${SETTINGS_FORGE}#save`]: [saveResponse.promise] },
  });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();
  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'codex-draft';
  modelInput.dispatchEvent(new Event('input'));
  const saving = findByTestId(detail, 'agent-settings-save').click();
  const agentRows = findByTag(harness.document.node('agent-list'), 'BUTTON');
  await agentRows[1].click();
  saveResponse.resolve(response({
    ...agentSettingsFixture().agents[0],
    model: 'codex-saved',
  }));
  await saving;

  assert.match(detail.textContent, /Cursor/);
  assert.equal(findByTestId(detail, 'agent-settings-model').value, 'claude-3-7-sonnet');
  assert.doesNotMatch(detail.textContent, /codex-saved/);
});

test('Settings distinguishes missing agents, unsupported OpenCode effort, and unverified effort', async () => {
  const settings = agentSettingsFixture();
  settings.agents = settings.agents.filter((entry) => entry.agent !== 'claude');
  const harness = fakeEnvironment({ settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  const rows = findByTag(harness.document.node('agent-list'), 'BUTTON');
  const cursorRow = rows.find((row) => row.getAttribute('data-agent') === 'cursor');
  const opencodeRow = rows.find((row) => row.getAttribute('data-agent') === 'opencode');
  const claudeRow = rows.find((row) => row.getAttribute('data-agent') === 'claude');
  assert.match(cursorRow.textContent, /high/);
  assert.match(cursorRow.textContent, /Choices unavailable/);
  assert.match(opencodeRow.textContent, /Not supported/);
  assert.match(claudeRow.textContent, /Unavailable/);
  assert.doesNotMatch(claudeRow.textContent, /CLI default/);

  await opencodeRow.click();
  assert.match(harness.document.node('agent-detail').textContent, /Not supported/);
});

test('Cursor can clear an unverified effort before changing its model', async () => {
  const settings = agentSettingsFixture({
    cursor: { model: 'grok-4.6', effort: 'high', effort_options: [] },
  });
  const harness = fakeEnvironment({ settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();
  const cursorRow = findByTag(harness.document.node('agent-list'), 'BUTTON')
    .find((row) => row.getAttribute('data-agent') === 'cursor');
  await cursorRow.click();

  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'gpt-5';
  modelInput.dispatchEvent(new Event('input'));
  await findByTestId(detail, 'agent-settings-effort-reset').click();
  assert.match(detail.textContent, /CLI default/);
  await findByTestId(detail, 'agent-settings-save').click();

  const save = harness.fetchCalls.find(({ options }) => options?.method === 'POST');
  assert.deepEqual(JSON.parse(save.options.body), {
    agent: 'cursor',
    model: 'gpt-5',
    effort: null,
    revision: settings.agents.find((entry) => entry.agent === 'cursor').revision,
  });
});

test('Use CLI default stays visible through polling and a save error, then Cancel restores the saved value', async () => {
  const snapshot = taskSnapshotFixture();
  const settings = agentSettingsFixture({
    cursor: { model: 'grok-4.6', effort: 'high', effort_options: [] },
  });
  const harness = fakeEnvironment({
    snapshot,
    settingsByWorker: { 'mini-forge': settings },
    settingsRouteResponses: {
      [`${SETTINGS_FORGE}#save`]: [response({ error: { code: 'SETTINGS_UNAVAILABLE', message: 'save unavailable' } }, false, 503)],
    },
  });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();
  const cursorRow = findByTag(harness.document.node('agent-list'), 'BUTTON')
    .find((row) => row.getAttribute('data-agent') === 'cursor');
  await cursorRow.click();

  const detail = harness.document.node('agent-detail');
  assert.equal(findByTestId(detail, 'agent-settings-effort').value, 'high');
  await findByTestId(detail, 'agent-settings-effort-reset').click();
  assert.equal(findByTestId(detail, 'agent-settings-effort').value, 'CLI default');
  assert.match(cursorRow.textContent, /high/);

  snapshot.revision += 1;
  await client.refreshSnapshot();
  assert.equal(findByTestId(detail, 'agent-settings-effort').value, 'CLI default');

  await findByTestId(detail, 'agent-settings-save').click();
  assert.equal(findByTestId(detail, 'agent-settings-effort').value, 'CLI default');
  assert.equal(findByTestId(detail, 'agent-settings-save').disabled, false);
  assert.match(detail.textContent, /save unavailable/);

  await findByTestId(detail, 'agent-settings-cancel').click();
  assert.equal(findByTestId(detail, 'agent-settings-effort').value, 'high');
});

test('Codex can clear an unverified effort before changing its model', async () => {
  const settings = agentSettingsFixture({
    codex: { model: 'grok-4.6', effort: 'high', effort_options: [] },
  });
  const harness = fakeEnvironment({ settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'gpt-5';
  modelInput.dispatchEvent(new Event('input'));
  await findByTestId(detail, 'agent-settings-effort-reset').click();
  await findByTestId(detail, 'agent-settings-save').click();

  const save = harness.fetchCalls.find(({ options }) => options?.method === 'POST');
  assert.deepEqual(JSON.parse(save.options.body), {
    agent: 'codex',
    model: 'gpt-5',
    effort: null,
    revision: settings.agents.find((entry) => entry.agent === 'codex').revision,
  });
});

test('readonly unavailable entries never look like CLI defaults', async () => {
  const settings = agentSettingsFixture({
    codex: { model: null, effort: null, writable: false, message: 'native configuration is invalid' },
  });
  const harness = fakeEnvironment({ settingsByWorker: { 'mini-forge': settings } });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();

  const codexRow = findByTag(harness.document.node('agent-list'), 'BUTTON')
    .find((row) => row.getAttribute('data-agent') === 'codex');
  assert.match(codexRow.textContent, /Unavailable/);
  assert.doesNotMatch(codexRow.textContent, /CLI default/);
  await codexRow.click();
  assert.match(harness.document.node('agent-detail').textContent, /Unavailable/);
});

test('malformed settings responses fail safely and retain the draft', async () => {
  const malformedGet = fakeEnvironment({
    settingsRouteResponses: {
      [SETTINGS_FORGE]: [response({ agents: [{ agent: 'codex', model: null }] })],
    },
  });
  const malformedGetClient = createDashboardClient(malformedGet);
  await malformedGetClient.refreshSnapshot();
  await malformedGet.document.node('nav-settings').click();
  assert.match(malformedGet.document.node('agent-detail').textContent, /Load failed/);

  const malformedSave = fakeEnvironment({
    settingsRouteResponses: {
      [`${SETTINGS_FORGE}#save`]: [response({ agent: 'codex' })],
    },
  });
  const malformedSaveClient = createDashboardClient(malformedSave);
  await malformedSaveClient.refreshSnapshot();
  await malformedSave.document.node('nav-settings').click();
  const detail = malformedSave.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'draft-model';
  modelInput.dispatchEvent(new Event('input'));
  await findByTestId(detail, 'agent-settings-save').click();
  assert.equal(findByTestId(detail, 'agent-settings-model').value, 'draft-model');
  assert.equal(findByTestId(detail, 'agent-settings-save').disabled, false);
  assert.match(detail.textContent, /response was invalid/i);
});

test('a refresh already in flight cannot overwrite a successful save', async () => {
  const initial = agentSettingsFixture({ codex: { model: 'initial-model' } });
  const refreshResponse = deferred();
  const saved = {
    ...initial.agents.find((entry) => entry.agent === 'codex'),
    model: 'saved-model',
    revision: 's'.repeat(64),
  };
  const harness = fakeEnvironment({
    settingsRouteResponses: {
      [SETTINGS_FORGE]: [response(initial), refreshResponse.promise],
      [`${SETTINGS_FORGE}#save`]: [response(saved)],
    },
  });
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  await harness.document.node('nav-settings').click();
  const detail = harness.document.node('agent-detail');
  const modelInput = findByTestId(detail, 'agent-settings-model');
  modelInput.value = 'saved-model';
  modelInput.dispatchEvent(new Event('input'));
  const refreshing = harness.document.node('settings-refresh').click();
  await Promise.resolve();
  const saving = findByTestId(detail, 'agent-settings-save').click();
  await saving;
  refreshResponse.resolve(response(initial));
  await refreshing;

  assert.equal(findByTestId(detail, 'agent-settings-model').value, 'saved-model');
});

test('task filters are client-side and never call a mutating endpoint', async () => {
  const harness = createHarness(taskSnapshotFixture());
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  harness.nodes['task-filter-state'].value = 'active';
  harness.nodes['task-filter-state'].dispatchEvent(new Event('change'));
  assert.deepEqual(harness.visibleTaskIds(), [TASK_ACTIVE]);
  assert.ok(harness.fetchCalls.every(({ path, options }) =>
    path.startsWith('/api/v1/') && (!options?.method || options.method === 'GET')));
});

test('task detail renders result and normalized timeline as text', async () => {
  const harness = createHarness(taskSnapshotFixture(), {
    [`/api/v1/tasks/${TASK_ACTIVE}`]: taskDetailFixture(MALICIOUS_LABEL),
  });
  const client = createDashboardClient(harness);
  await client.openTask(TASK_ACTIVE);
  assert.match(harness.nodes['task-detail'].textContent, /worker task fetch task-active/);
  assert.match(harness.nodes['task-detail'].textContent, /<img src=x onerror=1>/);
  assert.match(harness.nodes['task-detail'].textContent, /prompt-like fixture text/);
  assert.match(harness.nodes['task-detail'].textContent, /\[path\]/);
  assert.doesNotMatch(harness.nodes['task-detail'].textContent, /production-secret-profile|\/Users\/alice|deadbeef/);
  assert.match(harness.nodes['task-timeline'].textContent, /Turn 1/);
  assert.equal(harness.nodes['task-detail'].querySelector('img'), null);
});

test('active task logs poll every second with independent byte cursors', async () => {
  const harness = createHarness(taskSnapshotFixture(), taskLogResponses());
  const client = createDashboardClient(harness);
  client.start();
  await client.openTask(TASK_ACTIVE);
  await harness.timers.tick(1_000);
  await harness.timers.tick(1_000);
  assert.deepEqual(client.taskOffsets(), { stdout: 10, stderr: 10 });
  assert.equal(harness.nodes['task-stdout-log'].textContent, 'out-1out-2');
  assert.equal(harness.nodes['task-stderr-log'].textContent, 'err-1err-2');
});

test('a completed task drains recorded output before stopping its log timer', async () => {
  const detail = taskDetailFixture();
  detail.task.state = 'closed';
  detail.task.active_turn_id = null;
  detail.turns[1].terminal = 'succeeded';
  const harness = createHarness(taskSnapshotFixture(), {
    [`/api/v1/tasks/${TASK_ACTIVE}`]: detail,
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`]: logChunk('stdout', 0, 4, 'ZG9uZQ=='),
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stderr&offset=0&limit=65536`]: logChunk('stderr', 0, 0, ''),
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stdout&offset=4&limit=65536`]: logChunk('stdout', 4, 4, ''),
  });
  const client = createDashboardClient(harness);
  client.start();
  await client.openTask(TASK_ACTIVE);
  await harness.timers.tick(1_000);
  await harness.timers.tick(1_000);
  assert.equal(harness.nodes['task-stdout-log'].textContent, 'done');
  assert.deepEqual(client.taskOffsets(), { stdout: 4, stderr: 0 });
  assert.deepEqual([...harness.timers.intervals.values()].filter((timer) => !timer.cleared).map((timer) => timer.delay), [2_000]);
});

test('Settings uses latest task settings for the selected worker and keeps remote labels as text', async () => {
  const snapshot = taskSnapshotFixture();
  snapshot.tasks[1] = { ...snapshot.tasks[1], model: MALICIOUS_LABEL, effort: null, permissions: 'full-auto', env_profile: 'agents' };
  snapshot.workers[0].agent_facts = { freshness: 'current', agents: [{ name: 'codex', version: '1.2.3', auth: 'unauthenticated', auth_by_profile: [{ profile: 'agents', auth: 'authenticated' }] }] };
  snapshot.project_defaults = { default_agent: 'codex', timeout_seconds: 1800, max_followups: 10, source: 'local', publish: ['fetch'], env_profile: null, permissions: {} };
  const harness = createHarness(snapshot);
  await createDashboardClient(harness).refreshSnapshot();
  assert.match(harness.nodes['agent-detail'].textContent, /Connected/);
  assert.match(harness.nodes['agent-detail'].textContent, /<img src=x onerror=1>/);
  assert.match(harness.nodes['agent-detail'].textContent, /Not reported/);
  assert.equal(findByTag(harness.document.root, 'IMG').length, 0);
  harness.nodes['settings-worker'].value = 'mini-anvil';
  harness.nodes['settings-worker'].dispatchEvent(new Event('change'));
  assert.match(harness.nodes['agent-detail'].textContent, /Unknown/);
  assert.doesNotMatch(harness.nodes['agent-detail'].textContent, /<img/);
});

test('a failed snapshot makes cached agent connections and worker readings stale', async () => {
  const snapshot = taskSnapshotFixture();
  snapshot.workers[0].agent_facts = { freshness: 'current', agents: [{ name: 'codex', auth: 'authenticated', auth_by_profile: [] }] };
  const env = fakeEnvironment({ deferredResponses: { '/api/v1/snapshot': [
    response(snapshot), response({ error: { message: 'offline' } }, false),
  ] } });
  const client = createDashboardClient(env);
  await client.refreshSnapshot();
  assert.match(env.document.node('agent-detail').textContent, /Connected/);
  await client.refreshSnapshot();
  assert.doesNotMatch(env.document.node('agent-detail').textContent, /Connected/);
  assert.equal(env.document.node('worker-grid').children[0].getAttribute('data-state'), 'stale');
});

test('worker cards show task title and agent without losing active turn identity', async () => {
  const harness = createHarness(taskSnapshotFixture());
  await createDashboardClient(harness).refreshSnapshot();
  assert.match(harness.nodes['worker-grid'].textContent, /Repair login/);
  assert.match(harness.nodes['worker-grid'].textContent, /codex/);
});

test('overview selects an active task once and preserves the user navigation on a new turn', async () => {
  const snapshot = taskSnapshotFixture();
  const detail = taskDetailFixture();
  const harness = createHarness(snapshot, { [`/api/v1/tasks/${TASK_ACTIVE}`]: detail });
  const client = createDashboardClient(harness);
  client.start();
  await client.refreshSnapshot();
  assert.equal(harness.fetchCalls.filter(({ path }) => path === `/api/v1/tasks/${TASK_ACTIVE}`).length, 1);
  await harness.document.node('nav-settings').click();
  detail.task.active_turn_id = 'turn-new';
  detail.task.turn_count = 3;
  detail.turns[1].terminal = 'succeeded';
  detail.turns.push({ turn_id: 'turn-new', turn_number: 3, terminal: null });
  snapshot.tasks[1].active_turn_id = 'turn-new';
  snapshot.tasks[1].turn_count = 3;
  snapshot.revision += 1;
  await client.refreshSnapshot();
  assert.equal(harness.document.node('page-settings').hidden, false);
  assert.match(harness.document.node('inspector-label').textContent, /Turn 3/);
  assert.deepEqual(client.taskOffsets(), { stdout: 0, stderr: 0 });
  await client.refreshTaskLogs();
  assert.ok(harness.fetchCalls.some(({ path }) => path.includes('/turns/turn-new/logs?stream=stdout&offset=0')));
  const detailCalls = harness.fetchCalls.filter(({ path }) => path === `/api/v1/tasks/${TASK_ACTIVE}`).length;
  await client.refreshSnapshot();
  assert.equal(harness.fetchCalls.filter(({ path }) => path === `/api/v1/tasks/${TASK_ACTIVE}`).length, detailCalls);
});

test('startup polls only the snapshot endpoint at the required cadence', async () => {
  const snapshot = snapshotFixture();
  snapshot.tasks = [];
  const env = fakeEnvironment({ snapshot });
  const client = createDashboardClient(env);

  client.start();
  await env.timers.fire(env.timers.idForDelay(2_000));
  await env.timers.fire(env.timers.idForDelay(1_000));

  assert.deepEqual(env.fetch.calls, [
    { path: '/api/v1/snapshot', options: { cache: 'no-store' } },
  ]);
  assert.deepEqual(env.timers.delays(), [2_000, 1_000]);
});

test('renderer populates every region while keeping labels and command data safe', async () => {
  const snapshot = snapshotFixture();
  snapshot.active_jobs[0].project_label = MALICIOUS_LABEL;
  snapshot.active_jobs[0].command = 'DO NOT RENDER --token secret';
  snapshot.active_jobs[0].argv = ['DO', 'NOT', 'RENDER'];
  snapshot.active_jobs[0].shell = 'DO NOT RENDER';
  snapshot.queue[0].project_label = '<svg onload=1>';
  const env = fakeEnvironment({ snapshot });
  const client = createDashboardClient(env);

  await client.refreshSnapshot();
  await client.openJob(JOB_A);

  for (const testId of [
    'worker-grid',
    'queue-list',
    'active-jobs',
    'recent-jobs',
    'job-detail',
    'stdout-log',
    'stderr-log',
  ]) {
    assert.ok(env.document.node(testId).children.length > 0 || testId.endsWith('-log'));
  }
  assert.equal(env.document.node('worker-grid').children.length, 3);
  assert.match(env.document.node('active-jobs').textContent, /<img src=x onerror=1>/);
  assert.match(env.document.node('queue-list').textContent, /<svg onload=1>/);
  assert.equal(findByTag(env.document.root, 'IMG').length, 0);
  assert.equal(findByTag(env.document.root, 'SVG').length, 0);
  assert.match(env.document.node('recent-jobs').textContent, /project-recent-proje\/worktree-recent-workt/);
  assert.match(env.document.node('active-jobs').textContent, /argv \(3 arguments\)/);
  assert.match(env.document.node('queue-list').textContent, /shell/);
  assert.match(env.document.node('queue-list').textContent, /Job fedcba987654/);
  assert.doesNotMatch(env.document.root.textContent, /DO NOT RENDER/);

  const controls = [...findByTag(env.document.node('active-jobs'), 'BUTTON'),
    ...findByTag(env.document.node('recent-jobs'), 'BUTTON')];
  assert.ok(controls.length > 0);
  const accessibleNames = controls.map((control) => {
    assert.equal(control.getAttribute('aria-label'), null);
    assert.equal(control.getAttribute('aria-labelledby'), null);
    return control.textContent;
  });
  assert.equal(new Set(accessibleNames).size, controls.length);
  assert.equal(accessibleNames.some((name) => name.includes(MALICIOUS_LABEL)), true);
  assert.equal(
    accessibleNames.some((name) =>
      name.includes('project-recent-proje/worktree-recent-workt')),
    true,
  );
  for (const control of controls) {
    assert.match(control.textContent, /^Inspect/);
    assert.doesNotMatch(control.textContent, /cancel|retry|delete|submit/i);
  }
  const hostileControl = controls.find((control) => control.textContent.includes(MALICIOUS_LABEL));
  assert.ok(hostileControl);
  assert.equal(
    allNodes(env.document.root).some((node) =>
      [...node.attributes.values()].some((value) => value.includes(MALICIOUS_LABEL))),
    false,
  );
  assert.equal(env.fetch.calls.some(({ path }) => /cancel|retry|delete|submit/i.test(path)), false);
});

test('identical job records have distinct native names from visible short IDs', async () => {
  const firstJob = jobFixture({
    job_id: JOB_A,
    project_label: MALICIOUS_LABEL,
  });
  const secondJob = { ...firstJob, job_id: JOB_B };
  const snapshot = snapshotFixture();
  snapshot.active_jobs = [firstJob, secondJob];
  snapshot.recent_jobs = [];
  const env = fakeEnvironment({ snapshot });
  const client = createDashboardClient(env);

  await client.refreshSnapshot();

  const controls = findByTag(env.document.node('active-jobs'), 'BUTTON');
  assert.equal(controls.length, 2);
  const accessibleNames = controls.map((control) => {
    assert.equal(control.getAttribute('aria-label'), null);
    assert.equal(control.getAttribute('aria-labelledby'), null);
    return control.textContent;
  });
  assert.equal(new Set(accessibleNames).size, 2);
  assert.match(accessibleNames[0], /Job 0123456789ab/);
  assert.match(accessibleNames[1], /Job fedcba987654/);
  assert.equal(accessibleNames.every((name) => name.includes(MALICIOUS_LABEL)), true);
  assert.equal(
    allNodes(env.document.root).some((node) =>
      [...node.attributes.values()].some((value) => value.includes(MALICIOUS_LABEL))),
    false,
  );
});

test('stdout and stderr advance independent byte offsets without duplicate text', async () => {
  const env = fakeEnvironment({
    details: { [JOB_A]: jobFixture({ job_id: JOB_A }) },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'YWJj')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZGU=')],
      [`${JOB_A}:stdout:3`]: [logChunk('stdout', 3, 5, 'Zmc=')],
      [`${JOB_A}:stderr:2`]: [logChunk('stderr', 2, 3, 'aA==')],
    },
  });
  const client = createDashboardClient(env);

  await client.openJob(JOB_A);
  await client.refreshLogs();
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
  await client.refreshLogs();

  assert.deepEqual(
    env.fetch.calls.map(({ path }) => path),
    [
      `/api/v1/jobs/${JOB_A}`,
      `/api/v1/jobs/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`,
      `/api/v1/jobs/${JOB_A}/logs?stream=stderr&offset=0&limit=65536`,
      `/api/v1/jobs/${JOB_A}/logs?stream=stdout&offset=3&limit=65536`,
      `/api/v1/jobs/${JOB_A}/logs?stream=stderr&offset=2&limit=65536`,
    ],
  );
  assert.deepEqual(client.offsets(), { stdout: 5, stderr: 3 });
  assert.equal(env.document.node('stdout-log').textContent, 'abcfg');
  assert.equal(env.document.node('stderr-log').textContent, 'deh');
});

test('overlapping log refreshes share one in-flight fetch and append each stream once', async () => {
  const slowStdout = deferred();
  const stdoutPath = `/api/v1/jobs/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`;
  const env = fakeEnvironment({
    deferredResponses: { [stdoutPath]: [slowStdout.promise] },
    logs: {
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZGU=')],
    },
  });
  const client = createDashboardClient(env);
  await client.openJob(JOB_A);

  const firstRefresh = client.refreshLogs();
  const secondRefresh = client.refreshLogs();
  assert.equal(env.fetch.calls.filter(({ path }) => path === stdoutPath).length, 1);
  slowStdout.resolve(response(logChunk('stdout', 0, 3, 'YWJj')));
  await Promise.all([firstRefresh, secondRefresh]);

  assert.deepEqual(
    env.fetch.calls.map(({ path }) => path),
    [
      `/api/v1/jobs/${JOB_A}`,
      stdoutPath,
      `/api/v1/jobs/${JOB_A}/logs?stream=stderr&offset=0&limit=65536`,
    ],
  );
  assert.equal(env.document.node('stdout-log').textContent, 'abc');
  assert.equal(env.document.node('stderr-log').textContent, 'de');
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
});

test('a rejected shared log refresh releases the mutex for a later retry', async () => {
  const failedStdout = deferred();
  const stdoutPath = `/api/v1/jobs/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`;
  const env = fakeEnvironment({
    deferredResponses: { [stdoutPath]: [failedStdout.promise] },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'YWJj')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZGU=')],
    },
  });
  const client = createDashboardClient(env);
  await client.openJob(JOB_A);

  const firstRefresh = client.refreshLogs();
  const sharedRefresh = client.refreshLogs();
  assert.equal(env.fetch.calls.filter(({ path }) => path === stdoutPath).length, 1);
  failedStdout.reject(new Error('temporary fetch failure'));
  await Promise.all([firstRefresh, sharedRefresh]);
  assert.deepEqual(client.offsets(), { stdout: 0, stderr: 0 });

  await client.refreshLogs();

  assert.equal(env.fetch.calls.filter(({ path }) => path === stdoutPath).length, 2);
  assert.equal(env.document.node('stdout-log').textContent, 'abc');
  assert.match(env.document.node('stderr-log').textContent, /de$/);
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
});

test('selecting another job resets both cursors and both log panes', async () => {
  const env = fakeEnvironment({
    details: {
      [JOB_A]: jobFixture({ job_id: JOB_A, project_label: 'First Job' }),
      [JOB_B]: jobFixture({ job_id: JOB_B, project_label: 'Second Job' }),
    },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'b25l')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 3, 'dHdv')],
    },
  });
  const client = createDashboardClient(env);

  await client.openJob(JOB_A);
  await client.refreshLogs();
  assert.equal(env.document.node('stdout-log').textContent, 'one');
  assert.equal(env.document.node('stderr-log').textContent, 'two');

  await client.openJob(JOB_B);

  assert.deepEqual(client.offsets(), { stdout: 0, stderr: 0 });
  assert.equal(env.document.node('stdout-log').textContent, '');
  assert.equal(env.document.node('stderr-log').textContent, '');
  assert.match(env.document.node('job-detail').textContent, /Second Job/);
});

test('out-of-order detail responses cannot replace the newest selection', async () => {
  const slowA = deferred();
  const fastB = deferred();
  const env = fakeEnvironment({
    deferredResponses: {
      [`/api/v1/jobs/${JOB_A}`]: [slowA.promise],
      [`/api/v1/jobs/${JOB_B}`]: [fastB.promise],
    },
  });
  const client = createDashboardClient(env);

  const selectingA = client.openJob(JOB_A);
  const selectingB = client.openJob(JOB_B);
  fastB.resolve(response(jobFixture({ job_id: JOB_B, project_label: 'Newest Job' })));
  await selectingB;
  slowA.resolve(response(jobFixture({ job_id: JOB_A, project_label: 'Stale Job' })));
  await selectingA;

  assert.match(env.document.node('job-detail').textContent, /Newest Job/);
  assert.doesNotMatch(env.document.node('job-detail').textContent, /Stale Job/);
  assert.deepEqual(client.offsets(), { stdout: 0, stderr: 0 });
  assert.equal(env.document.node('stdout-log').textContent, '');
  assert.equal(env.document.node('stderr-log').textContent, '');
});

test('a log response from an old selection cannot mutate or poll the new job', async () => {
  const oldStdout = deferred();
  const env = fakeEnvironment({
    details: {
      [JOB_A]: jobFixture({ job_id: JOB_A, project_label: 'Old Job' }),
      [JOB_B]: jobFixture({ job_id: JOB_B, project_label: 'New Job' }),
    },
    deferredResponses: {
      [`/api/v1/jobs/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`]: [oldStdout.promise],
    },
    logs: {
      [`${JOB_B}:stdout:0`]: [logChunk('stdout', 0, 3, 'bmV3')],
      [`${JOB_B}:stderr:0`]: [logChunk('stderr', 0, 3, 'ZXJy')],
    },
  });
  const client = createDashboardClient(env);
  await client.openJob(JOB_A);
  const oldRefresh = client.refreshLogs();
  await client.openJob(JOB_B);

  oldStdout.resolve(response(logChunk('stdout', 0, 3, 'b2xk')));
  await oldRefresh;

  assert.deepEqual(
    env.fetch.calls.map(({ path }) => path),
    [
      `/api/v1/jobs/${JOB_A}`,
      `/api/v1/jobs/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`,
      `/api/v1/jobs/${JOB_B}`,
    ],
  );
  assert.deepEqual(client.offsets(), { stdout: 0, stderr: 0 });
  assert.equal(env.document.node('stdout-log').textContent, '');
  assert.equal(env.document.node('stderr-log').textContent, '');

  await client.refreshLogs();
  assert.equal(env.document.node('stdout-log').textContent, 'new');
  assert.equal(env.document.node('stderr-log').textContent, 'err');
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 3 });
});

test('terminal polling stops only after both exact final byte lengths are reached', async () => {
  const terminal = jobFixture({
    job_id: JOB_A,
    state: 'succeeded',
    exit_code: 0,
    final_stdout_bytes: 3,
    final_stderr_bytes: 4,
    artifact_status: 'available',
  });
  const env = fakeEnvironment({
    details: { [JOB_A]: terminal },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'b3V0')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZXI=')],
      [`${JOB_A}:stderr:2`]: [logChunk('stderr', 2, 4, 'cm9y')],
    },
  });
  const client = createDashboardClient(env);
  client.start();
  const logTimer = env.timers.idForDelay(1_000);
  await client.openJob(JOB_A);

  await env.timers.fire(logTimer);
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
  assert.equal(env.timers.cleared(logTimer), false);

  await env.timers.fire(logTimer);
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 4 });
  assert.equal(env.timers.cleared(logTimer), true);
  const callsAtCompletion = env.fetch.calls.length;

  await env.timers.fire(logTimer);
  assert.equal(env.fetch.calls.length, callsAtCompletion);
  assert.equal(
    env.fetch.calls.some(({ path }) => path.includes('stream=stdout&offset=3')),
    false,
  );
});

test('a terminal snapshot refreshes the selected running job and stops completed log polling', async () => {
  const terminalSnapshot = snapshotFixture();
  terminalSnapshot.active_jobs = [];
  terminalSnapshot.recent_jobs = [jobFixture({
    job_id: JOB_A,
    project_label: 'Finished Project',
    state: 'succeeded',
    exit_code: 0,
    final_stdout_bytes: 3,
    final_stderr_bytes: 2,
    artifact_status: 'available',
  })];
  const env = fakeEnvironment({
    snapshot: terminalSnapshot,
    details: {
      [JOB_A]: jobFixture({ job_id: JOB_A, project_label: 'Running Project' }),
    },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'b3V0')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZXI=')],
    },
  });
  const client = createDashboardClient(env);
  client.start();
  const logTimer = env.timers.idForDelay(1_000);
  await client.openJob(JOB_A);

  await env.timers.fire(logTimer);
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
  assert.equal(env.timers.cleared(logTimer), false);
  assert.match(env.document.node('job-detail').textContent, /Running Project/);

  await client.refreshSnapshot();
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
  assert.match(env.document.node('job-detail').textContent, /Finished Project/);
  assert.match(env.document.node('job-detail').textContent, /Succeeded/);

  await env.timers.fire(logTimer);
  assert.equal(env.timers.cleared(logTimer), true);
  const callsAtCompletion = env.fetch.calls.length;

  await env.timers.fire(logTimer);
  assert.equal(env.fetch.calls.length, callsAtCompletion);
  assert.equal(
    env.fetch.calls.some(({ path }) => path.includes('stream=stdout&offset=3')),
    false,
  );
  assert.equal(
    env.fetch.calls.some(({ path }) => path.includes('stream=stderr&offset=2')),
    false,
  );
});

test('an older overlapping snapshot cannot replace a newer terminal revision', async () => {
  const olderResponse = deferred();
  const newerResponse = deferred();
  const olderSnapshot = snapshotFixture();
  olderSnapshot.revision = 43;
  olderSnapshot.collection.freshness = 'stale';
  olderSnapshot.workers = [workerFixture({ name: 'stale-worker' })];
  olderSnapshot.active_jobs = [jobFixture({
    job_id: JOB_A,
    project_label: 'Stale Running Project',
  })];
  olderSnapshot.recent_jobs = [];
  const newerSnapshot = snapshotFixture();
  newerSnapshot.revision = 44;
  newerSnapshot.workers = [workerFixture({ name: 'fresh-worker' })];
  newerSnapshot.active_jobs = [];
  newerSnapshot.recent_jobs = [jobFixture({
    job_id: JOB_A,
    project_label: 'Newest Finished Project',
    state: 'succeeded',
    exit_code: 0,
    final_stdout_bytes: 3,
    final_stderr_bytes: 2,
    artifact_status: 'available',
  })];
  const env = fakeEnvironment({
    details: {
      [JOB_A]: jobFixture({ job_id: JOB_A, project_label: 'Initially Running Project' }),
    },
    deferredResponses: {
      '/api/v1/snapshot': [olderResponse.promise, newerResponse.promise],
    },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'b3V0')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZXI=')],
    },
  });
  const client = createDashboardClient(env);
  client.start();
  const logTimer = env.timers.idForDelay(1_000);
  await client.openJob(JOB_A);
  await env.timers.fire(logTimer);
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });

  const olderRefresh = client.refreshSnapshot();
  const newerRefresh = client.refreshSnapshot();
  newerResponse.resolve(response(newerSnapshot));
  await newerRefresh;

  assert.match(env.document.node('refresh-status').textContent, /Current · revision 44/);
  assert.match(env.document.node('worker-grid').textContent, /fresh-worker/);
  assert.match(env.document.node('recent-jobs').textContent, /Newest Finished Project/);
  assert.match(env.document.node('job-detail').textContent, /Newest Finished Project/);
  assert.match(env.document.node('job-detail').textContent, /Succeeded/);
  await env.timers.fire(logTimer);
  assert.equal(env.timers.cleared(logTimer), true);
  const callsAtTerminal = env.fetch.calls.length;

  olderResponse.resolve(response(olderSnapshot));
  await olderRefresh;

  assert.match(env.document.node('refresh-status').textContent, /Current · revision 44/);
  assert.match(env.document.node('worker-grid').textContent, /fresh-worker/);
  assert.doesNotMatch(env.document.node('worker-grid').textContent, /stale-worker/);
  assert.match(env.document.node('recent-jobs').textContent, /Newest Finished Project/);
  assert.match(env.document.node('job-detail').textContent, /Newest Finished Project/);
  assert.match(env.document.node('job-detail').textContent, /Succeeded/);
  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });

  await client.refreshLogs();
  await env.timers.fire(logTimer);
  assert.equal(env.fetch.calls.length, callsAtTerminal);
  assert.equal(env.timers.cleared(logTimer), true);
});

test('a snapshot missing the selected job preserves its identity and log cursors', async () => {
  const partialSnapshot = snapshotFixture();
  partialSnapshot.active_jobs = [jobFixture({
    job_id: JOB_B,
    project_label: 'Different Project',
  })];
  partialSnapshot.recent_jobs = [];
  const env = fakeEnvironment({
    snapshot: partialSnapshot,
    details: {
      [JOB_A]: jobFixture({ job_id: JOB_A, project_label: 'Selected Project' }),
    },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 3, 'b3V0')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 2, 'ZXI=')],
      [`${JOB_A}:stdout:3`]: [logChunk('stdout', 3, 4, 'IQ==')],
      [`${JOB_A}:stderr:2`]: [logChunk('stderr', 2, 3, 'IQ==')],
    },
  });
  const client = createDashboardClient(env);
  await client.openJob(JOB_A);
  await client.refreshLogs();

  await client.refreshSnapshot();

  assert.deepEqual(client.offsets(), { stdout: 3, stderr: 2 });
  assert.match(env.document.node('job-detail').textContent, /Selected Project/);
  assert.doesNotMatch(env.document.node('job-detail').textContent, /Different Project/);
  await client.refreshLogs();
  assert.deepEqual(client.offsets(), { stdout: 4, stderr: 3 });
  assert.equal(
    env.fetch.calls.some(({ path }) => path.includes(`/jobs/${JOB_B}/logs`)),
    false,
  );
});

test('stream decoders join split UTF-8 and flush incomplete terminal bytes once', async () => {
  const terminal = jobFixture({
    job_id: JOB_A,
    state: 'failed',
    exit_code: 1,
    final_stdout_bytes: 3,
    final_stderr_bytes: 1,
  });
  const env = fakeEnvironment({
    details: { [JOB_A]: terminal },
    logs: {
      [`${JOB_A}:stdout:0`]: [logChunk('stdout', 0, 2, '4oI=')],
      [`${JOB_A}:stdout:2`]: [logChunk('stdout', 2, 3, 'rA==')],
      [`${JOB_A}:stderr:0`]: [logChunk('stderr', 0, 1, '4g==')],
    },
  });
  const client = createDashboardClient(env);
  client.start();
  const logTimer = env.timers.idForDelay(1_000);
  await client.openJob(JOB_A);

  await env.timers.fire(logTimer);
  assert.equal(env.document.node('stdout-log').textContent, '');
  assert.equal(env.document.node('stderr-log').textContent, '');
  assert.equal(env.timers.cleared(logTimer), false);

  await env.timers.fire(logTimer);
  assert.equal(env.document.node('stdout-log').textContent, '€');
  assert.equal(env.document.node('stderr-log').textContent, '�');
  assert.equal(env.timers.cleared(logTimer), true);
  const completedCalls = env.fetch.calls.length;

  await client.refreshLogs();
  assert.equal(env.document.node('stdout-log').textContent, '€');
  assert.equal(env.document.node('stderr-log').textContent, '�');
  assert.equal(env.fetch.calls.length, completedCalls);
});

test('API failures are rendered as literal offline text', async () => {
  const env = fakeEnvironment({
    snapshotError: {
      error: { code: 'DASHBOARD_UNAVAILABLE', message: '<img src=x onerror=alert(1)>' },
    },
  });
  const client = createDashboardClient(env);

  await client.refreshSnapshot();

  assert.match(env.document.node('refresh-status').textContent, /Dashboard unavailable/);
  assert.match(env.document.node('refresh-status').textContent, /<img src=x onerror=alert\(1\)>/);
  assert.equal(findByTag(env.document.root, 'IMG').length, 0);
});

test('top rail indicator mirrors current, stale, and offline collection state', async () => {
  for (const freshness of ['current', 'stale']) {
    const snapshot = snapshotFixture();
    snapshot.collection.freshness = freshness;
    const env = fakeEnvironment({ snapshot });
    const client = createDashboardClient(env);

    await client.refreshSnapshot();

    assert.equal(env.document.node('rail-state-indicator').getAttribute('data-state'), freshness);
  }

  const offlineEnv = fakeEnvironment({
    snapshotError: { error: { code: 'DASHBOARD_UNAVAILABLE', message: 'not reachable' } },
  });
  const offlineClient = createDashboardClient(offlineEnv);
  await offlineClient.refreshSnapshot();
  assert.equal(
    offlineEnv.document.node('rail-state-indicator').getAttribute('data-state'),
    'offline',
  );
});

function fakeEnvironment({
  snapshot = snapshotFixture(),
  snapshotError = null,
  details = { [JOB_A]: jobFixture({ job_id: JOB_A }) },
  taskDetails = { [TASK_ACTIVE]: taskDetailFixture() },
  logs = {},
  deferredResponses = {},
  taskRouteResponses = {},
  settingsByWorker = {},
  settingsRouteResponses = {},
} = {}) {
  const fetchDouble = new FakeFetch({
    snapshot,
    snapshotError,
    details,
    taskDetails,
    logs,
    deferredResponses,
    taskRouteResponses,
    settingsByWorker,
    settingsRouteResponses,
  });
  const fetch = fetchDouble.fetch.bind(fetchDouble);
  fetch.calls = fetchDouble.calls;
  const environment = {
    document: new FakeDocument(),
    fetch,
    timers: new FakeTimers(),
  };
  environment.nodes = Object.fromEntries(
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
      'nav-overview', 'nav-tasks', 'nav-history', 'nav-settings',
      'page-overview', 'page-tasks', 'page-history', 'page-settings',
      'pool-summary', 'slots-summary', 'tasks-summary', 'queue-count', 'queue-attention',
      'overview-progress', 'inspector-label', 'inspector-title', 'inspector-meta',
      'inspector-runner', 'inspector-branch', 'inspector-run', 'inspector-empty',
      'console-stdout', 'console-stderr', 'console-details',
      'tab-stdout', 'tab-stderr', 'tab-details', 'settings-worker', 'settings-summary',
      'agent-list', 'agent-detail', 'project-defaults', 'agent-workers', 'agent-worker-title',
      'settings-count', 'settings-refresh',
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
    ].map((id) => [id, environment.document.node(id)]),
  );
  environment.fetchCalls = fetch.calls;
  environment.visibleTaskIds = () => environment.nodes['task-list'].children
    .filter((node) => node.getAttribute('data-task-id'))
    .map((node) => node.getAttribute('data-task-id'));
  return environment;
}

function createHarness(snapshot, routeOverrides = {}) {
  const taskDetails = {};
  const taskRouteResponses = {};
  for (const [path, value] of Object.entries(routeOverrides)) {
    const detailMatch = path.match(new RegExp(`^/api/v1/tasks/([^/]+)$`));
    if (detailMatch) {
      taskDetails[decodeURIComponent(detailMatch[1])] = value;
    } else {
      taskRouteResponses[path] = [response(value)];
    }
  }
  const environment = fakeEnvironment({
    snapshot,
    taskDetails: Object.keys(taskDetails).length ? taskDetails : undefined,
    taskRouteResponses,
  });
  environment.fetchCalls = environment.fetch.calls;
  return environment;
}

class FakeNode {
  constructor(tagName, ownerDocument) {
    this.tagName = tagName.toUpperCase();
    this.ownerDocument = ownerDocument;
    this.children = [];
    this.attributes = new Map();
    this.events = new Map();
    this._textContent = '';
    this.className = '';
    this.value = '';
    this.disabled = false;
    this.readOnly = false;
  }

  get textContent() {
    return this._textContent + this.children.map((child) => child.textContent).join('');
  }

  set textContent(value) {
    this._textContent = value == null ? '' : String(value);
    this.children = [];
  }

  append(...nodes) {
    for (const node of nodes) {
      assert.ok(node instanceof FakeNode, 'client must append DOM nodes, not HTML strings');
      this.children.push(node);
    }
  }

  replaceChildren(...nodes) {
    this._textContent = '';
    this.children = [];
    this.append(...nodes);
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  }

  querySelector(selector) {
    const wanted = selector.startsWith('.') || selector.startsWith('#')
      ? null
      : selector.toUpperCase();
    return wanted ? findByTag(this, wanted)[0] ?? null : null;
  }

  addEventListener(type, listener) {
    const listeners = this.events.get(type) ?? [];
    listeners.push(listener);
    this.events.set(type, listeners);
  }

  dispatchEvent(event) {
    for (const listener of this.events.get(event.type) ?? []) {
      listener({ currentTarget: this, target: this, preventDefault() {} });
    }
    return true;
  }

  async click() {
    for (const listener of this.events.get('click') ?? []) {
      await listener({ currentTarget: this, preventDefault() {} });
    }
  }
}

class FakeDocument {
  constructor() {
    this.root = new FakeNode('main', this);
    this.nodes = new Map();
    this.defaultView = {
      location: { hash: '', origin: 'http://127.0.0.1:59623' },
      history: { pushState() {} },
      addEventListener() {},
    };
    for (const id of [
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
      'nav-overview', 'nav-tasks', 'nav-history', 'nav-settings',
      'page-overview', 'page-tasks', 'page-history', 'page-settings',
      'pool-summary', 'slots-summary', 'tasks-summary', 'queue-count', 'queue-attention',
      'overview-progress', 'inspector-label', 'inspector-title', 'inspector-meta',
      'inspector-runner', 'inspector-branch', 'inspector-run', 'inspector-empty',
      'console-stdout', 'console-stderr', 'console-details',
      'tab-stdout', 'tab-stderr', 'tab-details', 'settings-worker', 'settings-summary',
      'agent-list', 'agent-detail', 'project-defaults', 'agent-workers', 'agent-worker-title',
      'settings-count', 'settings-refresh',
    ]) {
      const tag = id.endsWith('-log') ? 'pre' : id.startsWith('task-filter-') ? 'select' : 'section';
      const node = new FakeNode(tag, this);
      node.setAttribute('id', id);
      node.setAttribute('data-testid', id);
      this.nodes.set(id, node);
      this.root.append(node);
    }
  }

  createElement(tagName) {
    return new FakeNode(tagName, this);
  }

  getElementById(id) {
    return this.nodes.get(id) ?? null;
  }

  node(id) {
    const node = this.getElementById(id);
    assert.ok(node, `missing fake node ${id}`);
    return node;
  }
}

class FakeFetch {
  constructor({ snapshot, snapshotError, details, taskDetails, logs, deferredResponses, taskRouteResponses, settingsByWorker, settingsRouteResponses }) {
    this.snapshot = snapshot;
    this.snapshotError = snapshotError;
    this.details = details;
    this.taskDetails = taskDetails;
    this.logs = new Map(Object.entries(logs));
    this.deferredResponses = new Map(Object.entries(deferredResponses));
    this.taskRouteResponses = new Map(Object.entries(taskRouteResponses));
    this.settingsByWorker = new Map(Object.entries(settingsByWorker));
    this.settingsRouteResponses = new Map(Object.entries(settingsRouteResponses));
    this.calls = [];
  }

  async fetch(path, options) {
    this.calls.push({ path, options });
    const pending = this.deferredResponses.get(path);
    if (pending?.length) return pending.shift();
    if (path === '/api/v1/snapshot') {
      return response(this.snapshotError ?? this.snapshot, !this.snapshotError);
    }

    const taskRouteResponse = this.taskRouteResponses.get(path);
    if (taskRouteResponse?.length) return taskRouteResponse.shift();

    const settingsMatch = path.match(/^\/api\/v1\/workers\/([^/]+)\/agent-settings$/);
    if (settingsMatch) {
      const worker = decodeURIComponent(settingsMatch[1]);
      const routeKey = options?.method === 'POST' ? `${path}#save` : path;
      const settingsRouteResponse = this.settingsRouteResponses.get(routeKey);
      if (settingsRouteResponse?.length) return settingsRouteResponse.shift();
      if (options?.method === 'POST') {
        const payload = JSON.parse(options.body);
        const settings = this.settingsByWorker.get(worker) ?? agentSettingsFixture();
        const next = structuredClone(settings);
        const entry = next.agents.find((value) => value.agent === payload.agent);
        Object.assign(entry, { model: payload.model, effort: payload.effort, revision: `${entry.revision.slice(0, -1)}s` });
        this.settingsByWorker.set(worker, next);
        return response(entry);
      }
      return response(this.settingsByWorker.get(worker) ?? agentSettingsFixture());
    }

    const taskDetailMatch = path.match(/^\/api\/v1\/tasks\/([^/?]+)$/);
    if (taskDetailMatch) {
      const detail = this.taskDetails[decodeURIComponent(taskDetailMatch[1])];
      return detail
        ? response(detail)
        : response({ error: { code: 'TASK_NOT_FOUND', message: 'not found' } }, false);
    }

    const taskLogMatch = path.match(
      /^\/api\/v1\/tasks\/([^/?]+)\/turns\/([^/?]+)\/logs\?stream=(stdout|stderr)&offset=(\d+)&limit=65536$/,
    );
    if (taskLogMatch) {
      const [, encodedTaskId, encodedTurnId, stream, offset] = taskLogMatch;
      const key = `${decodeURIComponent(encodedTaskId)}:${decodeURIComponent(encodedTurnId)}:${stream}:${offset}`;
      const queue = this.logs.get(key) ?? [];
      const value = queue.shift() ?? logChunk(stream, Number(offset), Number(offset), '');
      return response(value);
    }

    const detailMatch = path.match(/^\/api\/v1\/jobs\/([^/?]+)$/);
    if (detailMatch) {
      const detail = this.details[decodeURIComponent(detailMatch[1])];
      return detail ? response(detail) : response({ error: { code: 'JOB_NOT_FOUND', message: 'not found' } }, false);
    }

    const logMatch = path.match(
      /^\/api\/v1\/jobs\/([^/?]+)\/logs\?stream=(stdout|stderr)&offset=(\d+)&limit=65536$/,
    );
    if (logMatch) {
      const [, encodedJobId, stream, offset] = logMatch;
      const key = `${decodeURIComponent(encodedJobId)}:${stream}:${offset}`;
      const queue = this.logs.get(key) ?? [];
      const value = queue.shift() ?? logChunk(stream, Number(offset), Number(offset), '');
      return response(value);
    }

    return response({ error: { code: 'UNEXPECTED_REQUEST', message: path } }, false);
  }
}

class FakeTimers {
  constructor() {
    this.nextId = 1;
    this.intervals = new Map();
  }

  setInterval(callback, delay) {
    const id = this.nextId++;
    this.intervals.set(id, { callback, delay, cleared: false });
    return id;
  }

  clearInterval(id) {
    const interval = this.intervals.get(id);
    if (interval) interval.cleared = true;
  }

  async fire(id) {
    const interval = this.intervals.get(id);
    assert.ok(interval, `unknown timer ${id}`);
    if (!interval.cleared) await interval.callback();
  }

  async tick(delay) {
    const ids = [...this.intervals.entries()]
      .filter(([, interval]) => interval.delay === delay && !interval.cleared)
      .map(([id]) => id);
    for (const id of ids) await this.fire(id);
  }

  idForDelay(delay) {
    const entry = [...this.intervals.entries()].find(([, interval]) => interval.delay === delay);
    assert.ok(entry, `no timer scheduled at ${delay}ms`);
    return entry[0];
  }

  delays() {
    return [...this.intervals.values()].map(({ delay }) => delay);
  }

  cleared(id) {
    return this.intervals.get(id)?.cleared ?? false;
  }
}

function response(body, ok = true, status = ok ? 200 : 503) {
  return {
    ok,
    status,
    async json() {
      return structuredClone(body);
    },
  };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function findByTag(node, tagName) {
  const wanted = tagName.toUpperCase();
  return [
    ...(node.tagName === wanted ? [node] : []),
    ...node.children.flatMap((child) => findByTag(child, wanted)),
  ];
}

function findByTestId(node, testId) {
  return allNodes(node).find((candidate) => candidate.getAttribute('data-testid') === testId);
}

function allNodes(node) {
  return [node, ...node.children.flatMap(allNodes)];
}

function agentSettingsFixture(overrides = {}) {
  const defaults = {
    codex: {
      model: 'gpt-5.6-sol',
      effort: 'max',
      effort_options: ['low', 'medium', 'high', 'xhigh', 'max'],
      source: 'native-codex',
    },
    cursor: {
      model: 'claude-3-7-sonnet',
      effort: 'high',
      effort_options: [],
      source: 'native-cursor',
    },
    opencode: {
      model: 'opencode/default',
      effort: null,
      effort_options: [],
      source: 'native-opencode',
    },
    claude: {
      model: 'claude-sonnet',
      effort: 'high',
      effort_options: ['low', 'medium', 'high', 'xhigh', 'max'],
      source: 'native-claude',
    },
  };
  return {
    agents: Object.entries(defaults).map(([agent, value], index) => ({
      agent,
      model: null,
      effort: null,
      effort_options: [],
      source: `native-${agent}`,
      revision: `${String(index + 1).repeat(64)}`,
      writable: true,
      message: null,
      ...value,
      ...(overrides[agent] ?? {}),
    })),
  };
}

function snapshotFixture() {
  return {
    api_version: 1,
    revision: 42,
    generated_at_millis: 1_725_000_000_100,
    collection: { freshness: 'current', errors: [] },
    workers: [
      workerFixture({
        name: 'mini-forge',
        freshness: 'current',
        slot: { state: 'busy', capacity: 1, active_job_id: JOB_A },
        active_task: {
          task_id: TASK_ACTIVE,
          title: 'Repair login',
          agent: 'codex',
          turn_number: 2,
          started_at_millis: 1_725_000_000_050,
          runner: 'live',
        },
      }),
      workerFixture({ name: 'mini-anvil', freshness: 'stale', health: 'unavailable', observed_at_millis: 1_725_000_000_000 }),
      workerFixture({ name: 'mini-lathe', freshness: 'offline', health: 'unavailable', observed_at_millis: null }),
    ],
    queue: [
      {
        position: 1,
        job_id: JOB_B,
        project_id: 'queue-project-identifier',
        worktree_id: 'queue-worktree-identifier',
        project_label: 'Queued Project',
        command_summary: { mode: 'shell', arg_count: null },
        created_at_millis: 1_725_000_000_010,
        requirements: ['swift', 'xcode'],
        blocking_code: 'NO_COMPATIBLE_IDLE_WORKER',
        entry_kind: 'batch',
        task_id: null,
        turn_id: null,
        run_id: null,
        run_max_parallel: null,
        pinned_worker: null,
      },
      {
        position: 2,
        job_id: JOB_A,
        project_id: 'task-project-identifier',
        worktree_id: 'task-worktree-identifier',
        project_label: 'Repair login',
        command_summary: { mode: 'argv', arg_count: 0 },
        created_at_millis: 1_725_000_000_011,
        requirements: ['agent:codex'],
        blocking_code: 'RUN_MAX_PARALLEL',
        entry_kind: 'task_turn',
        task_id: TASK_ACTIVE,
        turn_id: JOB_A,
        run_id: RUN_ALPHA,
        run_max_parallel: 1,
        pinned_worker: 'mini-forge',
      },
    ],
    active_jobs: [jobFixture({ job_id: JOB_A, project_label: 'Active Project' })],
    recent_jobs: [
      jobFixture({
        job_id: JOB_B,
        project_id: 'recent-project-identifier',
        worktree_id: 'recent-worktree-identifier',
        project_label: null,
        state: 'succeeded',
        exit_code: 0,
        final_stdout_bytes: 4,
        final_stderr_bytes: 0,
        artifact_status: 'available',
      }),
    ],
    tasks: [
      taskRow({
        task_id: TASK_QUEUED,
        run_id: RUN_ALPHA,
        run_position: 1,
        title: 'Queue migration',
        state: 'queued',
        worker: null,
        runner: null,
        turn_count: 0,
        updated_at_millis: 1_725_000_000_012,
      }),
      taskRow({
        task_id: TASK_ACTIVE,
        run_id: RUN_ALPHA,
        run_position: 2,
        title: 'Repair login',
        state: 'active',
        worker: 'mini-forge',
        runner: 'live',
        turn_count: 2,
        active_turn_id: JOB_A,
        updated_at_millis: 1_725_000_000_013,
      }),
      taskRow({
        task_id: TASK_OPEN,
        run_id: RUN_ALPHA,
        run_position: 3,
        title: 'Review copy',
        agent: 'claude',
        state: 'open',
        last_outcome: { kind: 'needs_input' },
        worker: 'mini-anvil',
        runner: 'exited',
        updated_at_millis: 1_725_000_000_014,
      }),
      taskRow({
        task_id: TASK_CLOSED,
        run_id: RUN_BETA,
        run_position: 1,
        title: 'Close ticket',
        state: 'closed',
        last_outcome: { kind: 'done' },
        worker: 'mini-lathe',
        runner: 'exited',
        updated_at_millis: 1_725_000_000_015,
      }),
      taskRow({
        task_id: TASK_LOST,
        run_id: RUN_BETA,
        run_position: 2,
        title: 'Lost workspace',
        state: 'lost',
        last_outcome: { kind: 'lost' },
        worker: 'mini-lathe',
        runner: 'dead',
        updated_at_millis: 1_725_000_000_016,
      }),
    ],
    runs: [
      {
        run_id: RUN_ALPHA,
        name: 'Login sprint',
        max_parallel: 1,
        created_at_millis: 1_725_000_000_000,
        progress: { total: 3, queued: 1, active: 1, open: 1, closed: 0, failed_like: 0 },
      },
      {
        run_id: RUN_BETA,
        name: 'Cleanup sprint',
        max_parallel: 2,
        created_at_millis: 1_725_000_000_001,
        progress: { total: 2, queued: 0, active: 0, open: 0, closed: 1, failed_like: 1 },
      },
    ],
    progress: { total: 5, queued: 1, active: 1, open: 1, closed: 1, failed_like: 1 },
  };
}

function taskSnapshotFixture() {
  return snapshotFixture();
}

function workerFixture(overrides = {}) {
  return {
    name: 'mini-forge',
    health: 'ready',
    freshness: 'current',
    observed_at_millis: 1_725_000_000_099,
    hostname: 'mini-forge.local',
    slot: { state: 'idle', capacity: 1, active_job_id: null },
    capabilities: ['swift', 'xcode'],
    missing_capabilities: [],
    system: {
      free_disk_bytes: 300_000_000_000,
      total_disk_bytes: 500_000_000_000,
      memory_pressure: 'normal',
      swap_used_bytes: 0,
      cpu_busy_percent: 37.5,
    },
    error: null,
    active_task: null,
    ...overrides,
  };
}

function jobFixture(overrides = {}) {
  return {
    job_id: JOB_A,
    worker_name: 'mini-forge',
    project_id: 'active-project-identifier',
    worktree_id: 'active-worktree-identifier',
    project_label: 'Active Project',
    manifest_digest: 'b'.repeat(64),
    command_summary: { mode: 'argv', arg_count: 3 },
    resource_class: 'heavy',
    created_at_millis: 1_725_000_000_020,
    updated_at_millis: 1_725_000_000_030,
    state: 'running',
    exit_code: null,
    terminating_signal: null,
    final_stdout_bytes: null,
    final_stderr_bytes: null,
    artifact_status: 'pending',
    remote_uncertainty: null,
    ...overrides,
  };
}

function taskRow(overrides = {}) {
  return {
    task_id: TASK_ACTIVE,
    run_id: RUN_ALPHA,
    run_position: 2,
    title: 'Repair login',
    agent: 'codex',
    state: 'active',
    last_outcome: null,
    worker: 'mini-forge',
    branch: `task/${TASK_ACTIVE}`,
    turn_count: 2,
    runner: 'live',
    freshness: 'current',
    created_at_millis: 1_725_000_000_020,
    updated_at_millis: 1_725_000_000_030,
    active_turn_id: JOB_A,
    ...overrides,
  };
}

function taskDetailFixture(summary = 'Safe task result summary') {
  const task = taskRow();
  const turns = [
    {
      turn_number: 1,
      turn_id: 'turn-one',
      terminal: 'succeeded',
      outcome: { kind: 'done' },
      agent_committed: true,
      log_truncated: false,
      started_at_millis: 1_725_000_000_040,
      ended_at_millis: 1_725_000_000_050,
    },
    {
      turn_number: 2,
      turn_id: JOB_A,
      terminal: null,
      outcome: null,
      agent_committed: null,
      log_truncated: false,
      started_at_millis: 1_725_000_000_051,
      ended_at_millis: null,
    },
  ];
  return {
    task,
    project_id: 'project-task-identifier',
    worktree_id: 'worktree-task-identifier',
    base_oid: '0123456789abcdef0123456789abcdef01234567',
    head_oid: 'fedcba9876543210fedcba9876543210fedcba98',
    session_present: true,
    summary,
    questions: ['prompt-like fixture text should stay literal'],
    files_changed: ['src/auth/login.rs', '[path]'],
    diff_stat: '2 files changed, 14 insertions(+), 3 deletions(-)',
    fetch_command: `worker task fetch ${TASK_ACTIVE}`,
    turns,
    timeline: turns.map(({ turn_number, turn_id, outcome, started_at_millis, ended_at_millis, terminal }) => ({
      turn_number,
      turn_id,
      outcome,
      started_at_millis,
      ended_at_millis,
      terminal,
    })),
  };
}

function taskLogResponses() {
  return {
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stdout&offset=0&limit=65536`]:
      logChunk('stdout', 0, 5, 'b3V0LTE='),
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stderr&offset=0&limit=65536`]:
      logChunk('stderr', 0, 5, 'ZXJyLTE='),
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stdout&offset=5&limit=65536`]:
      logChunk('stdout', 5, 10, 'b3V0LTI='),
    [`/api/v1/tasks/${TASK_ACTIVE}/turns/${JOB_A}/logs?stream=stderr&offset=5&limit=65536`]:
      logChunk('stderr', 5, 10, 'ZXJyLTI='),
  };
}

function logChunk(stream, offset, nextOffset, data) {
  return { stream, offset, next_offset: nextOffset, data };
}

test('automatic refresh recovers from transient task-detail failure', async () => {
  const snapshot = taskSnapshotFixture();
  const detail = taskDetailFixture();
  const path = `/api/v1/tasks/${TASK_ACTIVE}`;
  const env = fakeEnvironment({snapshot, taskDetails: {[TASK_ACTIVE]: detail}, deferredResponses: {[path]: [response(detail), response({error: {message: 'temporary failure'}}, false)]}});
  const client = createDashboardClient(env);
  client.start();
  await client.refreshSnapshot();
  snapshot.tasks[1].state = 'closed';
  snapshot.tasks[1].active_turn_id = null;
  snapshot.revision += 1;
  await client.refreshSnapshot();
  detail.task.state = 'closed';
  detail.task.active_turn_id = null;
  detail.turns[1].terminal = 'succeeded';
  snapshot.revision += 1;
  await client.refreshSnapshot();
  assert.equal(env.document.node('inspector-title').textContent, detail.task.title);
});
