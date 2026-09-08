# mac-worker: порядок исправлений и ускорений

Дата: 2026-09-08. Исходный checkout: `57f9276`. Основание: [архитектурный разбор](../../2026-09-08-architecture-and-flow-review.md) и результаты первого live pool run, описанные в параллельной Claude-сессии.

Это последовательность независимых изменений. Подробный план следующего этапа уточняется по результату предыдущего. Этапы 1–2 выполнены в `.worktrees/admission-isolation`, ветка `fix/admission-worker-isolation`, и перенесены в main через fast-forward.

## 0. Завершено: transfer lock

- Изменение параллельной Claude-сессии вошло в main как `b305e32` (`fix: share the transfer repository lock across its users`).
- Этот коммит включён в ветку admission через merge `9a893bd`; этап 2 прошёл совместный полный Rust-прогон с ним.
- Критерий: два turn одного репозитория доходят до разных workers; новый submit/fetch не ждёт завершения чужого turn; GC не удаляет используемый репозиторий.
- Transfer users держат shared-lock, инициализация сериализуется отдельно, GC требует exclusive-lock.

## 1. Выбор исправного Mac

**Результат:** ошибка проверки или обновления facts одного Mac не прерывает automatic selection остальных. Pinned-задача сохраняет привязку. Режим ожидания продолжает ждать; no-wait возвращает отсутствие доступной capacity.

**Существующие механизмы:** `WorkersService`, `WorkerHealth`, `SchedulerProbeAdapter` и `ClientStateStore::publish_admission_observation` в `src/transport.rs`, `src/protocol.rs`, `src/scheduler_adapter.rs`, `src/client_state.rs`.

**Изменяемые файлы:** `src/turn_runner.rs`, `tests/turn_runner.rs`.

- [x] Создать отдельный worktree; проверить исходные tests/turn_runner.rs: 32 passed.
- [x] Воспроизвести automatic selection с исправным и недоступным worker в обоих порядках конфигурации.
- [x] Воспроизвести отдельный случай: probe успешен, facts устарели, refresh завершается ошибкой.
- [x] Проверить, что прежняя положительная запись кэша заменяется отрицательной после наблюдаемой ошибки.
- [x] Не запускать refresh для уже недоступного worker; `REFRESH_FACTS_FAILED` превращать в недоступность конкретного кандидата. Ошибки локальной конфигурации и состояния продолжать возвращать вызывающему коду.
- [x] Проверить pinned/no-wait, ожидание восстановления pinned-worker, сохранение диагностического сообщения без секретов, отказ при повреждённом локальном состоянии.
- [x] Выполнить targeted tests, fmt, Clippy; проверить смежные scheduler/task tests и полный Rust-набор.
- [x] Выполнить review diff; зафиксировать проверенное изменение отдельно.

Команды из worktree:

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test turn_runner
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test scheduler_adapter --test scheduler_queue --test task_conversation
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
```

Кэш до SSH и параллельные probes относятся к этапу 5: сначала фиксируется корректность отбора кандидатов.

Результат: коммит `03450e2` в `fix/admission-worker-isolation`, включён в main вместе с этапом 2. На этапе 1 полный Rust-набор завершился с exit 0: 1 516 passed, 0 failed. После добавления шестого regression-теста повторно выполнен весь runner-набор: 38 passed (32 исходных + 6 новых). Финальные fmt и Clippy passed. Независимое review не выявило блокирующих замечаний; предложенный тест ожидания восстановления добавлен и прошёл.

## 2. Использование свободных Mac при ожидающих задачах

**Результат:** runner, ожидающие занятую pinned-машину, не блокируют запуск совместимых задач на свободных машинах.

Реализован перенос существующего detached PID на подходящий parked turn. Передача под QueueLock сохраняет очередь и pins донора, резервирует worker для получателя и не создаёт дополнительный процесс. FIFO учитывает старые исполнимые parked turn; выбор сохраняет run caps, capabilities и ограничения владения.

Новые задачи сохраняют закрытый контекст проекта в `turns/<task>/project.json`. Исполнение, recovery и import результата используют каталог соответствующей задачи. Старый recipient может унаследовать только сохранённый контекст донора из того же worktree; непроверенный cwd не записывается. Attached runner остаётся привязан к исходному turn. Подсчёт runner учитывает уникальные PID/start-time; reconciliation сохраняет владение живого процесса при незавершённой записи метаданных.

Проверен сценарий с тремя занятыми runner slots: reservation на mini-1, два pinned waiter и parked-задача для mini-2. Получатель завершает turn, пока donor остаётся queued. Проверены отдельный репозиторий, linked worktree и восстановление после сбоя публикации из постороннего cwd. После ревью добавлены проверки legacy FIFO и невозможности закрепить неверный cwd. Итог: коммит `b68aff9`, включён в main; 64 профильных теста и полный Rust-набор (1 551 passed, 0 failed), fmt, Clippy и review пройдены. Реальные Mac/агенты в этих локальных fixtures не запускались.

Подробности: [спецификация](../specs/2026-09-08-runner-reassignment-design.md), [план и результаты проверок](2026-09-08-runner-reassignment.md).

Завершающая оптимизация: свободная pinned-машина получает собственную задачу до опроса остальных Mac. Оставшаяся часть пула опрашивается только после неудачного claim и при наличии parked-задач. После этой небольшой правки повторены все пять затронутых наборов (140 passed, 0 failed), fmt, Clippy и review; полный прогон 1 551 passed относится к основному коммиту `b68aff9`.

Отдельный оставшийся пункт надёжности: атомарное резервирование runner slot при одновременных submit/reconcile. Существующий count → spawn → adopt состоит из нескольких операций; новая передача работы использует уже запущенный PID.

## 3. Диагностика и авторизация агентов

Порядок внутри этапа:

1. [x] CLI: показывать причину неуспешного turn без обязательного `--raw`.
2. [ ] Runner: дочитывать финальные stdout/stderr; восстанавливать позиции после restart без дублирования уже записанного префикса. Переиспользовать существующую семантику `TerminalLogDrain`; согласовать завершение `logs -f` с окончанием выбранного turn и его публикации.
3. [ ] Cursor: сопоставить окружение auth probe и фактического запуска с env profile; воспроизвести расхождение и исправить его причину.

Критерии: причина ошибки видна обычной командой; длинные потоки и restart не теряют bytes; утверждение о готовности Cursor подтверждается запуском в том же профиле. Проверки, требующие реального агента, выполняются отдельно от локальных fixtures.

**Результат 3.1:** `worker task logs <id>` сохраняет форматирование событий агента и показывает обычный stderr, ранние сообщения runner, а также сохранённую причину `failed`, `cancelled`, `timed_out` или `lost` выбранного turn. Использованы существующие `adapter.parse_event`, `TaskOutcome` и закрытый runner log в `src/task_client.rs`; новые форматы хранения и события не введены. Ранний лог первого turn доступен до появления `TurnSummary`, если закрытый каталог однозначно определяет turn. При отмене до старта отсутствие файла лога не скрывает сохранённый результат; остальные ошибки чтения возвращаются как прежде.

`--follow` сохраняет неполные строки и UTF-8 между чтениями, показывает ошибку уже в состоянии Open, не повторяет неизменившуюся причину и сообщает её последующее изменение при ошибке публикации. `--raw` сохраняет исходные байты. Условие завершения follow остаётся прежним до пункта 3.2; чтение CLI ограничено уже записанным локальным логом и не исправляет потерю удалённого хвоста самим runner.

Проверено в ветке `fix/task-log-diagnostics`: 15 новых regression-тестов и пять смежных наборов — 146 passed, 0 failed; fmt, Clippy для всех targets и повторное независимое review пройдены. До исправления воспроизведены потеря stderr, недоступность раннего лога, отсутствие сохранённой причины, разрыв JSON/UTF-8, гонка последнего чтения со статусом, отсутствие сообщения в Open и отмена без созданного лога. Проверки используют локальные fixtures; реальные агенты не запускались. Полный Rust-набор для этой ограниченной CLI-правки повторно не запускался.

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test task_logs --test task_command --test task_conversation --test agent_adapters --test turn_runner --test cli_help
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
```

### 3.2. Durable runner logs and selected-turn completion

Implemented in `fix/runner-log-recovery`: private bounded write-ahead journal, committed native offsets, terminal job identity/final-length checks and confirmed EOF for both streams, publication outcome and completion committed together, and recovery ownership retained through cleanup. Follow finishes for its selected turn while the task remains Open; `say` still waits for cleanup. Cancelled Waiting/Parked turns retain a recovery row until their local never-started completion is durable.

Legacy nonempty logs remain readable without follow. Missing checkpoints fail writer recovery with `LOG_CHECKPOINT_MISSING`; follow without provable completion fails with `LOG_COMPLETION_UNKNOWN`. No automatic legacy replay/migration is attempted.

Scoped verification: the eight-suite integrated run passed 233 tests; after final self-review changes, runner 48/48, reader 20/20, journal 8/8, and conversation/context 20/20 passed. All-target Clippy, fmt and diff whitespace checks passed. Four controlled journal mutations (append replay, skipped suffix check, missing writer exclusion, dropped completion) were each detected. The first full run found a missing native-status handler in the Cursor/OpenCode fake remote; after its faithful fixture extension that suite passed 21/21 and all-target Clippy passed again. The all-target rerun exited 0: **1,585 passed, 0 failed, 0 ignored across 62 top-level Cargo sections** (nested filtered harness summaries excluded). Two bounded late guards were added while that run was in progress: historical empty legacy follow uses the selected turn’s terminal state, and pending drained completion requires acceptance. Because the shared target directory could replace later test executables during the run, that full result is not claimed for one immutable final binary set. The frozen final source subsequently passed the **complete library suite (316 tests)** and **complete task_logs suite (21 tests)**, fmt, diff whitespace checks, and all-target Clippy, all exit 0. Independent review and main integration are pending.

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test turn_runner --test task_logs --test task_conversation --test task_project_context --test runner_dispatch --test scheduler_queue --test agent_adapters --test cli_help
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --lib runner_log::
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
```

Coverage includes multi-chunk tails, immediate terminal acceptance, restart at committed offsets, changed terminal lengths/identity, selected historical follow, failed local cancellation recovery, local-only completed retry, native-event spoofing, committed logs over 8 MiB, and substituted detached log paths. Verification uses real private local files/Git and fake remotes; no live fleet or provider agents were used. Some journal/lifecycle tests were added with or after implementation; the critical journal guarantees additionally underwent controlled mutation validation.


**Review fix 1 — finalization fence.** Cancellation, completed reconciliation and runner cleanup now share the journal flock through exact current-row/owner validation, task status mutation, base release, runner clearing and final row retirement. Dead-owner adoption and handoff publication respect the same fence. Three deterministic channel/real-Git regressions failed on base `00956237b15bb7634875994ffb84926f7d149afd` with old cleanup clearing a newer runner or deleting its new base, then passed with the fix.

Frozen fix-source verification (exit 0): **544 passed, 0 failed across 9 top-level suites**: library 316, Cursor/OpenCode 21, client state 42, scheduler queue 64, task command 9, conversation 15, logs 21, project context 8, runner 48. Nested filtered library harnesses are excluded from the count. Fmt, all-target Clippy with warnings denied, and `git diff --check` also passed. Exact commands:

```sh
cargo test --lib --test turn_runner --test task_conversation --test task_command --test task_logs --test task_project_context --test client_state --test scheduler_queue --test agent_cursor_opencode
cargo fmt --all --check
cargo clippy --locked --offline --all-targets -- -D warnings
git diff --check
```

Outputs: `/private/tmp/fix1-final-scoped.log`, `/private/tmp/fix1-final-fmt.log`, `/private/tmp/fix1-final-clippy.log`. The fix's final scoped run used frozen Rust sources and no parallel builds. A fresh whole-branch all-target run remains controller-owned after scoped re-review; the earlier all-target source-delta caveat above is not claimed resolved by scoped checks.


**Review fix 2 — transient fence contention.** A legitimate runner now retries only a busy journal held by queued refresh or parent handoff. Every attempt checks exact task/turn/owner authority, then rechecks under the acquired writer. A changed owner or retired/replaced row returns `TASK_BUSY`; the cleanup fence lifetime is unchanged. Channel-gated regressions reproduce the original WouldBlock abort and verify exactly one successful submission or zero submissions after ownership loss/replacement.

Frozen fix-2 source verification (exit 0): **547 passed, 0 failed across 9 top-level suites**: library 316, Cursor/OpenCode 21, client state 42, scheduler queue 64, task command 9, conversation 15, logs 21, project context 8, runner 51. Counts exclude nested filtered library subprocess summaries. Commands:

```sh
cargo test --locked --offline --lib --test turn_runner --test task_conversation --test task_command --test task_logs --test task_project_context --test client_state --test scheduler_queue --test agent_cursor_opencode
cargo fmt --all --check
cargo clippy --locked --offline --all-targets -- -D warnings
git diff --check
```

All commands passed. Outputs: `/private/tmp/fix2-final-scoped.log`, `/private/tmp/fix2-final-fmt.log`, `/private/tmp/fix2-final-clippy.log`. No concurrent rebuilds or Rust source changes occurred during final verification. Controller-owned frozen whole-branch all-target verification remains pending after scoped re-review. The pre-existing idle/no-queue status-refresh versus new-`say` projection race remains recorded for whole-branch review and outside these scoped fixes.

## 4. Контракты и восстановление dashboard

Независимые небольшие изменения:

1. Settings: обязательный заголовок, сохранение origin/revision checks, проверка frontend → backend.
2. Завершённые логи: чтение нескольких блоков, сохранение текста при переходе live → completed, корректный UTF-8.
3. Успешный запрос очищает прежнюю ошибку; вопросы всех ожидающих задач доступны независимо от лимита параллельных запросов.
4. Включить Rust/UI-проверки на PR; проверять соответствие собранных и встроенных UI assets.

## 5. Измеримые ускорения

До изменения записать время и счётчики для cold/warm submit, нескольких задач, offline worker, длинных логов и большой истории. Измерять queue wait, admission, preparation, transfer, publication/fetch; число SSH-вызовов, durable writes и пиковую память.

Порядок:

1. Пакетное хеширование файлов вместо отдельного `git hash-object` на файл. Проверить совпадение base tree/OID для changed/deleted/untracked файлов, symlinks и необычных имён.
2. Admission cache до сети и bounded parallel probes через существующий `WorkersService`.
3. Прямой lookup task по ID; пропуск no-op записей; отсутствие перекрывающихся detail polls; сбор observations независимо от HTTP-запроса.
4. Объединённое/адаптивное чтение статуса и логов; проверить фактическую SSH-конфигурацию до добавления multiplexing.
5. Устранить лишнюю Git-операцию для source=origin с сохранением pins/OID checks; потоковый разбор больших результатов и cap stderr.

Отдельное архитектурное изменение после измерений: durable origin delivery, позволяющий освобождать execution slot до завершения медленного push. Требует собственного протокола retries/fencing.

## 6. Пользовательский flow

1. Сделать явным цикл «готово к review → доработка → принято»; использовать существующий close policy.
2. Summary, diff, результаты проверок и fetched ref в одной карточке; открытие локального review.
3. Ответы агенту из dashboard, deep links и уведомления через уже проектируемый Herdr reporter.
4. Подготовка toolchain/dependencies и проверка готовности проекта; затем preview batch с пересечениями файлов и критериями приёмки.

Постоянный controller для работы без ноутбука, дополнительные execution slots и DAG задач рассматриваются после этих этапов и измерений. Каждый меняет отдельные архитектурные гарантии и требует собственного проекта.

## Правило завершения каждого изменения

Воспроизведение проблемы → минимальное исправление → regression и смежные проверки → review → отдельный коммит. Интеграция учитывает актуальный main и параллельные ветки. Проценты ускорения указываются только после сравнимых измерений.
