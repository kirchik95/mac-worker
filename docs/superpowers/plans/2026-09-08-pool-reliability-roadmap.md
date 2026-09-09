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
2. [x] Runner: дочитывать финальные stdout/stderr; восстанавливать позиции после restart без дублирования уже записанного префикса. Переиспользовать существующую семантику `TerminalLogDrain`; согласовать завершение `logs -f` с окончанием выбранного turn и его публикации.
3. [x] Cursor: сопоставить окружение auth probe и фактического запуска с env profile; воспроизвести расхождение и исправить его причину.

Критерии: причина ошибки видна обычной командой; длинные потоки и restart не теряют bytes; утверждение о готовности Cursor подтверждается запуском в том же профиле. Проверки, требующие реального агента, выполняются отдельно от локальных fixtures.

**Результат 3.1:** `worker task logs <id>` сохраняет форматирование событий агента и показывает обычный stderr, ранние сообщения runner, а также сохранённую причину `failed`, `cancelled`, `timed_out` или `lost` выбранного turn. Использованы существующие `adapter.parse_event`, `TaskOutcome` и закрытый runner log в `src/task_client.rs`; новые форматы хранения и события не введены. Ранний лог первого turn доступен до появления `TurnSummary`, если закрытый каталог однозначно определяет turn. При отмене до старта отсутствие файла лога не скрывает сохранённый результат; остальные ошибки чтения возвращаются как прежде.

`--follow` сохраняет неполные строки и UTF-8 между чтениями, показывает ошибку уже в состоянии Open, не повторяет неизменившуюся причину и сообщает её последующее изменение при ошибке публикации. `--raw` сохраняет исходные байты. Условие завершения follow остаётся прежним до пункта 3.2; чтение CLI ограничено уже записанным локальным логом и не исправляет потерю удалённого хвоста самим runner.

Проверено в ветке `fix/task-log-diagnostics`: 15 новых regression-тестов и пять смежных наборов — 146 passed, 0 failed; fmt, Clippy для всех targets и повторное независимое review пройдены. До исправления воспроизведены потеря stderr, недоступность раннего лога, отсутствие сохранённой причины, разрыв JSON/UTF-8, гонка последнего чтения со статусом, отсутствие сообщения в Open и отмена без созданного лога. Проверки используют локальные fixtures; реальные агенты не запускались. Полный Rust-набор для этой ограниченной CLI-правки повторно не запускался.

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test task_logs --test task_command --test task_conversation --test agent_adapters --test turn_runner --test cli_help
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
```

### 3.2. Восстановление логов и завершение выбранного turn

**Результат:** runner дочитывает stdout/stderr до подтверждённого EOF, включая длинные хвосты и ответ submit с уже завершённой задачей. Закрытый журнал сохраняет позиции обоих потоков вместе с записанными байтами; restart продолжает чтение без повторения подтверждённого префикса. `logs --follow` завершается после дочитывания и публикации результата выбранного turn, даже если задача остаётся Open или уже выполняется следующий turn.

Завершение, отмена и recovery сохраняют владение до окончания очистки. Старый finalizer не может очистить runner или base pin нового turn. Кратковременная занятость журнала повторно проверяется с контролем владельца. Запоздалые ответы cancel и status refresh записываются только при совпадении исходного task record под существующим `StateLock`, поэтому не затирают историю нового turn или fetched head. Отмена принятого turn не ждёт завершения чтения логов.

Старые логи без checkpoint остаются доступны без follow. Для непустого legacy-лога восстановление writer возвращает `LOG_CHECKPOINT_MISSING`; follow без доказуемого завершения — `LOG_COMPLETION_UNKNOWN`. Байты сохраняются; автоматической миграции нет.

Изменения `0095623`, `2323cb1`, `290f8c6`, `cb75307` включены в локальный main через fast-forward ветки `fix/runner-log-recovery`. Task review, whole-branch review и повторное review последней правки завершены; блокирующих замечаний нет. Финальный полный прогон на неизменном `cb75307`: **1 598 passed, 0 failed, 0 ignored, 62 top-level Cargo suites**, exit 0. Дополнительно прошли 618 профильных тестов, fmt, all-target Clippy с запретом warnings и `git diff --check`.

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
cargo fmt --all --check
cargo clippy --locked --offline --all-targets -- -D warnings
git diff --check
```

Проверки используют настоящие локальные файлы/Git и fake remotes. Они покрывают crash recovery, длинные потоки, подмену log paths, отмену, смену владельца, задержанные ответы, сохранение результата и завершение выбранного follower. Реальные Mac и provider agents не запускались. Часть journal/lifecycle-тестов добавлена вместе с реализацией или после неё; четыре критические гарантии дополнительно проверены контролируемыми мутациями. Финальный неизменный полный прогон заменяет ранний результат 1 585 тестов со смешанным набором binaries.

Подробности, решения и история проверок: [спецификация](../specs/2026-09-08-runner-log-recovery-design.md), [план и результаты](2026-09-08-runner-log-recovery.md).

### 3.3. Авторизация и окружение профиля Cursor

**Результат:** facts, prebind и turn используют общий account scaffold и `/bin/zsh -lc`. Facts получают явно переданный account HOME и проверяют каждый secure profile в его эффективном login-окружении. Profile-only бинарник доступен даже при отсутствии base-бинарника; PATH и credentials после login startup определяют реальный verdict. Непредусмотренное наследование переменных helper устранено для account-bound requests. Native session deletion использует тот же prebind boundary. Login startup может переопределить профиль; его stdout/stderr входят в probe output, поэтому worker login files должны работать без вывода.

Расхождение воспроизведено до реализации: login startup перезаписывал `CURSOR_API_KEY`, а прежний facts ошибочно возвращал Authenticated. Проверки теперь выполняют production facts, prebind и команду из LaunchPlan с настоящим zsh и synthetic executables. Порядок base auth перед разблокировкой keychain профилей сохранён и защищён regression-тестом.

Реализация `4ce0aad`, исправления review `d44912f`, финальные уточнения документации `d9fb584` / `f4c22e9`. Task review, scoped re-review, независимое whole-branch review и проверка финальной документационной правки пройдены; блокирующих замечаний нет. Полный неизменный прогон на `d44912f`: **1 615 passed, 0 failed, 0 ignored, 63 top-level Cargo suites**, exit 0; также 86 профильных тестов, fmt и all-target Clippy с запретом warnings. Проверки локальные; live provider/fleet smoke не выполнялся. Runtime после этого прогона не менялся.

Измерение login startup и refresh-facts остаётся для этапа 5: внешний бюджет refresh/install — 30 секунд, а количество shell launches растёт с числом secure profiles. Git identity пока сохраняет прежний ambient-home probe; его согласование с account boundary требует отдельной правки. Остальные неблокирующие замечания и принятые решения записаны в [плане](2026-09-08-cursor-auth-profile.md); контракт — в [спецификации](../specs/2026-09-08-cursor-auth-profile-design.md).

## 4. Контракты и восстановление dashboard

Сделано и проверено 2026-09-09 на ветке `dashboard-reliability`, коммит `7d91125c3d27cdaf05c5e65b3c209db55a933048`. Код dashboard и встроенные assets совпадают с `6db0679`; в полную проверку `7d91125` входят поздние правки только теста (`37821f0`) и только CI (`7d91125`). Не запушено и не выкладывалось.

План и спецификация: [план](2026-09-08-dashboard-reliability.md), [спецификация](../specs/2026-09-08-dashboard-reliability-design.md).

Что изменилось:

1. Сохранение настроек одного агента больше не затирает остальных; заголовок защиты и проверки origin/revision на месте. Клиент и реальный Axum-маршрут проверены отдельно, не одним браузерным e2e.
2. У завершённого хода читаются все доступные блоки лога, живой текст и разорванный UTF-8 сохраняются; остановка — «до текущего конца», не доказанный EOF. Если тот же ход снова живой, чтение продолжается.
3. После временной ошибки на экране остаются прежние данные задачи. Вопросы подгружаются для всех ожидающих задач, не больше шести одновременных чтений; сбой не превращается в «вопросов нет». Панель лога у ещё не стартовавшего хода ждёт метку старта или конца.
4. На PR включены проверки UI и Rust (`macos-15`, Node 22, `actions/checkout@v7` / `actions/setup-node@v7`) и сверка собранных файлов со встроенными. Rust-тесты в PR: `cargo test --locked --all-targets -- --test-threads=1`. Файл `release.yml` не менялся.

Текущий полный прогон на `7d91125` (локально, один раз, с `-- --test-threads=1`): UI 87 тестов / 11 файлов; lint 15 предупреждений (14 старых + запись `liveRef` в `useTurnLog.ts:36`); сборка и сверка 7 файлов; rustfmt, Clippy, `git diff --check`; Rust **63** набора, **1615** passed, **0** failed, **0** ignored, **738.252 с**. Doctest в этот gate не входит. Параллельный `cargo test` по умолчанию зелёным не считается. GitHub Actions и живой пул не запускались.

История: тот же `--all-targets` без `--test-threads=1` упал на `6db0679` (exit 101, 517.483 с, 16 падений в `turn_runner` из‑за часа наблюдения кэша; правка только теста `37821f0`, RED/GREEN с задержкой 2100 мс, TTL в коде не трогали) и снова на `37821f0` (exit 101, 19.223 с, lib 315/1, WouldBlock после `drop`). Серийный `--lib` тогда дал 316 passed за ~50 с. PID/FD держателя flock в упавшем процессе не установлен. Большинство долгих fork-фикстур уже запускает изолированного потомка через exec; дальше — довести изоляцию дескрипторов при создании тестового процесса. Серийный harness — смягчение, не доказанный ремонт runtime FD.

Дальше, не этот этап: довести изоляцию дескрипторов в fork-тестах; отдельно перенести `release.yml` с Node 20 / macos-14 (опубликованные даты 2026-09-23 и 2026-11-02: [changelog](https://github.blog/changelog/2025-09-19-deprecation-of-node-20-on-github-actions-runners/), [runner-images#13518](https://github.com/actions/runner-images/issues/13518); это расписание, не гарантия поломки в тот день). Источники PR: [hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners), [runner-images](https://github.com/actions/runner-images/blob/main/README.md), [checkout](https://github.com/actions/checkout), [setup-node](https://github.com/actions/setup-node). Семнадцать отложенных замечаний review не закрыты. Этап 5 — только после измерений; ускорений кода здесь нет.

## 5. Измеримые ускорения

До изменения записать время и счётчики для cold/warm submit, нескольких задач, offline worker, длинных логов и большой истории. Измерять queue wait, admission, preparation, transfer, publication/fetch; число SSH-вызовов, durable writes и пиковую память.

**Прогресс 2026-09-09 — поднабор, не весь этап 5.** Проверенная реализация: измерения и полный Rust на `1c0bb51703c65587d25c658c046f236f671c6fab` (cherry-pick `1a11257` / `41b02be` / `45f02013` поверх `69aa8a6`): пакетное заполнение индекса (`update-index -z --index-info`), reuse digest→OID внутри одного `build_wip_base` при повторном свежем чтении байтов, cleanup owner-regular scratch-index, locked point lookup для dashboard detail/log. Первый capture по-прежнему вызывает Git `hash-object` на каждый distinct content (нет batched hash-object protocol). Admission/probes, пропуск no-op записей, фоновые observations, SSH multiplexing и origin delivery в этом инкременте не менялись. Полный Rust на этом HEAD: 67 `Running` targets, 1710 passed / 0 failed / 0 ignored, `logs/ci-test-all-targets.log` (~797.8 с). Подробности, ограничения измерений и таблица before/after: [2026-09-09-performance-improvements.md](../../2026-09-09-performance-improvements.md). Этап 5 целиком не закрыт; пункты ниже остаются.

**Прогресс 2026-09-09 — пункт 1 через ограниченный Git blob protocol, не весь этап 5.** Измерения на `39f5909f02dc77e14e9203d42e2788414613df8a` (merge local main `d1c239d`, cherry-pick `afff564` / `ece7bf9`). Полный Rust на `6603f448cc5a216069f47e96d31d2f70a3b72f06` (однострочная адаптация init-фикстуры под Gatekeeper warmup параллельного main; runtime snapshot не менялся). Bounded `git fast-import` (≤64 unique objects, ≤1 MiB unique payload), singleton и oversized `hash-object`, два свежих capture, per-build memo. Outer Git на H=1000: 1021 → 37 ProcessRunner-запросов; Trace2: 1021 → 53 sid, финал 37 outer + 16 unpack-objects — не полный census OS-процессов. Исторический empty 167→214 ms не объяснён. Admission/probes, no-op writes, observations, SSH multiplexing и origin delivery не менялись. Полный Rust: 67 `Running`, 1737 passed / 0 failed / 0 ignored, `logs/ci-test.log` (~784 с). Открытое наблюдение: существующий intermittent сбой `distinct_validated_incoming_roots_race_one_no_replace_cache` требует отдельной диагностики; финальный полный suite прошёл, этот инкремент эту ошибку не чинил. Подробности: [2026-09-09-snapshot-batch-performance.md](../../2026-09-09-snapshot-batch-performance.md).

Порядок:

1. Пакетное хеширование файлов вместо отдельного `git hash-object` на файл. Проверить совпадение base tree/OID для changed/deleted/untracked файлов, symlinks и необычных имён. **Сделано через bounded blob batching (этот инкремент); этап 5 целиком открыт.**
2. Admission cache до сети и bounded parallel probes через существующий `WorkersService`. Реализация Stage 5.2 на ветке `admission-cache-20260909`: bound skip-SSH, общий 3-wide pipeline, один 60s deadline; измерение и полный suite — после freeze.
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
