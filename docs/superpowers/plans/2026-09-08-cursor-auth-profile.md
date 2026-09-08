# Cursor auth/profile parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. The user selected external Cursor sessions in visible Herdr panes. Do not spawn nested agents.

**Goal:** Make facts and prebind observe the same account/profile login environment as a turn.

**Architecture:** Share the existing turn account scaffold and use an explicitly isolated login ProcessRequest for facts and prebind. Resolve commands under each effective profile. Keep the turn's login startup and all wire contracts.

**Tech Stack:** Rust, existing ProcessRunner, macOS zsh, Cargo integration tests; no new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-08-cursor-auth-profile-design.md`; original `docs/superpowers/specs/2026-09-03-agent-task-pool-design.md` sections 5, 15, 20.

## Global Constraints

- Preserve `/bin/zsh -lc`; supplied account HOME; existing turn USER/LOGNAME/SHELL fallback behavior; profile-before-login startup precedence.
- No injected PATH snapshot and no conversion to `-c`; login startup may override profile PATH/credentials.
- Account-bound ProcessRequests explicitly isolate the parent; unrelated requests and batch launches retain their existing behavior.
- Keep facts/profile wire schemas, classifiers, CLI flags, capability projection and dependencies unchanged.
- Preserve 2-second facts process deadline, 4-KiB stdout/stderr limits, secure-profile validation, keychain unlock-before-version/auth and secret redaction.
- Only local synthetic credentials and fake provider executables. No live provider invocation, fleet access, push or deploy. Preserve unrelated work.

### Task 1: Align facts, prebind and turn account launch semantics

Worktree: `/Users/kirchik/.cursor/worktrees/mac-worker/cursor-auth-profile`, branch `cursor-auth-profile`, runtime baseline `e53e13b2ae5b64aad2533b5d22769d0908031ab2`.
Read the stage-specific spec above, not the entire roadmap. This brief supersedes the exploratory revised-design file.

**Files and responsibilities:**
- Create `src/account_launch.rs`: crate-private account scaffold and login request builder, with no PATH-capture helper.
- Modify `src/lib.rs`, `src/process.rs`, `src/agent_facts.rs`, `src/agent/mod.rs`, `src/probe.rs`, `src/supervisor.rs`: shared account boundary, opt-in env isolation, profile-aware facts and explicit home.
- Mechanically update all other `ProcessRequest` literals to retain inheritance (`isolate_parent_environment: false`); no behavior changes in unrelated callers.
- Tests: `tests/agent_facts.rs`, `tests/agent_probe.rs`, `tests/agent_cursor_opencode.rs`, `tests/process_runner.rs`, `tests/task_turn.rs`; a dedicated `tests/agent_launch_environment.rs` and support module may hold the coherent real-shell fixture to keep existing large files focused.
- Update `docs/setup-macos-worker.md` to explain effective login/profile precedence. Controller records completion/test results and roadmap after independent review.

**Interfaces:**
```rust
// crate-private module, reusable by facts, prebind and supervisor
pub(crate) fn account_environment_scaffold(account_home: &std::path::Path)
    -> Vec<(std::ffi::OsString, std::ffi::OsString)>;
pub(crate) fn account_login_shell_request(
    account_home: &std::path::Path,
    profile_entries: &[(std::ffi::OsString, std::ffi::OsString)],
    shell_command: &str,
    policy: crate::process::ProcessPolicy,
) -> crate::process::ProcessRequest;

// new explicit field, with false on every unrelated existing literal
pub isolate_parent_environment: bool,

pub fn collect_agent_facts<P: ProfileInput>(
    runner: &dyn ProcessRunner, account_home: &Path, profiles: &[P],
) -> AgentFacts;
pub fn collect_agent_facts_at<P: ProfileInput>(
    runner: &dyn ProcessRunner, account_home: &Path, profiles: &[P],
    collected_at_millis: u64,
) -> AgentFacts;
```

The shell helper always sets `isolate_parent_environment: true`, `/bin/zsh`, `[-lc, shell_command]`, scaffold then profile entries, empty environment_remove and no stdin. Reuse safely quoted `render_prebind_shell` for exec; secrets stay in environment, never shell text. Preserve exact turn scaffold order and ambient USER/LOGNAME/SHELL fallback by moving that small block into the shared helper; keep turn task vars, Git identity, profile order and batch code unchanged.

- [x] **Step 1: Write and run real production REDs before runtime edits.**

Execution record: profile-clobber RED is recorded; a separate historical explicit-home RED is not recorded. Explicit-home behavior is covered by the final real-shell subprocess test. See the verification record below.

Start with the existing `ProbeCollector::refresh_facts_at` API and real SystemProcessRunner in a subprocess whose HOME/PATH point only to the fixture. This avoids invoking installed provider CLIs on the old resolver path. `.zprofile` establishes a fixture-only PATH, fake `cursor-agent` uses `/bin/sh`, supports `--version`, `status` and a synthetic launch check, and never accesses a provider. Profile files use the production owner-only loader.

Concrete shell fixture for the clobber regression:
```sh
# .zprofile (fixture_bin is an absolute, safely quoted fixture directory)
export PATH="$HOME/bin"
export CURSOR_API_KEY=login-bad
# fake cursor-agent; version branch omitted here only because it is the usual printf fixture
if [ "$1" = status ]; then
  if [ "$CURSOR_API_KEY" = profile-good ]; then
    printf 'Authenticated\n'
  else
    printf 'Not authenticated\n'
  fi
fi
```
Create a profile with `CURSOR_API_KEY=profile-good`. The assertion is:
```rust
assert_eq!(facts.agents.iter().find(|a| a.name == "cursor").unwrap()
    .auth_by_profile.iter().find(|(name, _)| name == "fixture")
    .map(|(_, auth)| *auth), Some(AgentAuth::Unauthenticated));
```
Current direct exec must fail that behavioral assertion with Authenticated. Record actual command, exit and failure in the report. A compiler error from a new API is not the behavioral RED. Add explicit-home RED with fake provider only in the supplied home and parent home containing no providers. The already-recorded Python boundary reproduction is background evidence, not a Rust RED.

- [x] **Step 2: Implement opt-in isolation and the shared scaffold.**

SystemProcessRunner must apply this before envs/removes:
```rust
if request.isolate_parent_environment {
    command.env_clear();
}
```
Preserve defaults by explicitly using false at old unrelated literals. Prebind delegates to the shared login helper with its existing policy. Native deletion already using prebind inherits that boundary. Supervisor consumes only the shared scaffold; no other launch change.

- [x] **Step 3: Align collection to account/profile execution.**

Pass `home` from refresh through both collection APIs. For each adapter, resolve availability using `command -v` in the isolated account login context; repeat with each secure profile's entries. Execute version/auth via quoted `exec` of the adapter command name inside the same kind of login shell, so effective PATH resolves the executable used by that execution. Do not reuse one base absolute path for every profile. Remove the old PATH snapshot/direct-exec path.

A missing base binary yields base version None and auth Unknown, but a discovered secure-profile binary still produces the AgentProbe and that profile's auth. Omit the agent only if absent from base and all secure profiles. Never apply insecure entries. Keep keychain failure UnknownWithReason and unlock before any associated version/auth. Keep existing parse/error/UTF-8/bounds behavior and unchanged Git-identity behavior.

- [x] **Step 4: Complete focused behavioral matrix and get GREEN.**

Use real shell/fake executable checks for:
1. Profile credential accepted by facts, prebind and actual LaunchPlan argv/environment.
2. Login-only credential accepted by all three (catches a `-c` shortcut).
3. Login startup overwrites profile credential: all three Unauthenticated.
4. Profile-only executable with login startup preserving/augmenting PATH: profile is available and authenticated even when base lacks it.
5. Base and profile PATH resolve different fake executables: profile verdict comes from the profile executable.
6. Login startup unconditionally overwrites PATH: login executable determines all three verdicts.
7. Explicit supplied home differs from process HOME: refresh uses supplied home and its profile.
8. Parent-only CURSOR_API_KEY, no profile/login credential: account requests reject it; ordinary non-isolated SystemProcessRunner still inherits it.

Use isolated subprocesses for parent environment changes; do not mutate global environment unsafely in multithreaded tests. All fake executable paths must stay local; tests must not launch installed provider tools. Extend existing fake-runner tests for failures/keychain/redaction and update request expectations to the new boundary without weakening them.

Run focused tests as needed, then once on the finished diff:
```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test agent_launch_environment --test agent_facts --test agent_probe --test agent_cursor_opencode --test process_runner --test task_turn
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
git diff --check
```
If no dedicated test file was needed, omit only its target. Controller owns the final all-target test run; do not duplicate that full run. Do not run concurrent Cargo builds against this target directory.

- [x] **Step 5: Document, self-review and commit the complete task.**

Check effective behavior and maintainability, all constraints, missing-base/profile-only discovery, keychain ordering, safe shell quoting and deterministic tests. Document login precedence in setup docs. Commit only this task's source/tests/docs. If Git writes are blocked, report the exact limitation and leave the completed diff for controller-managed commit.

Write the full report to the path supplied by the controller: implementation, changed files, RED and GREEN commands/output, focused test counts, fmt/Clippy status, self-review findings, concerns and commit SHA. Return only DONE / DONE_WITH_CONCERNS / BLOCKED / NEEDS_CONTEXT, commits, short test summary, concerns and report path. Do not spawn subagents or reviewers.

## Controller completion

After implementation: immutable task diff package, independent Cursor task review (spec and quality); fixes via original implementer; capable Cursor whole-branch review; one frozen all-target Cargo run; record exact counts, limitations and ruling; local integration and cleanup. No push.

## Результат и проверки

Реализовано в `4ce0aad` и `d44912f`. Facts и prebind используют общий account scaffold с turn, переданный account HOME и изолированное окружение. Поиск бинарника и auth выполняются в эффективном login-shell окружении отдельно для base и каждого secure profile. Profile-only бинарник больше не теряется из-за отсутствия base. Порядок base auth перед разблокировкой keychain профилей сохранён; остальные ProcessRequest сохраняют прежнее наследование окружения.

До реализации зафиксирован поведенческий RED: при перезаписи `CURSOR_API_KEY` в login startup старый facts возвращал Authenticated вместо Unauthenticated. Отдельный RED до изменения runtime для explicit-home случая в отчёте не зафиксирован; итоговая проверка supplied-home выполняется настоящим shell в изолированном subprocess. При исправлении review зафиксирован RED порядка base auth/keychain unlock, затем GREEN после перестановки блока.

Финальные профильные проверки на `d44912f`: 86 passed, 0 failed (`agent_launch_environment` 14, `agent_facts` 12, `agent_probe` 8, `agent_cursor_opencode` 21, `process_runner` 13, `task_turn` 18). Парный прогон shell fixtures на default threads: 27 passed. fmt, all-target Clippy с `-D warnings` и `git diff --check` пройдены.

Полный неизменный прогон controller на `d44912f8dac04abe0fac5fe05056d306fd6ec133`: **1 615 passed, 0 failed, 0 ignored, 63 top-level Cargo suites**, exit 0, 520.0s. HEAD и tracked src/tests проверены до/после запуска. Подсчёт берёт последний `test result` каждого Cargo `Running`, исключая повторный учёт вложенных subprocess-тестов. Предыдущий полный прогон на `4ce0aad` дал 1614 passed; итоговый результат выше включает regression для keychain ordering.

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test agent_launch_environment --test agent_facts --test agent_probe --test agent_cursor_opencode --test process_runner --test task_turn
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
git diff --check
```

Проверки используют настоящие локальные `/bin/zsh`, файлы профилей и synthetic executables. Четыре parity-сценария выполняют команду из production LaunchPlan program/argv/env через SystemProcessRunner; низкоуровневый supervisor fork/exec этим replay не проверяется. Реальные provider credentials и fleet smoke не использовались. Git identity остаётся прежним отдельным probe; в real-shell fixtures он заменён synthetic result, чтобы не читать login files основного аккаунта.

Наблюдавшийся ранний Unknown в параллельных fixtures не доказывал конкретный timeout: типизированная ошибка тогда не была сохранена. Итоговый harness сериализует real-shell проверки внутри процесса, защищает dispatch subprocess и показывает фактические process errors, stderr и elapsed time. Между integration binaries Cargo здесь исполняется последовательно; глобальный межпроцессный lock не нужен.

## Решения

Ruling: Preserve /bin/zsh -lc and existing profile-before-login startup semantics; align facts/prebind with the actual turn boundary instead of replacing turn startup with a PATH snapshot — login-defined credentials and runtime setup are part of the existing task contract — cost: login startup may still override a profile value, and probes must now report that real effective outcome; shell startup per probe may add latency.

Ruling: Treat the plan’s Tests list as validation targets and keep new parity cases in its explicitly permitted dedicated fixture module — changing unrelated passing test files merely to create hunks adds no coverage — cost: the final review must verify that the dedicated tests cover the promised surfaces.

Измерение для этапа 5: длительность refresh-facts в зависимости от login startup и числа secure profiles; сопоставить с существующим внешним 30-секундным REFRESH_FACTS_POLICY в src/transport.rs. Shell startup теперь выполняется отдельно для availability/version/auth. В этом этапе его задержка на реальных воркерах не измерялась.


## Независимое review и оставшиеся пункты

Реализацию выполнила внешняя Cursor-сессия Composer 2.5. Task review, scoped re-review первой правки и независимое whole-branch review выполнены внешними Cursor-сессиями Claude Opus 5. Whole-branch review на `d44912f`: **spec compliant, Ready to merge: Yes, Critical 0, Important 0, Minor 12**. Ревьюер самостоятельно пересчитал полный test log и подтвердил 1 615 passed / 63 suites. Все существенные замечания первого task review закрыты: base auth/keychain ordering, видимость helper, диагностика ошибок shell fixtures, защита subprocess dispatch, actual LaunchPlan parity и устаревший fallback-сценарий.

| Финальный пункт | Решение |
|---|---|
| M1: цена login startup и внешний 30-секундный бюджет refresh/install | Измерить на реальном worker в этапе 5; не менять deadline без измерения. |
| M2: login startup попадает в stdout/stderr probe | Закрыт в `d9fb584` / `f4c22e9`; scoped re-review: ADDRESSED. |
| M3: ambient USER/LOGNAME/SHELL | Закрыт той же docs-only правкой; scoped re-review: ADDRESSED; runtime соответствует контракту. |
| M4: native session deletion теперь тоже изолирует окружение через prebind | Предусмотренное поведение; daemon-only переменные больше не наследуются. |
| M5: Git identity пока использует ambient HOME | Отдельный последующий этап согласования account boundary; текущая спецификация требует сохранить старое поведение. |
| M6: fake runner agent_probe возвращает успех на неизвестный exec | Не блокирует: ветка сейчас недостижима; заменить на panic при следующей правке этого harness. |
| M7: дублированный tokenizer exec в двух fake runners | Отложенная косметическая правка при следующем изменении тестов. |
| M8: старые имена fixture helpers и промежуточные aliases | Завершить переименование при следующей правке fixtures. |
| M9: читаемое поле guard называется _inner | Переименовать при следующей правке fixtures; логика владения проверена. |
| M10: лишний to_owned перед OsString | Отложенная косметическая правка; поведение не меняется. |
| M11: subprocess helper tests в обычном запуске выходят без assertions | Принятый pattern репозитория; wrappers выполняют assertions. Три таких helper в agent_launch_environment и один в process_runner входят в Cargo totals. |
| M12: bounds-test проверяет request shape Claude, но больше не Codex | Per-adapter auth по-прежнему покрыт поведением; расширить shape assertion при следующей правке теста. |

Ни один отложенный пункт не блокирует интеграцию по независимому review. Исторические планы 2026-09-03 описывают прежнюю реализацию; актуальный account/profile boundary определён спецификацией этапа 3.3 и данным планом.

## Финальное завершение

Документационные уточнения M2/M3 внесены исходным Cursor-агентом в `d9fb584` и `f4c22e9`. После проверки controller формулировки уточнены: лимит 4 КиБ относится к каждому потоку отдельно, classifier допускает несколько согласованных recognised status lines. Единственный scoped re-review всей docs-only правки выполнен новой внешней Cursor-сессией Composer 2.5: **M2 ADDRESSED, M3 ADDRESSED, new issues 0**.

После полного прогона на `d44912f` менялась только документация. Controller дополнительно проверил `git diff --check` и отсутствие изменений runtime/tests/dependencies; setup docs не встроены в runtime через include_str/include_bytes. Поэтому повторный полный Cargo-прогон для docs-only commits не нужен.

Способ интеграции: локальный fast-forward ветки `cursor-auth-profile` в `main`, без push. Native Cursor worktree сохраняется; временные материалы текущего SDD-плана архивируются локально, а созданные controller панели Herdr закрываются после завершения агентов. Следующий пункт roadmap — этап 4, контракты и восстановление dashboard.
