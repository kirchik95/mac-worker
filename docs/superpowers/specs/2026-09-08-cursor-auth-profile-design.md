# Cursor auth/profile parity — stage 3.3

The ready/auth verdict for an agent and a secure named environment profile must use the account, profile, login startup and parent-environment isolation used by the actual turn. Existing task-pool design sections 5, 15 and 20 remain binding.

Existing mechanisms: `turn::EnvProfile` securely loads owner-only profile files; `agent_facts::ProfileInput` supplies entries and keychain configuration; `agent::prebind_login_request` renders a safely quoted login command; `supervisor::LaunchPlan` builds the explicit turn environment. Reuse these mechanisms, their classifiers and the existing facts schema.

The current facts path resolves binaries in the helper's home, snapshots only login PATH, and directly executes the binary with profile entries. A login file that overwrites a profile credential therefore makes facts report authenticated although a turn sees an unauthenticated credential. Prebind also inherits unspecified helper variables whereas turns use an explicit execve environment.

## Contract

- Preserve `/bin/zsh -lc`. Account HOME is the supplied account home. Match the turn's existing USER/LOGNAME/SHELL values, including its nonempty ambient value and account-name fallback rules.
- Explicit account fields are followed by profile entries; login startup subsequently runs and may override profile variables, including PATH and credentials. No injected PATH snapshot and no conversion to `-c`.
- Facts resolution and version/auth execution use this boundary separately for the base account and every secure profile. Each execution resolves the adapter's command name in its effective shell context. A binary present only in a secure profile must not disappear because the base account lacks it.
- Account-bound ProcessRequests explicitly opt out of incidental parent environment inheritance. All unrelated ProcessRequest callers retain their previous inheritance behavior. Keep batch launch environment unchanged.
- `ProbeCollector::refresh_facts_at` supplies its home to facts collection. Keep facts/profile wire schemas, auth classifiers, adapter CLI flags, scheduler capability projection and dependencies unchanged.
- Preserve the 2-second deadline and 4-KiB stdout/stderr limits for each facts process; keep secure-profile validation, keychain unlock-before-version/auth, bounded failure classification and secret redaction.
- Do not run real provider binaries, use real credentials, access the fleet, push, or deploy during validation. Local real-shell/fake-executable tests establish boundary parity, not live provider acceptance.

## Acceptance

Production regression tests exercise real `/bin/zsh` and synthetic executables, checking profile credentials, login-only credentials, login overrides, profile-only/different binaries, login PATH override, explicit home selection, parent-only credential isolation and legacy process inheritance. Parity includes the production facts and prebind paths and actual turn launch environment/argv. Child processes isolate test environment changes from concurrent Rust tests. Fake runners continue covering output limits, errors, keychain failure and redaction.

Ruling: Preserve /bin/zsh -lc and existing profile-before-login startup semantics; align facts/prebind with the actual turn boundary instead of replacing turn startup with a PATH snapshot — login-defined credentials and runtime setup are part of the existing task contract — cost: login startup may still override a profile value, and probes must now report that real effective outcome; shell startup per probe may add latency.

## Implementation record

Implemented in `4ce0aad` and `d44912f`; independent whole-branch review found the contract compliant with no blocking findings. Verification, operational limits and both controller rulings are recorded in [the implementation plan](../plans/2026-09-08-cursor-auth-profile.md). The runtime at `d44912f` passed the frozen full suite: 1615 tests across 63 top-level Cargo suites.
