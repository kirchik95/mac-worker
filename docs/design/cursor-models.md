# Cursor model choices

Settings reads the catalogue from Cursor installed on the selected Mac when
opening its native settings. There is no frontend model snapshot. Search matches
both the visible name and canonical ID; keyboard selection only changes the
draft until Save defaults is pressed.

The helper runs `cursor-agent acp`, waits for `initialize`, then calls
`cursor/list_available_models`. This returns canonical model IDs and native
parameter definitions. `agent models` prints expanded aliases and is unsuitable
for writing native configuration. The four legacy entries hidden by Cursor's
interactive picker are also hidden here, unless one is the current selection.

The ACP reader in `src/cursor_catalog.rs` uses the worker account's authentication
and an isolated temporary Cursor config/data directory. It creates no session,
sends no prompt, and never initiates login. It limits the complete exchange to
20 seconds, stdout to 1 MiB and stderr to 64 KiB, then kills and reaps its process
group and removes the temporary directory. stdin stays open through the response:
pipelining requests or closing stdin early aborts Cursor's catalogue reply.

`NativeAgentSettingsStore` validates the catalogue and marks verified model
capabilities explicitly. If discovery is unavailable or the CLI does not support
the extension, it retains remembered models from that Mac and the current
selection, with `model_catalog_source: remembered`. Empty capability data is
shown as unavailable, not as proof that a feature is unsupported. Reopening
Settings or switching Macs performs a fresh lookup. Saves revalidate capabilities.

Settings uses the project's `env_profile` when configured, otherwise the
`agents` profile when that Mac has reported it for Cursor. The GET query carries
only its name. The host loads the existing `EnvProfile`, verifies its permissions,
and uses the existing bounded Keychain unlock when configured. Only Cursor's
`CURSOR_API_KEY` is taken from the profile; reserved Keychain values and other
agents' credentials never reach Cursor. The selected key is restored after login
shell startup, without putting its value in shell arguments. A Keychain-only
profile clears an inherited API key instead of silently choosing another account.

Cursor responses include `model_catalog_profile` for live and remembered models.
Save sends that same profile name for capability revalidation; saves for other
agents do not use it. Authentication in Settings and the shelf follows the same
selected profile's fresh facts. The card's icon, ordering, and sign-in hints use
that status, and its tooltip names the profile. Missing profile facts stay
unknown even when the base account is
authenticated. An explicitly selected missing, insecure, or unusable profile
fails with a generic error; its contents are never returned to the dashboard.

Cursor uses `effort`, `reasoning`, or `reasoning_effort` depending on the model.
Reading, changing, and clearing defaults preserve that native key, exact values
such as `extra-high`, and unrelated settings. Both selected and remembered
parameter arrays are updated. For a previously unselected model, the verified
native defaults retain its context/thinking choices. Fast uses Cursor's native
string values `true` and `false`; older boolean files retain their existing type.

Regression coverage includes ACP ordering, bounds, timeout and process cleanup,
native-file isolation, all three reasoning keys, unremembered model saves,
unavailable catalogues, keyboard search, empty results, Escape focus return,
canonical IDs in dashboard save requests, profile propagation through HTTP and
SSH, post-login environment precedence, safe unlock failure, and profile-specific
authentication states.

## Catalog sources for all agents

Settings discovers Cursor, Codex, and OpenCode concurrently on the selected Mac,
so lookup latency follows the slowest discovery. The SSH settings request has a
30-second deadline. Cursor gets 20 seconds because mini-3 measured 8.5–9.3 seconds
warm and 10.4 seconds cold; Codex and OpenCode each get 15 seconds.

| Agent | Preferred source | Fallback |
| --- | --- | --- |
| Codex | `codex debug models`, with `visibility: list` and native priority order | `~/.codex/models_cache.json`, then the current selection |
| Cursor | `cursor-agent acp` → `cursor/list_available_models` using the selected environment profile | Native remembered models and the current selection |
| OpenCode | On OpenCode 1.x, `opencode models`, preserving usable `provider/model` IDs from every provider in CLI order. On 2.x there is no live source | The existing current-provider list from `~/.cache/opencode/models.json`, then the current selection |
| Claude | Built-in `fable`, `opus`, `sonnet`, `haiku`, supplemented by `~/.claude/models.json` and `~/.claude/model-catalog.json` | Built-in aliases and the current selection |

Codex and OpenCode use the adapters' account login shell with no environment-profile
entries. Their commands run in empty temporary directories after shell startup;
reserved Keychain variables are removed before the CLI starts. `ProcessRunner`
bounds stdout to 4 MiB for Codex and 256 KiB for OpenCode, stderr to 64 KiB each,
and kills the process group on timeout or output overflow. Temporary directories
are removed after discovery. A failed command, timeout, excessive output,
malformed output, or empty catalog uses the existing file fallback. Codex may
refresh its own model cache as a side effect of its catalog command.

OpenCode 2 lists models only through its shared background service, which a
settings read must neither use nor start, and `opencode models --standalone`
prints nothing. Discovery therefore runs `opencode --version` first and
`opencode models` only when the version is positively 1.x. On 2.x, and when the
version cannot be read, OpenCode returns `remembered`. Both commands share the
15-second budget.

OpenCode output is stripped of ANSI sequences, validated, and deduplicated. The
cache supplies display names only; it cannot add unavailable models to a live
list. OpenCode still offers no global effort or fast override. Every list retains
the current selection when absent from its catalog and is bounded to 128 options.
The SSH response limit remains 1 MiB; readers and the UI impose no 64-option cap.

Codex, Cursor, and OpenCode return `model_catalog_source: live` or `remembered`;
the UI currently displays this hint only for Cursor. Codex saves fetch a fresh
catalog to validate fast support, as Cursor already does. OpenCode saves do not
perform discovery. Only Cursor uses or returns `model_catalog_profile`.

## Live validation

The installed laptop Cursor returned 37 visible models through the host endpoint;
its native `cli-config.json` stayed byte-for-byte unchanged.

On 2026-09-16, real Cursor tasks using `--env-profile agents` completed on mini-1,
mini-2, and mini-3. Each ran the disposable project's existing `npm test` suite:
15 passed, 0 failed, unchanged repository files and commit. Raw SSH checks without
the profile can still report Cursor's generic Keychain error; that does not
describe authentication through the configured profile. No credentials were
copied or printed during verification.

After updating the helpers and dashboard, all three Settings API responses
returned `model_catalog_source: live`, `model_catalog_profile: agents`, and
37 models with verified capabilities. In the browser, Cursor on mini-2 showed
Connected, the profile hint, all 37 native choices plus Agent default, and
working model search.
