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
10 seconds, stdout to 1 MiB and stderr to 64 KiB, then kills and reaps its process
group and removes the temporary directory. stdin stays open through the response:
pipelining requests or closing stdin early aborts Cursor's catalogue reply.

`NativeAgentSettingsStore` validates the catalogue and marks verified model
capabilities explicitly. If discovery is unavailable or the CLI does not support
the extension, it retains remembered models from that Mac and the current
selection, with `model_catalog_source: remembered`. Empty capability data is
shown as unavailable, not as proof that a feature is unsupported. Reopening
Settings or switching Macs performs a fresh lookup. Saves revalidate capabilities.

Cursor uses `effort`, `reasoning`, or `reasoning_effort` depending on the model.
Reading, changing, and clearing defaults preserve that native key, exact values
such as `extra-high`, and unrelated settings. Both selected and remembered
parameter arrays are updated. For a previously unselected model, the verified
native defaults retain its context/thinking choices. Fast uses Cursor's native
string values `true` and `false`; older boolean files retain their existing type.

Regression coverage includes ACP ordering, bounds, timeout and process cleanup,
native-file isolation, all three reasoning keys, unremembered model saves,
unavailable catalogues, keyboard search, empty results, Escape focus return,
and canonical IDs in dashboard save requests.

## Live validation

The installed laptop Cursor returned 37 visible models through the host endpoint;
its native `cli-config.json` stayed byte-for-byte unchanged. The three worker
helpers were updated and their fallback responses verified. At the time of the
check, normal Cursor `status` on all three minis reported its generic locked
Keychain error; an actual Cursor smoke task also failed authentication. Native
and isolated discovery had the same outcome. A recheck after a GUI unlock still
returned remembered models. SSH and the console use the same account and login
Keychain on each mini. The installed CLI's SSH diagnostic treats any failed or
timed-out dummy Keychain write as "locked", so it proves unavailable access from
that context, not the actual GUI lock state. The remaining check is interactive
Keychain access in the SSH context, followed by the same supported CLI/API read.
No credentials need to be copied or printed.
