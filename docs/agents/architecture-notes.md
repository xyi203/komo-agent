# Architecture notes (archived long-form AGENTS.md)

The pre-2026-07 long-form `AGENTS.md`, kept for its design rationale — the
*why* behind each module's decisions (cancel semantics, tool-output store
edges, policy ladder, provider quirks, …). The live agent guide is the concise
`AGENTS.md` at the repo root; this file is reference only and is not kept in
sync with code changes.

## Commands

```bash
cargo check                        # fast compile check
cargo build                        # build
komo init                         # bootstrap ~/.komo: default config.toml + .env template + SOUL.md persona + USER.md profile scaffold (never overwrites)
cargo run -- chat                  # interactive chat: full-screen TUI (needs a terminal; scripts use the api channel) (db at ~/.komo/state.db)
cargo run -- gateway               # always-on process: maintenance sweeps + ingress channels (feishu, telegram, wechat)
cargo test                         # run all tests
cargo test tools::time             # run a single test module
cargo fmt                          # format

komo gateway start                # macOS only: supervise the gateway with launchd
komo gateway stop                 # macOS only: stop it and remove the launchd job
komo gateway restart              # macOS only: stop + start (picks up a reinstalled binary)
komo gateway status               # macOS only: launchd state
komo upgrade [--no-restart]       # git pull --ff-only + cargo install (reinstall) + restart the gateway (analog of `hermes update`)
komo logs [-n N] [-f] [--stdout]  # tail the gateway tracing log (-f follows; --stdout shows gateway.log)

komo memory list [--status S]     # list/triage memories (candidate/active/archived/rejected)
komo memory search <query>        # substring search across all memories
komo memory promote <id>...       # candidates → active+confirmed (batch; works with the gateway up)
komo memory reject <id>...        # candidates → rejected (batch; works with the gateway up)
komo memory pin <id>              # pin into the L1 per-turn profile (manual-only path)
komo memory triage                # interactively clear the candidate pile (oldest first; p/r/s/q)
komo memory report                # quality report: status/confidence counts + piles needing triage
komo dream [--apply]              # usage-driven consolidation: preview (default) or run one cycle — promote well-recalled candidates, archive never-recalled ones

komo cron list                    # scheduled jobs (cron.db, with last-run status) + pending reminders
komo cron add <name> <cron> <cmd> [-- args…]  # schedule a command job (deterministic, stdout → home channel)
komo cron add-agent <name> <cron> <prompt> [--skill S]…  # schedule an agent turn (unattended, full tools, policy-gated)
komo cron run <name>              # fire a job now (due on the gateway's next sweep tick)
komo cron enable|disable <name>   # resume / pause a job without deleting it
komo cron remove <name>           # delete a job

komo run list [--limit N]         # recent runs (one per turn), newest first; ⟲ marks recoverable
komo run inspect <id>             # one run in full: input, plan, outcome, every tool step
komo run resume [<id>]            # re-dispatch an interrupted run (defaults to the latest recoverable)
komo run prune --before <date>|--keep <N>   # trim the run ledger (delete old runs + their steps)

komo journey [--limit N] [--since YYYY-MM-DD]  # learning timeline: memories (born/promoted/archived) + skills (proposed/activated), newest first
komo skills list                   # managed skills + read-only ~/.agents/skills + reviewer candidates
komo skills install <source>       # fetch a skill (owner/repo[/subpath], GitHub/*.git/git@ URL, or a raw SKILL.md URL) straight into the active store
komo skills inspect <name>         # one skill in full: status, provenance, path, history, body
komo skills promote|reject <name>  # triage a reviewer candidate (accept into active / discard)
komo skills protect|unprotect <name>  # operator-edit-only: reviewer stops proposing changes
komo skills enable|disable <name>  # hide from the agent without deleting (and back)
komo skills audit <name>           # which turns loaded this skill (derived from the run ledger)
komo policy list                  # every grant source in one view: config rules + job grants + saved grants
komo policy check <cat> <target>  # dry-run one action: verdict + deciding rule ([--channel c] [--dangerous] [--write])
komo wiki index [--rebuild]       # incremental by mtime; --rebuild resets the store first (minutes)
komo policy saved list            # grants accumulated by answering `a` at an approval prompt (permissions.json)
komo policy saved forget <n>|--all  # stop honoring one/every saved grant — that action asks again
komo doctor                       # config & gateway health: model+key, schedules, policy, channels, home, recent failures
komo health                       # one-line gateway liveness probe (exit 0 = healthy; the Docker HEALTHCHECK command)

komo channel list [--json]              # resolved channel inventory + gateway mounted state
komo channel probe <channel>            # verify one configured channel without sending a message
komo channel setup <channel>            # interactive setup: feishu | telegram | wechat | homeassistant
komo channel wechat login               # provision WeChat iLink creds by scanning a QR (run on the host)

komo workday [YYYY-MM-DD]          # is a date a Chinese working day? (statutory holidays + 调休); defaults to today
```

Logs: a `tracing` subscriber is installed in `main.rs` (`init_tracing`) — without
it every `info!`/`warn!`/`debug!` is a silent no-op. Output goes to stderr
(launchd captures the gateway's via the plist's `StandardErrorPath` →
`~/.komo/logs/gateway.err.log`; Docker reads it via `docker logs`), and the
gateway additionally tees into a daily-rotated file
(`~/.komo/logs/gateway.YYYY-MM-DD.log`, 30 files kept, the appender deletes
older ones — `main.rs::open_gateway_log`), which is what `komo logs` reads. Level is `KOMO_LOG` (e.g. `KOMO_LOG=debug`),
defaulting to `info,toasty=warn,rig_core=warn` (komo's own logs at info; ORM
schema chatter muted; and rig's `prompt_request` INFO events muted — they log
every tool call's *full result* verbatim, a wall of text for any list-returning
tool). Each turn runs inside a `run` span (`run_id`) and each tool call inside a
`tool` span (`name`/`seq`) and is recorded by komo's own concise `tool ok`
line (name/seq/elapsed, no result), so live logs still line up with the
persisted run ledger. Set `KOMO_LOG=debug` (or `rig_core=info`) to see the full
tool results again.

`~/.komo/state.db` is disposable developer state (sessions, messages, session
todos, skills, reminders, pairings, settings, **run ledger**) — delete it freely
to reset. `~/.komo/tool-output/` is disposable the same way: the full text of
over-limit tool results (`services/tool_output_store.rs`), expired after 7 days
and safe to delete at any time — a stale path in an old transcript is a dead
pointer, nothing more.
Durable personal data lives in **its own files** so resetting `state.db` never
wipes it — including the operator's saved approvals in
**`~/.komo/permissions.json`** (`infra/permissions_store.rs`). The three dbs:
cross-session **tasks in `~/.komo/kanban.db`**
(`infra/persistence/kanban.rs`), long-term **memories in `~/.komo/memory.db`**
(`infra/memory/memory_db.rs`), and scheduled **cron jobs in `~/.komo/cron.db`**
(`infra/persistence/cron.rs`). After a schema change on **disposable** state,
delete the affected file — `push_schema` only runs for newly created database
files: a `TaskRecord` change means deleting `kanban.db`, a `CronJobRecord`
change `cron.db`, any other model means
`state.db` (e.g. a `RunRecord`/`RunStepRecord` change — the run ledger lives in
`state.db`). **Column additions can skip the reset**: the shared
`infra/persistence/mod.rs::ensure_columns` runs an additive `ALTER TABLE ADD
COLUMN` in place on connect — `memory.db` uses it for every `MemoryRecord`
column (durable data must never need a reset; extend the `EXPECTED` list in
`memory_db.rs`), and `state.db` uses it for `SessionRecord` columns (see
`SESSION_COLUMNS` in `db.rs::connect` — extend it when adding a column, so an
upgraded gateway doesn't hard-fail on the old file until someone remembers to
delete it). Columns must be NOT NULL with a DEFAULT, or nullable. A new *table*
or any non-additive change still means deleting the disposable file.

**Running the CLI while the gateway is up.** Turso takes an *exclusive
cross-process lock* on each db file (no multi-process open), so while the gateway
runs it is the sole owner of all three dbs — a CLI that opened one directly would
fail with `File is locked by another process`. So the gateway runs an **always-on
loopback api channel** (`infra/messaging/api.rs`) and advertises it in
`~/.komo/gateway.json` (`infra/rendezvous.rs`: bind/port/auto-key/pid, written on
start, removed on graceful shutdown). Every **operator** action goes through
`services/operator_control/` — the CLI resolves one `OperatorControl::connect`
per invocation (probe the rendezvous file → `/health`, exactly once) and issues
typed `OperatorQuery`/`OperatorCommand` calls; whichever backend answered is
invisible to the command modules. The **gateway adapter** maps those onto the
existing `/api/*` routes via `infra/gateway_client.rs::GatewayClient` (reads
deserialize the domain types verbatim; writes hit the loopback-gated `POST`
endpoints — memory promote/reject/pin, `runs/prune`, `sessions/clean`,
`pairings/approve|revoke`, `dream/apply`, `runs/{id}/resume`). The **direct
adapter** opens the stores lazily per request family (a `run list` never
touches memory.db; a triage batch reuses one connection), so there is no "stop
the gateway first" refusal. Business results can't fork between the two paths:
both run the shared projections/transitions in `operator_control/actions.rs`
(`OperatorActions` is the same bundle the api channel's handlers delegate to,
and transition semantics live on `Memory::promote/reject/pin`). `run resume`
keeps eligibility, the priming digest, and the at-most-once `recoverable` clear
inside `OperatorControl::resume_run`; only the interactive local turn is a
caller-supplied closure over the already-open stores. `komo chat` is the one
non-operator path: the TUI connects via `GatewayClient::chat` →
`POST /v1/chat/completions` with a stable `X-Komo-Session-Id` (server-side
history) and `X-Komo-Trusted` (the gateway runs the turn with
`SessionContext::trusted` → side-effecting tools **auto-approve**, since the CLI
user is the host operator; gated to **loopback** callers, so a publicly-bound api
never gets it). `/pair approve` in chat remains the other in-gateway admission
path. **Cancelling a turn** (`domain/cancel.rs`): an api turn runs on a spawned task,
so a client hanging up doesn't stop it — the agent would keep going and its reply
would land in the transcript unread. `POST /api/interactions/{session}/cancel`
stops it for real: it resolves any pending approval as *deny* and any pending
clarify with a stop answer (a turn parked on a prompt isn't at an await the
signal can interrupt), then flips the session's `CancelSignal`
(`agent/interaction.rs::CancelState`, a `watch` channel per session, hung on the
turn's `SessionContext` like the event sink). `run_agent_loop` races **every**
await against it — the model round-trip included, since that is the longest wait
and the likeliest thing a user interrupts — so a cancel lands within one await
rather than after the whole turn. The turn then fails with `Cancelled`:
`（已中断）` is persisted as the assistant message (keeping the transcript
alternating and self-explanatory) and the run is finalized `Failed` /
`cancelled by user`, deliberately **not** `recoverable` — a deliberate stop is
not crash residue. A tool call **already executing** stops only if it claims the
signal through `ToolContext::cancelled()` (resolves on cancel, pends forever when
the turn has no signal — so the `select!` arm is inert for sweeps/cron/aux):
`shell` claims it and `killpg`s its process group, so interrupting a ten-minute
build ends the build; `web_fetch` / `web_search` claim it and drop the request.
Everything else runs to completion and notices only on return. The executor
deliberately does **not** race every call against the signal on the tools'
behalf — that would also interrupt the filesystem tools, and `apply_patch` writes
several files in sequence, so stopping between two of them would turn a patch
that would have finished into a half-applied tree (a single `write`/`edit` is safe
either way: one `tokio::fs::write` is one `spawn_blocking`, so the syscall
completes regardless). A claimed cancel fails the call with `Cancelled`, which
the retry classifier treats as terminal and the ledger records as `cancelled by
user` — the same wording as the run's own stop. Turns with no signal attached
(chat channels, cron, sweeps, aux sub-agents) are unaffected.

A **CORS layer** (`infra/messaging/api.rs::cors_layer`, applied outermost so a
preflight is answered before the bearer-key middleware could 401 it) grants
loopback page origins — plus the opaque `null` origin a packaged Electron
renderer sends from `file://`. Without it the desktop/web renderers, which do
their HTTP from the renderer process, can't reach the gateway at all unless they
happen to be same-origin with it. Credentials are off and only loopback origins
are granted, so the bearer key stays the only thing that admits a caller.

The api channel is loopback-only on an ephemeral port by default;
`[channels.api] enabled = true` widens it to an external bind/port (requires
`API_SERVER_KEY`) for Open WebUI / the dashboard. Two further `[channels.api]`
options serve the web client (`apps/web`): `web_dir = "…/apps/web/dist"` serves
that built SPA same-origin as the router's fallback (static assets are public,
like `/health`; `/api` + `/v1` stay key-gated — served this way the SPA is
same-origin, so CORS never enters into it), and
`remote_interactive = true` lets **keyed remote** (non-loopback) callers run
interactive turns (`X-Komo-Interactive`) and resolve approval/clarify prompts
over `/api/interactions/*` (off by default — those assume a host operator behind
a loopback socket; `X-Komo-Trusted` auto-approve stays loopback-only regardless).

Building requires `protoc` (`brew install protobuf`): the feishu channel's websocket
frames are protobuf, and `lark-websocket-protobuf` compiles its `.proto` at build time.

Runtime settings (provider/model/`models`/base_url/aux_model, maintenance `schedule`,
the opt-in daily `briefing_schedule` + its `briefing_workdays_only` gate, the
`dream_schedule` for usage-driven memory consolidation (on by default, nightly
`0 3 * * *`; set to `"off"` to disable), the
`[channels.*]` tables) live in
`~/.komo/config.toml`; credentials (API keys,
`FEISHU_APP_ID` / `FEISHU_APP_SECRET`, `TELEGRAM_BOT_TOKEN`, `HASS_TOKEN`) only
in `~/.komo/.env`. Priority: built-in defaults < config.toml < `KOMO_*` env
vars. `KOMO_HOME` relocates the whole directory.

**Per-session model + reasoning effort.** `models = ["a", "b"]` (or
`KOMO_MODELS=a,b`, comma-separated) declares the menu a client may switch a
session to; unset it defaults to `model` plus `aux_model`, and `model` is always
force-included first so the running model can't be unselectable. An entry may be
**provider-qualified** (`deepseek:deepseek-chat`) to name a backend other than
`provider`, so one menu spans providers — see the next section. The gateway
advertises the menu over `GET /api/models`
(`api.rs::ModelMenu::from_config`), and a chat turn carries the choice in
`X-Komo-Model` / `X-Komo-Effort`. Unlike the workspace (creation-locked), the
choice is **not** locked: the client sends its current selection every turn and
the gateway validates it against the advertised menu — an unknown value resolves
to the default rather than reaching the provider — then stores it on the session
(`SessionRepository::set_model`, additive `model`/`effort` columns). Sending
*neither* header leaves the stored selection alone, so a third-party
OpenAI-compatible client can't silently reset a conversation. The turn reads the
choice back off the `Session` (`infra/llm.rs::RigLlm::agent_for`), which clones
the per-turn agent and swaps only its model handle (`M::make` over the retained
provider client) plus `additional_params`. Reading it off the session is safe
because every aux path (reviewer, delegate, recall screening, sweeps) builds a
*synthetic* `Session` whose overrides are empty — that invariant is what keeps a
conversation's model from leaking onto the aux model, so preserve it if you add
an aux caller. Effort is provider-specific (`Provider::efforts` advertises,
`infra/llm.rs::reasoning_params` spells it on the wire): openai / openrouter /
codex take `reasoning.effort`; anthropic has no effort scale, so the levels map
onto `thinking.budget_tokens` and `max_tokens` is raised to clear the budget;
DeepSeek advertises **no** levels (its only knob is a thinking on/off flag, and
squeezing three levels onto a boolean would misreport what the model did) and the
UI says so rather than showing a dead switch. A test asserts the two halves agree
— every advertised level must actually map. Because the levels are per *provider*,
`/api/models` reports `efforts` **per entry**, not once per gateway, and the chat
path validates a requested level against *the model that will actually run* — so
switching a session to deepseek clears a stale `high` instead of storing one that
silently does nothing.

**Multiple providers in one menu.** A qualified id routes across backends:
`config::split_model_id` splits `provider:model`, but **only when the prefix names
a known provider** — model ids legitimately contain colons (`llama3:8b`), so
requiring a real provider name is what makes the syntax unambiguous rather than
merely conventional (openrouter's `deepseek/deepseek-chat` uses a slash and is
unaffected). Keys were already per-provider env vars (`Secrets::key`), so nothing
new is needed to authenticate: `ModelConfig.keys` carries every configured one and
`ModelConfig::menu()` **drops** entries whose provider has no credential — offering
a model that errors on every turn is worse than not offering it. The one exception
is the configured `model` itself, which always survives, because hiding what the
gateway is actually running would misreport reality.

`infra/llm.rs::RoutingLlm` is the dispatcher: one type-erased backend per
provider, picked per turn off the session's qualified id. It exists because
`RigLlm<M>` is generic over a *single* provider's model type — within-provider
switching happens inside one `RigLlm` (swap the model handle), but
`deepseek::CompletionModel` and `openai::CompletionModel` are unrelated types, so
crossing providers needs erasure. `build_llm` returns a bare backend for a
single-provider menu and a `RoutingLlm` otherwise, so the common case pays
nothing. An unroutable id falls through to the default rather than failing the
turn (config can change under a stored session). `ModelConfig::for_provider`
builds each backend and deliberately does **not** carry `base_url` to a
non-default provider — it overrides one specific endpoint, and applying it to
every backend would silently point deepseek at an OpenAI-compatible proxy.

**Tool-capable sub-agents with a chosen model** (`tools/delegate.rs`). `delegate
{task, model?}` runs a *real agent turn*, not a bare completion: the full tool
set, so a handed-off subtask can search/read/edit/run — and `model` picks which
model does it (plan on one, apply changes on another). The model travels the same
way a chat session's does: the tool creates a `delegate:<uuid>` session carrying
it and calls `AgentRuntime::handle_input`, so `RoutingLlm` reads it off the row —
no separate plumbing for sub-agents.

What makes that safe is that it **inherits the parent's ambient session context
instead of replacing it**: `handle_input` never overrides an existing one and
`run_agent_loop` reads it, so a sub-agent's side effects prompt the human in the
real conversation through the main approver, resolve against the parent's
workspace root, and stop when the parent turn is cancelled. Recursion is blocked
*structurally* — wiring builds the sub-agent's tool set with `delegate: None`, so
a sub-agent has no such tool, rather than relying on a depth counter. Each
delegation is its own ledger run, so `komo run list` / `run inspect` show exactly
which tools it called. Two consequences to know: session-scoped tools (`todo`) see
the **parent's** session id (a sub-agent shares the conversation's working list),
and `delegate:*` sessions are filtered out of the session list
(`actions.rs::is_subagent_session`) because they are scratch work, not
conversations — the run ledger is the right lens. The unattended **cron** agent
gets no `delegate` (`build_full_tools(..., None)`): the sub-agent runtime carries
the interactive approver, and handing that to a job with no human mixes trust
models — a cron job needing a sub-agent should build its own with the unattended
approver.

**Scheduled cron jobs** (`komo cron`): the gateway runs operator-configured jobs
on cron schedules and delivers the output through the same `HomeNotifier` as
reminders. Jobs live in their own durable **`~/.komo/cron.db`**
(`infra/persistence/cron.rs::CronDb`, domain `komo-core`'s `domain/cron.rs`,
`CronAction` enum) — not in config.toml (an operator accumulates many) and not
in disposable state.db (a job vanishing on a reset means its work silently
stops). Each job is one of two modes (hermes' cron modes, minus the schedule
kinds — 5-field cron only):

- **command** (`komo cron add`, hermes' `no_agent`): run a fixed program,
  deliver its stdout verbatim. Deterministic, no LLM. The command is
  operator-authored (CLI or loopback-gated api — the same trust boundary as
  running the gateway itself), so it executes directly: no shell tool, no
  approver, no `[policy]`. The reliable default for scripts (e.g. alarmhandler).
- **agent** (`komo cron add-agent`): run a prompt through an **unattended,
  tool-capable agent turn** (wiring's `cron_runtime`), deliver the reply.
  Optional `--skill` names are loaded first (progressive disclosure). The agent
  has the **full tool set** (shell/file/skill/web/…), but side effects use the
  briefing's unattended safety model — a `PolicyApprover` over a deny-all inner,
  so a `Risk::Normal` action passes **only** through an `unattended = true`
  `[policy]` rule. Main model, no memory enricher, per-run session
  (`cron:<name>:<unix>`) so each run is an isolated, run-ledgered turn.

```bash
# command mode — deterministic script
komo cron add weekly-alarmhandler-rotation "0 14 * * 5" \
  /path/to/alarmhandler_no_agent_notify.py -- --flag   # args after `--`
  # [--workdir /path] [--timeout-secs 900]
# agent mode — prompt + optional skills, runs an unattended agent turn
komo cron add-agent morning-brief "0 8 * * *" "总结我今天的日程和待办" \
  --skill calendar --skill weather
komo cron list                    # jobs (kind + last-run status) and reminders
komo cron run <name>              # fire now (due on the next sweep tick)
komo cron enable|disable <name>   # keep but stop scheduling / resume
komo cron remove <name>
```

Execution is one every-minute `CronJobSweep` (`agent/daemon.rs`) reading the
store per tick — jobs added/removed/toggled while the gateway runs take effect
within a minute, no restart. The sweep **claims before running** (advances
`next_run_at` first), so a crash mid-run never re-fires a slot and a
longer-than-a-minute job is never double-started; a gateway asleep over a slot
runs the job late, once (computed from now, like recurring reminders).
Validation (cron parse, key-shaped unique name — `domain::cron::valid_cron_job_name`
— per-kind required fields) lives in the
shared operator action (`operator_control/actions.rs::add_cron_job`), so the CLI,
api, and `cron`-tool paths can't fork; `komo cron` works with the gateway up (routed to
`/api/cron/*`; writes loopback-gated) or down (direct `cron.db` open). Every
outcome is delivered, success and failure alike, and recorded on the job
(`last_status`/`last_error`, shown in `komo cron list` and `komo doctor`) — a
failed command/turn still returns `Ok` (the operator was told; breaker cooldowns
are meaningless on a weekly cron), only delivery failure fails the cycle. Command
jobs aren't run-ledgered (sweeps run outside any session); agent jobs are (they
run a real turn on `cron_runtime`, so `komo run list` shows them).

**Scheduling from a conversation** (`tools/cron.rs::CronTool`): the same store,
the agent's surface — `list` / `add` / `remove` / `enable` / `disable` / `run`,
over the same shared operator actions, so a chat-created job is indistinguishable
from a `komo cron add` one once stored. What differs is *authorship*: a CLI job
is operator-authored by construction, a chat job is model-authored, so the tool
moves the human decision to creation time and gates every mutation through the
`Approver` — an agent job at `Risk::Normal` (scope `cron:add`), management
actions at `Risk::Normal` (scope `cron:manage`), and a **command** job at
`Risk::Dangerous` carrying an `ActionRef::Shell` (approving it approves every
future unattended execution, and no ordinary shell *allow* rule can grant it —
`include_dangerous` is required, while a shell deny rule still fences it). The
tool is part of `build_full_tools`, so the unattended `cron_runtime` has it too:
a job can reschedule itself, subject to the same `unattended = true` policy gate
as any other side effect. `CRON_GUIDANCE` in the system prompt owns the routing
rule the model needs — recurring *work* is a cron job, a recurring *message* is a
`reminder`.

The `codex` provider (`provider = "codex"`) is the exception to the API-key
rule: it has no env key, authenticating instead from the Codex CLI's OAuth login
at `~/.codex/auth.json` (run `codex` to create it; `$CODEX_HOME` honored). See
`infra/codex.rs` in the Architecture section.

Home Assistant keeps its URL and token in `.env` as a single self-contained
block: `HASS_TOKEN` (required — a long-lived access token) and `HASS_URL`
(optional, defaults to `http://homeassistant.local:8123`). These are shared by
both HA surfaces. No token = neither the `homeassistant` tool nor the channel
loads.

```bash
# ~/.komo/.env
HASS_TOKEN=your-long-lived-access-token
HASS_URL=http://192.168.1.100:8123   # optional; omit for homeassistant.local:8123
```

The `homeassistant` **tool** (agent controls HA) registers automatically once
`HASS_TOKEN` is set — no config.toml needed. The HA **event channel** (HA
pushes device events to the agent) is opt-in via `[channels.homeassistant]`,
which carries only event-filter behavior (URL/token still come from `.env`).
Forwarding is closed by default — set at least one of `watch_domains` /
`watch_entities` / `watch_all`:

```toml
[channels.homeassistant]
enabled = true
watch_domains = ["binary_sensor", "lock", "alarm_control_panel"]
watch_entities = ["cover.garage_door"]
ignore_entities = ["binary_sensor.always_chatty"]
watch_all = false            # forward every entity (overrides the watch lists)
cooldown_seconds = 30        # per-entity min seconds between forwarded events
```

Config resolution: `src/config/` resolves everything **once** into a
`ConfigSnapshot { runtime: RuntimeConfig, report: ConfigReport }`.
`sources.rs` reads the three raw sources one time (dotenvy loads `.env` into
the process env in `main.rs`; envy deserializes `KomoEnv` for `KOMO_*` and
`Secrets` for every credential; `FileConfig` parses config.toml) and
`resolved.rs` applies precedence/defaults purely — `from_sources` is the test
seam, so resolution tests never touch the real env. Resolution **never
aborts**: problems become `ConfigIssue`s in the report (with per-value
provenance, secrets redacted to presence-only), and startup paths fail fast via
`validate_agent` (env/model issues — wiring calls it) or `validate_gateway`
(any fatal issue, e.g. an enabled channel missing its credential —
`gateway::run` calls it). Two deliberate exceptions: a **missing model API key
is a warning, not fatal** — a fresh install (first Docker boot, before `komo
init` + a key exists) must boot rather than crash-loop, so `build_llm` degrades
to an `UnconfiguredLlm` whose every call errors with the fix, and that message
reaches the user as the turn's reply. And **`[channels.homeassistant]` enabled
without `HASS_TOKEN` is also a warning** — HA is a local convenience
integration, so the gateway boots with the channel offline (`Misconfigured`,
skipped by wiring's `.ready()`) instead of taking every other channel down. `komo init` scaffolds `config.toml` +
`.env` (`cli/init.rs`, pure file ops, never overwrites). `cli/app.rs` loads the snapshot once per invocation
and threads `&ConfigSnapshot` to chat/gateway/doctor/model/policy; channel
tri-state (disabled / ready / misconfigured) is `ChannelState<T>`. Never
re-read config.toml or call `std::env::var` in callers — the only exception is
`KOMO_HOME`, the bootstrap variable that locates `.env` itself.

Channel declarations follow hermes-agent's per-platform block shape — behavior
keys in the table, credentials in env:

```toml
[channels.feishu]
enabled = true
allow_from = ["ou_xxx"]   # pre-trusted sender open_ids (skip pairing)
require_mention = true     # group messages must carry an @mention (DMs bypass)
home_chat = "oc_xxx"      # optional: reminders go here instead of macOS notifications

[channels.telegram]
enabled = true
allow_from = ["123456789"]  # pre-trusted sender user-ids (skip pairing)
allowed_chats = ["-100123"]  # group chat-id allowlist (empty = any group; DMs always pass)
require_mention = true       # group messages must @mention the bot (DMs bypass)
home_chat = "123456789"     # optional: reminders go here instead of macOS notifications

[channels.wechat]
enabled = true
allow_from = ["wxid_xxx"]   # pre-trusted iLink user-ids (skip pairing)
home_chat = "wxid_xxx"      # optional: reminders go here instead of macOS notifications
```

WeChat (微信) has no credentials in config.toml or `.env`: login is QR-based and
the iLink token is stored in `~/.komo/wechat/credentials.json`. Provision it
once on the host with `komo channel wechat login` (scan the QR with the WeChat app); the
gateway can't render a QR, so its `[channels.wechat]` is **inert until those
credentials exist**. WeChat is DM-only (an iLink bot identity can't join ordinary
groups), so there is no `require_mention`/`allowed_chats` — pairing is the only
admission control. Proactive output (reminders/briefing) reaches a WeChat user
only after they've messaged the bot since the gateway started — see the channel
note below.

When multiple channels set `home_chat`, feishu takes reminder delivery. The
config `home_chat` is only a fallback: the `/sethome` chat command sets the home
channel at runtime (persisted in the db), and that override wins. See the
`HomeNotifier` in the gateway section below.

Senders outside `allow_from` must pair before the agent talks to them: their
first message gets a pairing code as the only reply, and someone with shell
access to the host runs `komo pair approve <code>`. Pairing is hardened after
hermes' `pairing.py` (`domain/pairing.rs`): the code is stored only as a salted
SHA-256 hash (never plaintext, so `komo pair list` shows pending/approved but
not the code — get it from the sender), a sender is issued at most one fresh
code per 10 min (`PAIRING_RATE_LIMIT_SECS`; codes still expire after 1h), at
most 3 senders may await approval per platform (`MAX_PENDING_PER_PLATFORM`), and
the approve path locks for 1h after 5 wrong codes (`APPROVE_MAX_FAILURES`).
`komo pair revoke <id>` un-pairs. Approval is written to the shared db, so it
takes effect on the sender's next message without a gateway restart.

## Architecture

Personal Agent framework v0.1, implemented in Rust. The codebase follows a DDD-style layered architecture.

**Request flow:**
```
CLI/channel → AgentRuntime ─ run_agent_loop ─┬→ LlmClient::begin_turn → TurnDriver (ONE rig completion / round)
                                             └→ ToolExecutor::execute_round → tools   (loop until Step::Final)
                          ↘ MessageRepository · RunRepository (ledger) → Response
```
komo owns the tool loop (roadmap §7): `AgentRuntime::run_agent_loop` drives the model one
round at a time via `LlmClient::begin_turn` — rig performs a **single** completion per round,
not its own multi-step loop — and hands each round of requested tools to the `ToolExecutor`,
threading the results back until the model returns a final answer. A hard per-turn round
budget (`max_turns`) forces a clean final answer once exceeded. There is still no separate
planner *type* — the loop is this one method, which is where control points (budget and
cancellation today;
clarify/resume next) live.

**Layers and their responsibilities:**

`domain/` — pure interfaces, no I/O, no external crates
- `repository.rs` — `SessionRepository` (find/save) and `MessageRepository` (list_by_session/save); the two traits `AgentRuntime` depends on
- `tool.rs` — `Tool` trait (name / description / execute / optional `redact_args` / optional `idempotent`); `idempotent` (default `false`) opts a read-only tool into retry on an ambiguous transient failure — see `tool_registry.rs`
- `message.rs`, `session.rs` — core value types

`infra/` is layered by concern: `infra/messaging/` (ingress channels, outbound
senders, proactive notifiers), `infra/memory/` (the memory.db connection +
legacy markdown store), `infra/persistence/` (the toasty-backed state.db /
kanban.db connections), and `infra/llm.rs` (the LLM backend) as a cross-cutting
file at the top level.

`infra/persistence/db.rs` + `infra/persistence/kanban.rs` + `infra/memory/memory_db.rs` — the only places toasty (ORM) appears. The backend is the **Turso engine** (`toasty-driver-turso`, the pure-Rust SQLite rewrite — no `rusqlite`/C dep), opened in **MVCC concurrent-write mode** (`Turso::file(p).concurrent_writes()`)
- `Db` (`infra/persistence/db.rs`) holds `Arc<toasty::Db>` over `state.db`; implements every repository trait *except* `TaskRepository`/`MemoryRepository`/`SkillRepository` (sessions, messages, reminders, session todos, pairings, settings, the **run ledger** `RunRepository`). Skills moved to files (`infra/skills.rs`); the `SkillRecord` table stays in the schema only so `export_legacy_skills` can read old dbs for the one-time candidate import
- `KanbanDb` (`infra/persistence/kanban.rs`) is a second, independent connection over `kanban.db`; it holds only `TaskRecord` and implements `TaskRepository`. Separate file = durable tasks survive a `state.db` reset
- `MemoryDb` (`infra/memory/memory_db.rs`) is a third, independent connection over `memory.db`; it holds only `MemoryRecord` and implements `MemoryRepository`. On first run it seeds itself from legacy `~/.komo/memory/*.md` via `import_legacy_markdown` (no-op once populated)
- **connection pool, no global lock**: `toasty::Db` is itself a deadpool-backed pool, so each repository method does `self.inner.connection().await?` and runs on its own pooled `Connection` (`Connection: Executor`) — independent reads/writes run concurrently. No `Arc<Mutex>`. Pool size is `DEFAULT_POOL_SIZE` (`infra/persistence/mod.rs`)
- **MVCC writes retry**: under `concurrent_writes`, conflicting commits fail and must be retried by the caller. Every **single-write** mutating repository method (the run ledger — a round's tool calls run in parallel — plus message/task/memory saves, and the skill/reminder/session-todo/pairing/home upserts) wraps its body in `with_write_retry` (`infra/persistence/mod.rs`), which re-runs the whole closure on a busy/conflict error. **Multi-write** methods (`rotate`, `prune`, `reconcile_interrupted`, pairing `approve_code`) wrap their statements in a real toasty transaction (`conn.transaction()` → `.commit()`; drop = rollback) *inside* `with_write_retry` — so a mid-sequence failure or lost MVCC commit rolls the whole sequence back and the retry re-runs it cleanly, never double-applying. (`delete_empty_sessions` stays a plain loop — its per-row deletes are independent and idempotent.) `SessionRepository::save` is an idempotent create: it pre-checks existence and inserts only when absent (retrying conflicts), rather than the old `let _ = create!(…)` that swallowed *every* error — including a conflict that left the session uncreated and the next message save failing with a phantom "session not found". MVCC rejects `AUTOINCREMENT`, so every key is a `String` (UUIDv7 via `uuid::Uuid::now_v7()`), never `#[auto]`
- **sqlite→turso migration**: a legacy rusqlite-written file is staged aside to `<name>.sqlite-backup` (`stage_sqlite_backup`), its rows extracted via the still-enabled `sqlite` driver and reloaded into a fresh Turso db, then a `<name>.turso` marker is written so it never re-migrates. Durable data (memory.db, kanban.db) migrates its rows; disposable `state.db` is just staged aside and rebuilt. Both `sqlite` and `turso` toasty features stay enabled (the former only to read backups)
- all: `connect(url)` checks if the db file exists; calls `push_schema()` only for new databases (toasty's `push_schema` is not idempotent — no `IF NOT EXISTS`; adding a table to an existing file means deleting it, or the `.sqlite-backup`/`.turso` sidecars, to rebuild)
- toasty model structs are private to their file
- DB URL format: `turso:<path>` (single colon); `turso::memory:` for in-memory. The old `sqlite:<path>` form is still understood by the migration's backup reader

`agent/runtime.rs` — application logic
- `AgentRuntime` holds `Arc<dyn LlmClient>` + a `ToolExecutor` (the loop hands it each round of requested calls) + `max_turns` + `Arc<dyn SessionRepository>` + `Arc<dyn MessageRepository>` + `Arc<dyn RunRepository>` — no knowledge of toasty
- `handle_input` owns the session lifecycle: load-or-create, append the user message, run the turn, persist the reply
- `turn_body` loads only a **recent window** of the transcript (`SessionRepository::find_windowed(id, history_window)`, where `history_window` mirrors the LLM's `max_history_messages`; `0` = whole transcript) — so a long-lived chat session no longer deserializes its full history every turn. The LLM windows again to the same bound, so this is loss-free. The periodic reviewer cadence is driven by `MessageRepository::count_user_turns` (a cheap `COUNT(*)`, since the windowed in-memory count would plateau and mis-fire the modulo), and when it fires the reviewer is handed a **full** reload via `find` (it needs the whole transcript, not the working window)
- `run_agent_loop` is komo's own tool-calling loop (roadmap §7): `llm.begin_turn` → `first()` → on `Step::ToolCalls`, hand the whole round to `ToolExecutor::execute_round` with an explicit `ToolTurnContext` (the run handle this turn opened + the session context, read once at the loop's start — the one ambient-to-explicit bridge) → `step(outcomes)` → repeat until `Step::Final`. Tool errors and unknown names come back as outcome content (the model recovers); only a driver/LLM error aborts the turn. Past the `max_turns` round budget it feeds a "budget reached" note in place of results and forces a final answer
- `run_turn` wraps each turn in one ledger `Run` (open → pass a `RunContext` explicitly into `turn_body` + a `run` tracing span → finalize with status/output/error). There is no run task-local: ledgering never depends on ambient scope. All ledger writes are best-effort (logged, never change the turn result). `Run.plan` is a post-hoc summary derived from the recorded step count ("respond" or "<n> tool call(s)")

`domain/llm.rs` — `LlmClient`: `complete(&Session) -> String` (one-shot, tool-less — delegate/reviewer/briefing) plus `begin_turn(&Session) -> Box<dyn TurnDriver>`, the seam `run_agent_loop` drives. A `TurnDriver` yields the turn's rounds as `Step` (`Final(String)` | `ToolCalls(Vec<ToolCallReq>)`) and takes `ToolOutcome`s back — all rig-agnostic. `begin_turn` has a default impl (a one-shot driver wrapping `complete`) so tool-less backends and test stubs need only `complete`

`infra/llm.rs` — `RigLlm<M>`: `LlmClient` backed by the `rig` framework (`rig-core`, aliased as `rig`)
- `build_llm` constructs it for the configured provider (deepseek/openai/anthropic/openrouter/**codex**), exposing the tool catalog via function calling; it takes `Option<Arc<MemoryEnricher>>` (`Some` = main agent) rather than raw memory/aux handles
- `assemble` (shared by `complete` and `begin_turn`, run **once per turn**) splits the session into prompt + history, rebuilds the tiered system prompt, and appends the finished memory prefix from the `MemoryEnricher` (main agent only) — the adapter never sees memory selection, screening, rendering, or usage tracking. The stable tier carries an operator-authored **user profile** (`~/.komo/USER.md`, hermes' USER.md analog; main agent only via `SystemPromptBuilder::user_profile()`, re-read on mtime like `SOUL.md`) — deliberately kept separate from the memory-derived pinned/recall blocks (different tier, operator-trusted vs untrusted-data-flagged, hand-authored vs pinned-during-use), so the two profile sources never dedup-fight. `komo init` scaffolds an empty `USER.md`; churny facts still belong in memory.db/AGENTS.md, not the profile
- there is no rig `Agent` in the picture: since rig 0.41 a configured `Agent` runs exclusively through rig's own `AgentRunner` loop, and komo owns the loop, so `RigLlm` holds the provider's `CompletionModel` handle directly plus what the `Agent` used to carry for us (retained client for per-session model minting, tool schemas). A per-turn `TurnModel` bundles {model handle, preamble, `max_tokens`, `additional_params`} and builds each round's request off `model.completion_request(prompt)`
- `begin_turn` returns a `RigTurnDriver` that owns that `TurnModel` + the growing history; each round is one `complete_once` (a single provider completion — komo owns the loop). It echoes the assistant turn back verbatim (text + tool calls + reasoning) and threads tool results via rig's own `UserContent::tool_result[_with_call_id]` (preserving both `id` and `call_id` so Anthropic and OpenAI-style providers both validate). `complete` is the same round with **no tools declared** — enforced there rather than assumed of the aux callers, since nothing on that path would dispatch a call
- the `stream` flag (set only for the Codex provider) flips `complete_once` to the **streaming** transport, aggregating the streamed deltas back into the same `(choice, message_id, usage)` triple `send()` yields — the Codex backend rejects non-streamed requests, everyone else keeps the one-shot path

`infra/codex.rs` — the **Codex provider** (`provider = "codex"`), borrowed from hermes-agent's `openai-codex` OAuth path. Codex models run on the ChatGPT backend (`https://chatgpt.com/backend-api/codex`, an OpenAI **Responses API** surface), authenticated not with an env API key but with the OAuth tokens the official Codex CLI writes to `~/.codex/auth.json` (`$CODEX_HOME` honored). `CodexAuth` reads that token set, decodes the access-token JWT to know when it's expiring, and refreshes it against `auth.openai.com/oauth/token` (Codex CLI's pinned client id), writing the result back to `auth.json` so the CLI and komo stay in sync. `CodexHttpClient` is a custom `rig` `HttpClientExt` backend that, on **every** request: re-stamps a freshly-resolved `Authorization: Bearer` (so a long-running gateway survives the hourly token rotation), and reshapes rig's Responses body for the picky Codex backend (`adapt_codex_body`: lift the `system` message into the required top-level `instructions`, force `store: false`). Static Cloudflare-dodging headers (`originator: codex_cli_rs`, codex-shaped `User-Agent`, `ChatGPT-Account-ID` from the JWT) are baked into the client's default headers in `build_llm`; the SSE response, which the backend serves without a `Content-Type`, is stamped `text/event-stream` so rig's stream reader accepts it. No env key: `Provider::uses_api_key()` is false for Codex, so `ModelConfig::resolve` leaves `api_key` empty and `komo doctor` validates `~/.codex/auth.json` instead. Default model `gpt-5.5` (account-/tier-dependent — others seen: `gpt-5.4`, `gpt-5.4-mini`; discover live at `GET /codex/models`), overridable via config `model`.

`services/tool_execution/` — the tool-execution module (deepening plan §6): `ToolExecutor` owns the whole pipeline the loop used to assemble by hand
- `ToolExecutor::execute_round(calls, &ToolTurnContext)` is the external interface: one round of model-requested calls in, order-preserving `ToolOutcome`s out (run concurrently — the interactive approver serializes prompts per session, so approvals stay safe). Unknown tools and tool errors become outcome content the model can recover from. `definitions()` is the read-only catalog view `build_llm` uses for function-calling schemas
- inside, each call runs the invariant order: claim a ledger seq (the per-turn call budget counts logical calls, not retry attempts) → redact args (`Tool::redact_args`) → execute on a panic-catching task with the session context installed and a `tool` tracing span → map panic/cancel to errors → **transient-error retry** (typed `RetryHint` from `TransientError` preferred, text markers as fallback; connection-level failures retry any tool, ambiguous ones only `Tool::idempotent()` tools, terminal never; retries collapse into one ledger step) → **bound the LLM-facing result** (`services/tool_output_store.rs`: over-limit output is written out in full and previewed head+tail; without a store or a ledger seq it degrades to the old one-sided truncation) → record the `RunStep` best-effort, including the tool's `structured` view and any stored `output_paths` — the ledger keeps the *original* result, so the audit trail holds what the model was not shown
- execution policy is **instance-owned** `ToolExecutionConfig` (`max_result_bytes` from config's `max_tool_result_bytes`; `max_calls_per_turn` = 500 backstop; `max_call_duration` from `tool_timeout_secs`) — no process globals; two executors can carry different policies. The per-call timeout is a **default**, not a law: `Tool::max_duration()` overrides it per tool, because the config value exists to catch a *hang* and several tools legitimately wait — `delegate` runs a whole sub-agent completion (10 min), `shell` honors its own `timeout` argument (up to 10 min, so its ceiling sits above that), and every approval-gated tool must outlast the 5-minute chat approval prompt (`domain::tool::APPROVAL_BOUND`) or it would abort *while the user is still deciding*
- context is **explicit**: the runtime passes `ToolTurnContext { session, run: Option<RunContext> }` per turn, and each call gets a `ToolContext { session, run, approver }`. `Tool::call(Value, &ToolContext)` is the **only** tool entry point — the old string-in/string-out `Tool::execute` and its bridge are gone. The `SESSION` task-local now serves **only the approvers** (`ChatApprover` / `PolicyApprover` resolve a prompt against the current conversation without a context parameter on the domain `Approver` trait): the dispatcher / api / `handle_input` establish it and the executor re-installs it around each spawned tool task. No tool reads it — session-scoped tools (`todo`, `memory`) take `ctx.session`
- the executor is the **only** thing that runs a tool. `build_llm` takes `definitions()` and maps it to rig `ToolDefinition`s — name, description, parameter schema — so all the provider ever gets is the *declaration*; there is no second execution path to keep in sync (rig's `ToolDyn` adapter and its fallback `call` are gone as of rig 0.41, where an `Agent` runs only through rig's own `AgentRunner` loop and komo talks to the `CompletionModel` directly)

`services/tool_output_store.rs` — over-limit tool output kept on disk instead of
thrown away. The result cap used to be a **one-sided** truncation, and the part
that answers the question (a test run's failure summary, a stack trace's innermost
frame, a compiler's error count) is usually at the *end*. So an over-limit result
is written in full to `<komo home>/tool-output/<session>/<run-seq>.txt` and the
model gets a **head + tail preview** naming that file (ends sampled by whole
lines, falling back to char-boundary byte slicing for a single minified line).
Three deliberate edges: the store is skipped **without a ledger seq** (aux
sub-agents, sweeps — no run to point an operator at, no follow-up turn to read
it, so a file nobody opens is litter); a write failure degrades to the old
truncation, because a full disk must not fail a working tool call; and the
directory is a **read-only root** of the `Workspace`
(`with_readonly` → `resolve_readable`, used by `read`/`grep` via
`fs_common::resolve_readable`) so the path in a preview is one the model can
actually page or search, while every mutating tool still resolves against the
workspace roots alone and refuses it. Retention is 7 days, swept once at gateway
startup plus at most hourly from inside the store — deliberately **not** a cron
schedule: expiring a scratch file does not need to happen on the minute.

`tools/time.rs` — first built-in tool; returns RFC 3339 UTC timestamp

`tools/shell.rs` — `sh -c` behind the approver, with a **hardline floor** of
commands no approval unlocks (`rm -rf /`, `mkfs`, …) and a dangerous-pattern list
that escalates to a `Risk::Dangerous` prompt. Takes a model-supplied `timeout`
(ms, default 2 min, max 10 min) and `workdir` (workspace-confined), and reports
`structured = {exit, truncated, timeout}` alongside the prose. The child runs in
its **own process group** (`process_group(0)`) so a timeout `killpg`s the whole
tree — killing `sh` alone left the processes it started running, holding the
pipes; there is a regression test for exactly that. Two nested clocks, on
purpose: this tool's own timeout fires first with an actionable "retry with a
bigger timeout", and the executor's (`Tool::max_duration`, set above this tool's
maximum) is only there if the inner one somehow doesn't.

`tools/grep.rs` + `tools/glob.rs` + `services/search.rs` — content and filename
search, built on **ripgrep's own libraries** (`ignore` + `globset` +
`grep-searcher`/`grep-regex`) rather than shelling out to an `rg` binary: komo
ships as a single binary and can't assume one exists. `services/search.rs` is the
blocking walk/match layer (the tools call it inside `spawn_blocking`), and it is
deliberately split into `candidates` (which paths) and `search_files` (what's in
them) so the permission policy runs over the paths **before any content is
read** — a `category = "file", access = "read"` deny rule therefore stops `grep`
from opening the file, not merely from printing it. The walk honors
`.gitignore`/`.ignore` (with `require_git(false)`, since a komo workspace need not
be a repo) and skips hidden entries, `.git/` and binaries, so results don't fill
up with `target/` and `node_modules/`. `glob` returns paths newest-first; `grep`
copies v2's output shape (`Found N matches`, then `path:` blocks of
`Line N: text`, indentation preserved). Both are `Risk::Safe` + `idempotent`.

`tools/edit.rs` + `tools/apply_patch.rs` + `services/diff.rs` +
`services/patch.rs` — the precise-mutation pair. `edit` replaces an **exact**
string: it refuses an ambiguous match (with the count) or a missing one (telling
the model to copy the text verbatim) rather than guessing — **no fuzzy
matching**, the same call v2 made. It matches in the file's own line-ending
style (a model that read a CRLF file still sends `\n`) and preserves a BOM.
`apply_patch` applies v2's `*** Begin Patch` envelope (`services/patch.rs`, ported
from its `patch.ts`): add/update/delete across files, **one approval for the whole
blast radius** (`fs_common::allow_write_batch` — one prompt, then a policy-only
`Risk::Safe` re-check per remaining path, so a deny rule on any target blocks the
batch before a byte is written). The format carries no line numbers, so chunks are
located by context with a progressively looser comparison ladder (exact →
trailing-whitespace → trimmed → punctuation-folded). There is **no rollback**: a
mid-patch failure leaves earlier operations applied and says exactly which, since
a model that doesn't know what landed makes things worse. Both tools go through
`file_mutation::write_if_unchanged` and report a unified diff + line counts via
`services/diff.rs` (counts inline for the model, full patch in `structured`).

`tools/read.rs` + `tools/write.rs` + `tools/fs_common.rs` — the filesystem pair
(replacing the old single `file{action}` tool, opencode-v2 shaped). `read` pages a
text file by 1-based `offset`/`limit` (≤2000 lines or 50 KB per page, whichever
comes first) with a line-number gutter and a "continue with offset=N" hint, lists
a directory, truncates a single overlong line (≥2000 chars, so one minified
bundle line can't eat the page), and **refuses binaries** (extension table +
image magic bytes + NUL/non-printable ratio) and invalid UTF-8 with an
explanation rather than a lossy-decoded page — the old tool read whole files and
cut them off at 64 KB, so the tail of anything larger was unreachable. `write`
creates/overwrites with `Risk::Normal` approval and drops the `content` body from
the run ledger. `fs_common` holds the shared order every fs tool follows: resolve
the path against the `Workspace` (relative paths anchor to its root; an escape is
`ToolError::Denied` — a floor, not a prompt) → ask the approver with the right
`ActionRef::File{write}` so `[policy]` `category = "file"` / `access` rules keep
matching → render a refusal the model can act on. Writes go through
`services/file_mutation.rs`: `snapshot` before prompting, `write_if_unchanged`
after, so a file edited *during* a slow chat approval is not silently
clobbered (opencode v2's `writeIfUnchanged`; a UTF-8 BOM present before the write
is re-applied). The guard closes the approval window only — there is deliberately
no "the model must have read the file first" rule

`tools/web_fetch.rs` — a GET behind an `ActionRef::Network` `Risk::Safe` check
(deny-only, see the policy section). Three things it is deliberate about:
**`format`** (`markdown` default / `text` / `html`) picks both the `Accept` header
and how a `text/html` body is rendered — `render_html` is a small hand-rolled
walker that keeps headings, list items, fenced `<pre>`, inline `<code>` and link
targets, chosen over an html5ever-based converter to keep the single binary's
dependency tree lean (swapping it out is one function). **Content-type gating**:
a raster image (SVG excluded — it's text) or any non-textual mime is refused with
a named error rather than lossy-decoded, because a PDF run through `text()` puts
a screen of replacement characters into the transcript for the rest of the
session. **Size** is bounded only at *download* (256 KB — a declared
`Content-Length` over it fails before a byte is read; a chunked body is kept up
to the limit with a marker saying so). It does **not** trim for the model: that
is the executor's single choke point (`max_tool_result_bytes`), so raising the
model's view is one config value, in one place.

`tools/homeassistant.rs` — `HomeAssistantTool`, the Home Assistant integration (reaches a smart-home instance over its REST API, 15s timeout). Four actions: `list_entities` (read; optional `domain` prefix + `area` filter), `get_state` (read one entity), and `list_services` (discover callable services per domain) are read-only; `call_service` (turn devices on/off, etc.) is side-effecting → gated through the shared `Approver` as `Risk::Normal` with a `homeassistant:{domain}.{service}` scope key (approve-for-session). Two safety floors *below* approval (HA has no service-level access control of its own): `domain`/`service`/`entity_id` are shape-validated (`valid_name` / `valid_entity_id`) to block path-traversal/SSRF in the request path, and a `BLOCKED_DOMAINS` list (`shell_command`, `command_line`, `python_script`, `pyscript`, `hassio`, `rest_command`) is refused outright — no approval unlocks it, like shell's hardline list. Registered only when `HASS_TOKEN` is set (`HASS_URL` optional, defaults to homeassistant.local:8123; resolved by `config::homeassistant_config`, wired in `cli/wiring.rs`)

`infra/messaging/homeassistant.rs` — **removed (2026-08-08).** HA was an event-ingress channel: it opened HA's WebSocket API, subscribed to `state_changed`, and dispatched each qualifying event as one turn under session `homeassistant:events`. Forwarding was closed by default (`watch_domains`/`watch_entities`/`watch_all` + a per-entity `cooldown_seconds`), but the shape was wrong regardless of tuning: every event cost a full LLM turn, and the reply went back to HA's notification drawer rather than the operator's real proactive channel. The `homeassistant` *tool* supersedes it — the agent pulls state on demand, and recurring reactions are written as HA automations via `save_automation`, so the rule runs inside HA at zero token cost and survives komo being down.

`domain/policy.rs` + `agent/policy_approver.rs` — the **configurable permission policy** (roadmap §3): a pure rule engine deciding whether a side-effecting action auto-allows, hard-denies, or escalates to the interactive approver
- `[policy]` + `[[policy.rule]]` in config.toml (parsed by `config::policy_config` / `policy_report`; invalid rules ignored with a warning, absent table = empty policy = ask-for-everything, never more permissive). Rule fields: `category` (shell/file/network/homeassistant/mcp/wiki), `match` (prefix/suffix/exact/contains), `value`, `effect` (allow/deny), optional `access` (file read/write), `channels` scope, `include_dangerous`, `unattended`. Omitting **both** `match` and `value` is the whole-category wildcard (`Matcher::Any`); a `match` *with* an empty `value` stays invalid, so a typo can never silently widen into "everything"
- `PolicyApprover` (same decorator shape as `WorkdayGated`) wraps `CliApprover`/`ChatApprover` in `cli/wiring.rs`: `Policy::decide` runs first, the inner approver only on `Ask`. Deny beats allow regardless of order; `Risk::Dangerous` auto-allows only via `include_dangerous`. **Unattended contexts** (the cron and briefing runtimes) grant only through an allow rule explicitly marked `unattended = true`; a `default_normal = allow` degrades to Ask there (`Policy::decide` enforces this in the engine, so `komo policy check` without `--channel` shows the real unattended verdict). What marks a turn unattended is **`SessionContext::origin`** (`SessionOrigin::Cron` / `Briefing`, set by the sweep around its `handler.handle(..)` call), which `PolicyApprover` turns into `channel = None`. It is deliberately *not* "has no ambient session": those turns own a real session id (`cron:<job>:<unix>`, `briefing:<date>`) for the ledger and session-scoped tools, and `handle_input` would build a plain detached context for them anyway — so deriving attendance from session presence silently handed the engine a `cron` channel and skipped the whole unattended branch
- **read-only actions are deny-only**: `web_fetch` (`ActionRef::Network`) and `read` (`ActionRef::File{write:false}`) consult the approver at `Risk::Safe` — a deny rule can blackhole hosts (matched on the URL host at dot boundaries, so `suffix github.com` ≠ `evilgithub.com`) or fence paths (`access = "read"`), but nothing ever prompts for a read and unmatched reads stay allowed (allow rules are meaningless there). This is the exfiltration guard: untrusted page content steering the model into fetching an attacker host is blockable in config
- **a wholly-denied tool never gets advertised**: wiring calls `ToolExecutor::drop_policy_denied` right after registration, so a tool whose whole category is denied by an unscoped wildcard rule (`Policy::wholly_denied`) leaves the catalog — no function schema, no entry in the prompt's tool list, no round-trip spent on a call that was certain to be refused (opencode v2's `whollyDisabled`). Deliberately conservative, and asymmetric on purpose: a **channel-scoped** or **value-scoped** deny keeps the tool (it can still act somewhere, and a refusal the model is told about beats a capability it can never discover). `file` splits by `access`, so denying writes takes `write`/`edit`/`apply_patch` and leaves `read`/`grep`/`glob`. The name→category map is `tool_execution::policy_scope`; a tool missing from it is never filtered. Since both the prompt list and the schemas read the *same* filtered catalog, they can't disagree — and because filtering happens once at wiring, the cache-stable prompt tier stays stable
- **saved grants** (`~/.komo/permissions.json`, `infra/permissions_store.rs`): the approval prompt's fourth answer — `y` once / `s` this session / **`a` from now on** / `n` — accumulates narrow allow rules so a recurring approval stops recurring across restarts. The full ladder, strongest first: **tool hardline floor > config `[policy]` deny > saved grant > config allow / `default_normal` > interactive ask**. Three floors a saved grant can never cross, all enforced in `Policy::decide` rather than at the call site: it never outranks a deny rule or a tool floor; it never covers `Risk::Dangerous` (that stays a config-only `include_dangerous` opt-in — "remember this" must not make a dangerous action silent); and it is **not read in an unattended context** (no channel — cron/sweeps/briefing), since it was accumulated interactively. Wiring reinforces the last one by handing the saved list only to the interactive `PolicyApprover` (`wrap_with_store`), never to the cron/briefing ones
- the entry a grant saves is the **narrowest rule that would match again** (`Rule::narrowest_for`): `shell` → the command's first token (`cargo build` → prefix `cargo `), `file` → the parent directory + the read/write kind, `network` → the host as a dot-boundary suffix, HA → the exact `domain.service`; always scoped to the answering channel, so a CLI grant never speaks for a chat. The prompt **spells the rule out** before the key that saves it (and omits the key entirely when there is nothing to generalize, or for a dangerous action), because the operator has to see how wide the grant is to judge it. Schema is isomorphic with `[[policy.rule]]`, so a saved entry is just a runtime-accumulated allow rule sharing one matcher — which is why `komo policy check` can explain it the same way
- **one writer**: only `PolicyApprover` persists. The three interactive approvers (`CliApprover` / `ChatApprover` / `TuiApprover`) merely report `Decision::AllowAlways`, so they can't drift on what "always" means; the store's in-memory list is *shared* with the `Policy` (`SavedRules = Arc<RwLock<Vec<Rule>>>`), so a grant applies to the next decision with no restart. JSON not a fourth db: few entries, and the operator should be able to read and delete them in an editor. Its own file not `state.db`: a grant is durable personal data, like memory.db / kanban.db / cron.db
- layering: the policy sits *above* each tool's hardline floor (shell's refused patterns, HA's `BLOCKED_DOMAINS`) — those short-circuit inside the tool, so no `Allow` rule can unlock them; policy only tightens, never loosens
- operator surface: `komo policy list` (config rules **and** saved grants, in evaluation order) / `komo policy check` (says whether a config rule or a saved grant decided) / `komo policy saved list|forget` — all `cli/policy.rs`, pure file parsing, no db/gateway — plus a saved-grant count in `komo doctor`'s `policy:` section

`domain/task.rs` + `tools/task.rs` — durable cross-session tasks (roadmap §2's "kanban layer", shaped after hermes-agent), persisted by `KanbanDb` in its own `kanban.db`
- single `Task` model: `status` (`inbox`→`todo`→`done`, plus `waiting`/`cancelled`), `waiting_on` (set = a commitment), optional `due_at`, `source`/`source_message_id` (origin session + dedup key for reviewer commitment extraction, see `ReviewSweep`), `board` (optional project/grouping label — a plain string, not a Project entity; the §2 escape hatch, as hermes does)
- `task` tool actions: `capture` (defaults to inbox) / `list` (filter by `status` and/or `board`) / `update` / `complete`; no `plan_today` — daily planning belongs to the briefing sweep
- operator view: `komo task list` (open tasks grouped by status, board shown as `#board`)
- deliberately NOT modeled: task-to-task dependency edges (`blockedBy`/`blocks`) or `owner` — those serve autonomous worker-swarm orchestration (hermes kanban's `task_links`, Claude Code's Task\* tools), which komo (single-turn personal assistant, no dispatcher) does not have. `waiting_on` covers personal-context blocking.

`domain/todo.rs` + `tools/todo.rs` — session-scoped working focus list (roadmap §2/§8; shaped after hermes `todo_tool` / Claude Code `TodoWrite`)
- `TodoItem { content, status: pending|in_progress|completed|cancelled, active_form }`; list order = priority; at most one `in_progress` (validated on write)
- distinct from `task`: a todo dies with the conversation. Persisted per session (`SessionTodoRecord`, keyed by session id) because komo reloads a session each turn, but it is disposable — the dispatcher clears it on `/new`
- `todo` tool: call with no args to read; pass `todos` to replace the whole list (full-list replace, no merge). Reads the current session from the ambient turn context (`current_session`); inert (no session) for aux sub-agents and sweeps
- the turn's session context is established for BOTH paths: the gateway dispatcher sets it (with a real `ReplySink`), and `AgentRuntime::handle_input` sets a *detached* context (no-op sink) when none exists, so the REPL gets `todo` too — see `SessionContext::detached`

`domain/memory.rs` + `tools/memory.rs` + `infra/memory/memory_db.rs` — long-term memory as three surfaces (roadmap §5)
> **Superseded in part.** The memory sections below describe the pre-consolidation
> design: promotion by `recall_count` + query-diversity fingerprints, and the
> reviewer writing memories directly. Promotion is now evidence-driven
> (`support_count` / `last_confirmed_at`, with `BeliefState` blocking contested
> claims), query fingerprints are gone, and every extracted observation goes
> through `MemoryConsolidator`. See `AGENTS.md`'s memory bullet for the current
> rules; this file is kept for the reasoning that led here.

- `Memory` model is governed and scoped: `kind` (profile/preference/feedback/project/person/fact/decision/reference), `status` (candidate→active, plus archived/rejected), `confidence` (extracted/inferred/confirmed/user_written), `importance`, `pinned`, `scope` (`MemoryScope` global/project/channel/session, serialized as `scope_type`+`scope_key`), `source`/`source_message_id`, timestamps, `expires_at`/`last_used_at`/`recall_count`/`recall_query_hashes` (the dreaming usage signals — see below). `MemoryContext::from_session` derives the turn's `allowed_scopes` from the session id (chat → global+channel+session; CLI → global+session, **never** infers project from chat). Governance transitions live on the model (`Memory::promote/reject/pin`) so the CLI, the api channel, and the `memory` tool share one definition
- **L1 pinned** (done): `select_pinned` filters `is_pinnable` (pinned + active + confirmed/user_written + identity-kind + in-scope); `services/memory_enrichment.rs` renders an ≤800-char block appended **after** the volatile tier (cache-stable), marked `<!-- komo:memory:pinned -->`, flagged as untrusted data. Main agent only (the enricher is `Some` only there); aux/delegate get none
- **L2 tool/governance** (done): `memory` tool `save/search/list/update/promote/reject/archive`; `search` is scope-bounded (`MemoryQuery` + `rerank_score`: lexical `LIKE` + importance/confidence/recency, no embedding). Operator CLI `komo memory list/search/promote/reject/pin/triage` (promote/reject take multiple ids; `triage` walks the candidate pile oldest-first with p/r/s/q; all three writes route through a running gateway — see the api-channel note above). `pin` is the manual-only path into L1 — automated extraction never pins
- reviewer writes extractions as `candidate + extracted`, scoped to the origin channel, deduped against the memory store loaded once per review (a `seen_keys` set over each session's `source_message_id`s — same governance as task inbox, where the dedup is still `TaskRepository::find_by_source_message_id`); user triages candidates up to active/pinned
- **L3 active recall** (done): `MemoryRepository::recall(ctx, text, limit)` scores active, in-scope memories against the turn's user message by **token overlap** (`recall_terms` = ASCII words + CJK bigrams + stopword filter; `recall_score`), distinct from L2 `search`'s whole-query substring match. **Fetch wide, inject narrow**: the enricher pulls up to `recall_fetch`=15 candidates; ≤`RECALL_LIMIT`=5 survivors inject directly (zero added latency), more get screened by the **aux recall agent** (`aux_select_recall` on the cheap `aux_model`: pick ≤5 genuinely relevant, optionally condense each to one line; strict-JSON reply validated against the candidate set — fabricated ids and oversized rewrites dropped, so aux output can never inject non-memory content; timeout 4s or any failure falls back to the lexical top 5). The kept hits render into an ≤2000-char block (each line `source:`-tagged, untrusted caveat, `<!-- komo:memory:recall -->`), appended **after** pinned (fixed `volatile | pinned | recall` order; pinned hits deduped out of recall). All of this lives in `services/memory_enrichment.rs::MemoryEnricher` — one interface (`enrich(session_id, user_message) → Option<MemoryPrefix>`) whose behavior tests inject fakes through the `MemoryRepository`/`LlmClient` seams. Recall failure is non-fatal but `warn!`-logged. **Recall surfaces both `Active` and `Candidate`** (only `Archived`/`Rejected` excluded) — a candidate must be recallable to *earn* its usage signal for dreaming; it scores lower and is confidence-tagged in the block. Only the **injected** memories get `recall_count` bumped, `last_used_at` stamped, and the turn's query fingerprint (`recall_query_hash`: sorted normalized terms → 16-hex SHA-256 prefix) recorded into `recall_query_hashes` (deduped, capped at `RECALL_QUERY_HASHES_CAP`=8) via `MemoryRepository::mark_used` (never touches `updated_at`) on a spawned best-effort task off the reply path — count + distinct-query fingerprints are the dreaming signals
- **Dreaming / consolidation** (OpenClaw-borrowed, on by default — nightly `0 3 * * *`, set `dream_schedule = "off"` to disable): `domain::memory::dream_verdict`/`dream_score` decide each **candidate**'s fate purely from accumulated usage — recalled ≥`DREAM_MIN_RECALL_COUNT`(3) **by ≥`DREAM_MIN_UNIQUE_QUERIES`(2) lexically-distinct queries** (the `recall_query_hashes` fingerprints — OpenClaw's `minUniqueQueries`; one repeated question can no longer pump a candidate to active on count alone, and pre-fingerprint candidates wait until diversity accrues) → promote to `Active`+`Inferred` (recallable, but still **not** L1-pinnable — pinning stays confirmed-only/manual); a candidate older than `DREAM_FORGET_AGE_DAYS`(30) that has gone **cold** (never recalled, or not recalled within that window — measured on `last_used_at`, so *weakly* recalled candidates are retired too rather than lingering forever) → `Archived`. (`dream_score` still ranks the `komo dream` preview but no longer gates: with recall-count its dominant term, a score threshold could never reject anything the count gate accepted, so it was removed.) `agent::daemon::DreamSweep` applies it (scheduled via `dream_schedule`, wired in `cli/gateway.rs`; `komo dream [--apply]` is the operator preview/run, showing `recalls=/queries=` per candidate). Only candidates are touched — active/user-saved memories are left to the operator (`komo memory report`). Importance is proven by use, not guessed at write time. Reviewer/`memory`-tool write guidance follows Hermes: declarative facts not instructions, nothing stale-in-a-week; the `memory` tool reports the L1 pinned-budget usage% on save/list to nudge self-curation

`domain/run.rs` + `RunRepository` (impl in `infra/persistence/db.rs`) — the **run ledger**: an execution/audit record of every agent turn (roadmap §7)
- one `Run` per turn (`id`, `session_id`, `input`, `plan` summary, `status` running/done/failed, `final_output`, `error`, timestamps) and one `RunStep` per tool call (`seq`, `tool_name`, `args`, `result`, `error`, `ok`, timestamps, `elapsed_ms`). Lives in `state.db` — execution state bound to a session, disposable like messages, **not** durable personal data. `started_at`/`ended_at` are whole unix seconds, so differencing them reports 0 for any sub-second call: **`elapsed_ms` is the duration field**, measured off a monotonic `Instant` in the executor (so the transient-error retry collapse is included) and shared verbatim with the live `TurnEvent::ToolFinished`. It is an additive column (`STEP_COLUMNS` in `db.rs::connect`), so an upgraded gateway reads an old `state.db` — steps written before it exist report 0, which every reader must treat as *unknown*, not instant (`komo run inspect` prints no duration for those; the web client omits its timing)
- steps are captured inside the tool executor (see `services/tool_execution/`), so the ledger covers every executed call. `RunContext` carries a shared `seq` counter so steps order stably even across the tool's spawned task
- every write is best-effort (warn-logged, never fails a turn or a tool) — same contract as memory `mark_used`
- **`structured` + `output_paths`**: a step also carries the tool's machine-readable third view (`ToolOutput::structured` — `shell`'s `{exit, truncated, timeout}`, an `edit`'s diff stats; the model never pays tokens for it) and the paths of any stored full output. Both are **additive** columns (`STEP_COLUMNS`), so an existing `state.db` migrates in place. Empty reads as *absence*, never as an empty object — the same convention as `elapsed_ms = 0` meaning "unknown". A `structured` view over `STEP_FIELD_CAP` is **replaced** by an `_elided` marker rather than cut: half a JSON document would force every reader to treat a truncated cell as corrupt. Consumers: `komo run inspect` (indented JSON + the output paths). The live `TurnEvent::ToolFinished` does **not** carry `structured` yet — it and the web client's rendering have to land together, or a call would render one way while it runs and another after a reload
- **redaction**: step `args` are stored verbatim *except* each `Tool` may scrub its own via `Tool::redact_args` (default identity) — `shell` strips secret-looking substrings (`key=value`, `Bearer`, `--password`, high-entropy tokens), `write` drops the `content` body. `result` is truncated but not scrubbed (shell *output* can still contain secrets — accepted, `state.db` is local/disposable). Fields are length-capped (`RUN_FIELD_CAP`/`STEP_FIELD_CAP`)
- aux sub-agents and maintenance sweeps run without a `RunContext`, so their tool use never enters the ledger
- operator view: `komo run list [--limit N]` / `komo run inspect <id>` (`cli/inspect.rs`)
- **resume** (roadmap §6): the ledger is an audit record, not a checkpoint — intermediate assistant turns are never persisted and step args are redacted/truncated, so faithful mid-loop replay is impossible by design. Instead `komo run resume [<id>]` (`cli/resume.rs`) re-dispatches one *fresh* turn in the interrupted run's session, primed by `domain::run::resume_prompt` (original input + a digest of completed steps, elided past `RESUME_DIGEST_CAP`); the model judges which side effects took hold, and new side effects go through approval as usual. `recoverable` is the resumable marker: set by `reconcile_interrupted` (gateway startup flips crash-residue `Running` runs to `Failed`/interrupted), cleared by `mark_resumed` after a resume dispatches (at-most-once), shown as `⟲` in `run list`. Only interruption makes a run recoverable — an ordinary `Failed` has no half-done steps worth handing over. While the gateway holds the db lock the whole action routes to `POST /api/runs/{id}/resume` (trusted for loopback callers, same rule as chat); otherwise the turn runs in-process like `komo chat` with `CliApprover`. No automatic resume: replaying half-done side effects unattended is not acceptable — resume is always an explicit operator action

`domain/skill.rs` + `infra/skills.rs` + `infra/skill_install.rs` + `services/skill_registry.rs` + `tools/skill.rs` — the **skill subsystem** (roadmap §9): skills are `SKILL.md` files, and the filesystem is the single source of truth
- `Skill` carries governance frontmatter next to identity: `protected` (operator-edit-only — the reviewer writes **no** candidate proposal, so a "just promote it" nudge can never overwrite the operator's version), `disabled` (kept on disk + inspectable, hidden from the model's catalog; `skill view` answers with its state, not its instructions), `source` (`user` | `reviewer` | `learned` provenance — `learned` marks the on-demand `skill learn` action below, distinct from the reviewer's passive `reviewer` extraction). `valid_skill_name` is the path-segment floor that keeps an LLM-suggested name inside the skills tree
- the `skill` tool has four actions: `list` / `view` (progressive disclosure the model uses to load a playbook), `learn`, and `install`. **`view` also reports where the skill lives** — a `<skill_content>` block carrying the instructions plus the skill's **base directory** and up to 10 of its own files (`SkillRegistry::skill_files`, sorted, `SKILL.md`/`.git` excluded, absolute). Without that, a SKILL.md telling the model to "run `scripts/foo.py`" is unactionable: it has no way to know what `scripts/` is relative to. The list is explicitly labeled *sampled* so a skill with more files doesn't read as complete, and a skill directory with no assets emits **no** `<skill_files>` block at all (an empty one reads as "there are files" to a model skimming the output). `SkillRegistry::get` returns a `LocatedSkill` (skill + its dir) for exactly this; the dir is `None` only for the static test registry. **learn** is the **on-demand distillation** path — when the user asks to "记住这个流程 / 存成 skill", the model calls `skill{action:"learn", name, description, instructions}`; it writes a `learned`-tagged **candidate** through the same `FsSkillStore::save` path as the reviewer (never active, refuses a protected active skill / path-escaping name), so it goes through the identical triage ladder (the active analog of the reviewer's passive extraction — no separate distillation LLM pass). **install** is the **remote-fetch** path — `skill{action:"install", source}` fetches a skill the user points at and, once the operator **approves** (`ApprovalRequest::normal`, scope key `skill:install`, so `/approve session` covers a batch), installs it **active** (the governance exception: install always has a human in the loop — an operator CLI invocation or an approved tool call — so unlike learn it doesn't detour through candidate). Denied ⇒ nothing fetched or written
- `infra/skill_install.rs` is the shared installer behind both the `skill` tool's `install` action and the `komo skills install` CLI. `resolve_source` maps a source string to either a **git clone** (`owner/repo`, `owner/repo/subpath`, a GitHub `tree`/`blob` URL, or any `*.git`/`git@` URL — shallow-cloned via the `git` binary) or a **single raw `SKILL.md` fetch** (a `.md` URL, or a GitHub `blob` link rewritten to `raw.githubusercontent.com`). The whole fetch stages in a temp dir (removed on drop) and is copied into the store only after a valid `SKILL.md` is located, so a failed clone/fetch leaves nothing behind; `locate_skill_dir` resolves the subpath, or the repo root, or — with no subpath — the sole `SKILL.md` in the tree (multiple ⇒ an error listing the choices). `safe_join` rejects `..`/absolute subpaths so a repo can't escape its checkout. `FsSkillStore::install_active_dir` copies the **whole skill directory** (SKILL.md + scripts/`references/`, `copy_dir_all` skipping `.git`), so multi-file skills install intact — distinct from `save`, which only renders a single-file candidate; it refuses to overwrite a protected active skill, matching the `save` floor
- `FsSkillStore` (`infra/skills.rs`) owns the governed store `~/.komo/skills/`: `<name>/SKILL.md` is active; `.candidates/<name>/SKILL.md` is a reviewer proposal (invisible to the runtime until promoted); `.candidates/<name>/.history/<ts>.md` rolls prior proposal versions. Its `SkillRepository` impl is the **automated write path**: `save` only ever writes a candidate — same triage ladder as memory candidates. The **install path** (`install_active_dir`) is the deliberate exception that writes active, gated by operator/approval upstream. A one-time import (wiring) moves skills a pre-filesystem komo accumulated in `komo.db` into the candidate pile (`.imported-from-db` marker)
- `SkillRegistry` is the per-process runtime view over the skill dirs (`KOMO_SKILLS_PATH`, `<workspace>/skills`, `<workspace>/.claude/skills`, `~/.komo/skills`, `~/.agents/skills`, `~/.claude/skills`; first name wins). It **re-scans those dirs on every query** (`SkillRegistry::snapshot`), so a skill installed/promoted/enabled/disabled on disk shows up on the `skill` tool's next `list`/`view` with **no gateway restart** — the filesystem is the source of truth, matching `FsSkillStore` and the `komo skills` CLI (which previously saw disk changes the running agent's `skill` tool did not). The one thing still frozen at startup is the **capped skills catalog in the system prompt** (`skills_note`, `catalog_capped`): it lives in the cache-stable prompt tier, so it stays a startup snapshot to preserve prompt caching — but it's only a bounded teaser that tells the model to call `skill` list for the full, live set, so a newly added skill is discoverable immediately even though it's absent from that teaser until the next restart
- governance CLI (`cli/skill.rs`) is **pure file ops** — no db lock, everything works while the gateway runs: `list` / `install` / `inspect` / `promote` / `reject` / `protect` / `unprotect` / `enable` / `disable` (`install` also does network I/O via `skill_install`, but still no db lock; the operator running the shell command is the trust boundary, so it lands active directly). Only `skills audit` touches the db (it derives "which turns loaded this skill" from the run ledger's `skill view` steps via `RunRepository::steps_by_tool` + `domain::run::step_views_skill`; routed to `GET /api/skills/{name}/audit` when the gateway holds the lock). No usage counters are stored anywhere — the audit is always derived

`cli/journey.rs` — `komo journey`, a read-only **learning timeline** across the two learning subsystems (memory §5 + skills §9), newest-first. Composes existing reads with **no new api endpoint or schema**: memories via `cli::memory::load_all` (gateway-over-HTTP when the lock is held, else the db directly), skills via `FsSkillStore` file mtimes (lock-free, like the skills CLI). Flattens each memory into born (`created_at`) + promoted/archived (`updated_at`, only when it moved past creation — the stores keep two timestamps, not a transition log, so these are *inferred*; rejected memories are skipped) and each skill into candidate/active events. `memory_events` and `finalize` (sort desc / `--since` filter / `--limit` cap) are pure and unit-tested. Deliberately **not** an execution log — that's `komo run list`

`tui/` — the full-screen chat TUI (ratatui), `komo chat`'s interface. A terminal is required on both ends (`cli/app.rs::require_terminal`; a piped invocation gets a pointer to the api channel — that is the scripting surface, roadmap §8). `main.rs::will_run_tui` mirrors the predicate to route tracing to `~/.komo/logs/chat-tui.log`, since a stderr log line would tear the alternate screen. Strictly a front end over two backends (`tui/mod.rs::connect`): `GatewayClient::chat` over trusted loopback when the gateway holds the db lock, else the in-process `AgentRuntime` — no protocol of its own. Layout: scrollable transcript (CJK-width-aware wrapping in `tui/ui.rs`, bottom-anchored scroll; agent replies render as markdown via `tui/markdown.rs` — pulldown-cmark events → styled logical lines, span-wrapped by `ui.rs::wrap_spans` with the same width rules; soft breaks stay line breaks so plain text is unchanged) · status line with a turn spinner · bordered input box (the user's entries show under a bare cyan `❯`). In local mode, tool approvals arrive over a channel (`tui/approver.rs::TuiApprover`, same `y`/`s`/`n` semantics as `cli/approver.rs::CliApprover`, which remains the stdin approver for `komo run resume`) and render as a modal; concurrent requests queue, one modal at a time, and a dropped modal reads as denial. Turn futures run on spawned tasks so the event loop (`tui/mod.rs`, `tokio::select!` over key events / turn results / approval prompts / a spinner tick) keeps handling keys mid-turn; one turn at a time per session. State + key handling live terminal-free in `tui/app.rs` (unit-tested); streaming output is deliberately not in v1.
- Session ids are program-managed (uuid v7); `komo chat` always starts a fresh session, and `/new`/`/clear` are equivalent — both rotate to a new client-side id. `komo resume <id>` is the concise entry point to re-enter an existing session (`komo session resume <id>` remains compatible): it reopens the TUI bound to that id, hydrates its transcript, and errors if no such session exists (it never creates one). A session id is a UUID and nothing else: it is what a client sends in `X-Komo-Session-Id` (400 if it is not a UUID), what the gateway stores, and what `komo resume` takes. Resume routes over the gateway when the lock is held (verifying the id via `GET /api/sessions` first), else runs in-process against the db like `komo chat`.

`agent/daemon.rs` — background maintenance supervisor, hosted by the gateway (pattern borrowed from gbrain's `autopilot` supervisor)
- `Schedule` wraps `croner` (5-field Unix cron, e.g. `0 * * * *`); `Maintenance` trait is the scheduled unit of work
- `ReviewSweep` is the one fixed action: it delegates to the shared `agent/review_coordinator.rs::ReviewCoordinator` (`ReviewTrigger::Scheduled`) and maps the `ReviewReport` into its maintenance summary. The coordinator owns the whole protocol for **both** triggers — the cheap `review_candidates()` projection (session id + live user-turn count + `reviewed_through` watermark, no transcripts) decides which sessions have unseen turns, only those are loaded in full and reviewed, `mark_reviewed` advances the watermark best-effort (clamped against stale detached writes — see `SessionRepository::mark_reviewed`), and a per-session in-flight guard (process-local, RAII-released) means a post-turn review and a sweep hitting the same session review it once. The runtime's post-turn trigger (`ReviewTrigger::AfterTurn`, fired via the same coordinator instance every `review_interval` user turns) advances the same watermark, so the two never duplicate work. Beyond memories/skills, the reviewer also extracts commitments ("I'll do X", "waiting on Y") and captures them as `inbox` tasks tagged with the origin `source` + a content-derived `source_message_id` dedup key (`TaskRepository::find_by_source_message_id` guards against re-capturing across sweeps). Auto-extracted tasks only ever land in `inbox`, never `todo`; extracted memories land as `candidate` (scoped to the origin channel, deduped via the in-memory `seen_keys` set over the session's prior extractions), never pinned/active; and extracted skills land as **candidate files** (`~/.komo/skills/.candidates/`, protected skills refuse even proposals), never active — the user triages all three up the ladder (`komo task` / `komo memory promote|pin` / `komo skills promote|reject`).
- `ReminderSweep` delivers due reminders via `Notifier` every minute (10-min grace window; older ones are marked `missed`)
- `CronJobSweep` reads `cron.db` every minute and executes due jobs — command jobs run the process, agent jobs run a turn on the unattended `cron_runtime` (claim-first; output via `Notifier` — see the Scheduled cron jobs section above)
- `TaskSweep` notifies once when an open task comes due (the task stays open; `due_notified_at` is the at-most-once guard)
- `BriefingSweep` is the opt-in daily briefing (roadmap §4): it reads open tasks + recently-learned memories, builds the digest (`briefing_prompt` is the pure, clock-injected prompt builder — returns `None` when there's nothing worth a ping), and delivers it through the same `Notifier`. Only scheduled when `briefing_schedule` is set (no default — proactive pings stay opt-in); wired in `cli/gateway.rs`. The compose step prefers a **tool-capable agent turn** (roadmap §2): wiring's `briefing_runtime` is a second, small `AgentRuntime` on the aux model with a read-only tool set (time / web_fetch / web_search / skill / HA when configured — no shell/file/task/memory) and a `PolicyApprover` over a deny-all inner, so a `Risk::Normal` action passes only through an `unattended = true` policy rule; briefing skills (calendar, weather) are how external data gets in. One session per day (`briefing:YYYY-MM-DD`), every execution lands in the run ledger, and any error degrades to the original tool-less `llm.complete` so the briefing always goes out.
- `WorkdayGated` (also `agent/daemon.rs`) is a `Maintenance` decorator that gates any sweep to Chinese **working days** — the "上班才执行" gate. cron still picks the time slot; the gate decides whether today counts as a workday at all (statutory holiday → skip, ordinary weekend → skip, 调休 makeup weekend → run). Lookups go through `domain::workday::WorkdayCalendar`, degrading to a Monday–Friday default (`is_weekday`) on any data outage so a real workday never gets blocked. Opt-in via `briefing_workdays_only` (config.toml / `KOMO_BRIEFING_WORKDAYS_ONLY`); when on, `cli/gateway.rs` wraps the briefing sweep. Calendar impl is `infra/workday.rs::HolidayCalendar`: it fetches one year at a time from a free holiday API (`api.jiejiariapi.com`, `date → isOffDay`) and caches each year to `~/.komo/workdays/{year}.json` — fetched the first time any date in a year is queried, then reused (a yearly refresh, no extra cron). `komo workday [date]` is the operator probe (also primes the cache).
- `supervise` is the loop: sleep to the next cron fire, run the cycle, isolate per-cycle failures, and trip a circuit breaker after 5 consecutive failures
- the OS-level supervisor is `cli/service.rs` (`komo gateway start/stop/restart/status`) and is macOS-only: `launchd` owns `komo gateway` with `KeepAlive` auto-restart + `RunAtLoad` at login. On Linux/container deployments, run bare `komo gateway` in the foreground and let Docker/Compose/systemd own start/stop/restart.

`agent/gateway.rs` — always-on gateway (pattern borrowed from hermes-agent's gateway: a persistent process hosting background services + ingress)
- `MessageHandler` (`domain/gateway.rs`) is the pure seam between a transport and the agent; `AgentRuntime` implements it (an inbound message is one session turn)
- `Channel` trait = a pluggable ingress; `Gateway` hosts N channels + N `MaintenanceService`s (the `daemon.rs` supervisor loop — review sweep on the config schedule, reminder + task sweeps every minute, optional daily briefing), all sharing one `watch` shutdown signal
- channels are declared in `~/.komo/config.toml` and constructed in `cli/gateway.rs`; `feishu`, `telegram`, `wechat`, and `homeassistant` (event ingress) are the wired channels
- sender admission is two-layered: each channel's `admit` filters message shape (non-text, bot senders, group mention gate), then the shared `PairingGuard` (`agent/pairing.rs`, store in `domain/pairing.rs`) decides identity — config `allow_from` is pre-trusted, approved pairings pass, anyone else gets a pairing code (`komo pair approve <code>` on the host admits them; `cli/pair.rs`)
- `GatewayDispatcher` (`agent/interaction.rs`) is the front door between a channel and the agent: a channel builds a `ReplySink` (`domain/gateway.rs`) for the chat and hands it each inbound message; the dispatcher classifies chat control commands and otherwise runs a turn. Channels no longer await turns or send agent replies themselves — the dispatcher owns that, and runs each turn on a spawned task so the receive loop keeps polling (which is what lets an `/approve` reply arrive mid-turn). One turn at a time per session.
- chat control commands (any channel): `/new` (also `/clear`, `/reset`) rotates the session hermes-style (`SessionRepository::rotate` archives the old transcript under a fresh id, leaving the chat's session empty — the reviewer can still see it), clears approval state, and clears the session's working todo list; `/approve` (+ `/approve session`, + `/approve always` — the last one saves a narrow grant to `permissions.json`, offered only when the prompt showed the rule it would save) and `/deny` resolve a pending approval; `/sethome` (also `/home`) makes the current chat the home channel for proactive output (persisted via `HomeRepository`, `domain/home.rs`); `/wechat login` (also `/weixin`) provisions the WeChat channel by sending its login QR **into the current chat** as a photo — so an already-working channel (e.g. Telegram) sets up WeChat with no host shell. It drives the `WeChatLogin` trait (`domain/gateway.rs`, impl `WeChatQrLogin` in `infra/messaging/wechat.rs`), which writes creds and pulses a `Notify` the WeChat channel's `serve` loop is waiting on, so it comes online without a restart
- home channel + shutdown notice (hermes-borrowed): a single `HomeNotifier` (`infra/messaging/home_notifier.rs`) delivers all proactive output — reminders, task due notices, and the gateway's shutdown notice. It resolves the home at notify-time: the `/sethome` override (db, a `{platform}:{chat_id}` **channel address** — a session id names no channel to send through) wins over the config `home_chat` fallback (feishu first), degrading to the macOS notifier when no chat home resolves. On shutdown the gateway sends an "offline" notice through it (bounded by `SHUTDOWN_NOTICE_TIMEOUT`) before tearing down — only wired when a chat channel exists, so a foreground Ctrl-C with no channels stays quiet
- interactive tool approval over chat (ported from hermes' gateway approval): the gateway wires `ChatApprover` (`agent/interaction.rs`), not a deny-everything approver. When a side-effecting tool requests approval (`Risk::Normal`/`Dangerous`), the agent sends a prompt to the chat and the turn suspends on a `oneshot` registered in the shared `ApprovalState` (keyed by session, 5-min timeout); the user's `/approve`/`/deny` resolves it. The prompt lists `/approve always` — and the rule it would save — only for a `Risk::Normal` action carrying an `ActionRef`; the policy engine refuses to read a saved grant for a dangerous action, so offering the option there would be a lie. `Risk::Safe` actions run without asking. With no chat session in context (maintenance sweeps, aux sub-agents) approval is denied. The turn's session context (id + `ReplySink`) reaches the approver via a task-local in `services::tool_execution` that the executor re-establishes across its `tokio::spawn`.
- background install: `komo gateway start` (see `cli/service.rs`) supervises it with launchd on macOS only; bare `komo gateway` is the foreground process for Docker/Linux and the process launchd invokes on macOS

`infra/messaging/feishu.rs` — the feishu integration: `FeishuChannel` (ingress), `FeishuSender` (outbound: cached tenant token + send; also a `TextSender` for the shared `HomeNotifier`)
- receives `im.message.receive_v1` over Feishu's WebSocket long connection (openlark, no public callback URL needed); event payloads are consumed raw with komo's own tolerant serde structs; replies via the IM REST API with plain reqwest
- the ws connection runs on a dedicated thread with a current-thread runtime, isolated from the main runtime; events cross back over an mpsc channel
- `admit` filters message shape: `require_mention` for group chats, non-text and bot-sent messages dropped; sender identity goes through the shared `PairingGuard`
- session id is `feishu:{chat_id}`, so each chat is one continuous session; group @mention placeholders are stripped

`infra/messaging/telegram.rs` — the telegram integration: `TelegramChannel` (ingress), `TelegramSender` (outbound send; also a `TextSender` for the shared `HomeNotifier`)
- receives messages via `getUpdates` long polling (no public callback URL needed); plain reqwest against the Bot API, no SDK dependency
- `admit` mirrors the feishu policy: `require_mention` (group text must contain `@bot_username`, resolved via `getMe` at startup), non-text and bot-sent messages dropped; sender identity goes through the shared `PairingGuard`
- session id is `telegram:{chat_id}`; replies are sent with `parse_mode=Markdown` (rich formatting), falling back to plain chunked text when the API rejects the Markdown or the reply exceeds 4096 UTF-16 units

`infra/messaging/wechat.rs` — the WeChat (微信) integration over the **iLink** personal-bot protocol, built on the `wechatbot` crate (HTTP/JSON long-polling against `ilinkai.weixin.qq.com`, no public callback URL). `WeChatChannel` (ingress) + `WeChatSender` (outbound, also a `TextSender`) **share one `WeChatBot` instance** (built by `build_bot`, wired in `cli/gateway.rs`) — required because the crate keeps each user's reply `context_token` in memory, populated by the poll loop, and `send` needs it.
- the crate owns its own poll loop (`WeChatBot::run`) and fires a **synchronous** `on_message` callback, so the channel adapts rather than drives: the handler clones the message and `tokio::spawn`s the async pairing + `dispatcher.handle`, then `serve` hands the thread to `run()` under a shutdown `select!` (dropping the `run()` future cancels the poll)
- login is **QR-based**; creds → `~/.komo/wechat/credentials.json`. Provision either on the host with `komo channel wechat login` (`cli/wechat.rs`, renders the QR in-terminal via the `qrcode` crate) or from chat with `/wechat login` (the QR is sent into the chat as a photo — see the chat-commands list). `WeChatChannel::serve` **waits** for the cred file on an `Arc<Notify>` shared with `WeChatQrLogin` (it doesn't die without creds), so a chat-provisioned login brings the channel online with no restart. QR→PNG is `render_qr_png` (qrcode matrix → `image` crate, png feature only); photo delivery is `ReplySink::send_photo` (default errors; Telegram overrides it via `sendPhoto`)
- **DM-only**: an iLink bot identity can't join ordinary WeChat groups, so there's no group/mention gate — `PairingGuard` (`platform = "wechat"`) is the only admission control. Session id is `wechat:{user_id}`
- known limitation: proactive output (reminders/briefing via `HomeNotifier`) reaches a user only after they've messaged the bot since process start (the `context_token` map is in-memory, not persisted). The `wechatbot` crate also forces `reqwest`'s default TLS (native-tls/openssl) rather than komo's rustls — accepted tech-debt; switching needs a vendored patch

`cli/gateway.rs` — wires the `gateway` subcommand; `cli/wiring.rs` — shared `AgentRuntime` construction used by both chat and gateway (differ only in the `Approver`)

`apps/` — the **JS/TS clients** (a bun workspace, not part of the cargo build):
`apps/app` is the shared React renderer (`@komo/app`), mounted by two thin hosts
— `apps/desktop` (Electron: a native window + gateway discovery over a preload
bridge) and `apps/web` (a static SPA the api channel can serve via `web_dir`).
Both talk to the gateway only over its HTTP api channel, through one
`HttpKomoClient`; the renderer is feature-first
(`features/{chat,sessions,settings,connect,workspaces,models}`
over `shared/{ui,api,lib}`), server state lives in react-query, client state in
zustand, and one chat turn is a plain-TypeScript orchestrator
(`features/chat/turn-orchestrator.ts`) so its approval/clarify timing is unit
tested. The thread itself is assistant-ui: `ThreadPrimitive` +
`useLocalRuntime` over a `ChatModelAdapter` that is an **async generator** — each
`event: tool` frame yields a fresh assistant message, so a running tool call is a
real tool-call part in the transcript (status `running`, `timing.startedAt` set,
no result yet) rather than a widget rendered beside it. That is why the wire
format carries `started_at_ms`/`elapsed_ms` and why the live event cap equals the
ledger's `STEP_FIELD_CAP`: one component renders a call while it runs, once it
lands, and after a reload, so anything the stream says less precisely than the
ledger would become a visible jump. `runTurn` reports by callback while a
generator must pull, and `shared/lib/async.ts::pushStream` is that join (it
coalesces, and tolerates a consumer abandoning it mid-turn — which is what an
interrupt does). The tool-call chrome is the assistant-ui shadcn kit vendored
into `shared/ui/` (`tool-group`, `tool-fallback`; each file's header lists the
departures from upstream), with komo's own copy in
`features/chat/ToolCalls.tsx`. The theme is a generated shadcn preset
(`bd1khtfE`: zinc + teal, Noto
Sans, lucide) and component code may only use semantic tokens — `bun run lint`
fails on a raw color. Conventions and commands: `apps/app/README.md`
(`cd apps && bun install`, then `bun run check` = typecheck + lint + fmt + test).
There is no second GUI: the former Dioxus `crates/komo-gui` was deleted in favor
of this one.

Two per-session settings sit in the composer, and the *difference* between them is
the design: **workspace** (`features/workspaces`) renders above the input, because
choosing it is part of *starting* a conversation — once the first turn dispatches
the gateway has bound it for good, so it degrades to a static label rather than a
disabled control (an interactive-looking picker would promise something the server
ignores). **Model and effort** (`features/models`) sit in the control row below and
stay live, because they are switchable mid-thread. Both live in the zustand store
keyed by session id; the model choice is additionally seeded from the session row
whenever this client has none of its own, so reopening the app — or opening a
conversation another client started — shows the model it actually runs on instead
of the default. `App.tsx` keeps visited threads mounted keyed by **session id
alone**: an unstarted session's workspace is editable, and a composite
id+workspace key would fork a second `ChatView` on every change.

## Key extension points

- **Add a tool**: implement `Tool` in `src/tools/`, register it in `cli/wiring.rs`
- **Swap LLM provider**: implement `LlmClient` (`domain/llm.rs`) for another backend and construct it in `cli/wiring.rs` (`build_llm`)
- **Swap persistence**: implement `SessionRepository + MessageRepository` for a different backend; no changes needed in `agent/` or `domain/`
- **Add agent-loop control** (hard budget / resume — roadmap §7): the tool loop lives in-house at `AgentRuntime::run_agent_loop`, so add control points there, between rounds. Retry and the per-call fan-out budget live inside `ToolExecutor`; the loop owns the `max_turns` round budget. A new round-level signal is a new `Step` variant or a sentinel tool the loop recognizes; `LlmClient::begin_turn`/`TurnDriver` is the seam to extend, not rig. **Clarify is shipped as the sentinel-tool form**: `tools/ask_user.rs` suspends the turn on a per-session oneshot in `services/clarify.rs::ClarifyState` (2 questions/turn budget, 10-min timeout, degrades to guidance text everywhere nobody can answer); the gateway dispatcher routes the next plain message into it as the answer, the TUI does the same in local mode
- **Change the scheduled action**: implement `Maintenance` (`agent/daemon.rs`) and construct it in `cli/gateway.rs`
- **Add a gateway ingress**: implement `Channel` (`agent/gateway.rs`) for a new transport (TCP/HTTP/chat platform), `add_channel` it in `cli/gateway.rs`, gated by a `~/.komo/config.toml` declaration — `infra/messaging/feishu.rs` is the reference implementation

## Testing

Tests live beside the code with `#[cfg(test)] mod tests`. Use `#[tokio::test]` for async. Name tests by behavior (`time_tool_returns_non_empty_string`).

**`cargo test` from the root runs the root package only** — komo-core's ~70 tests
need `cargo test --workspace` (or `-p komo-core`). That gap let a `RunStep`
fixture in `komo-core/src/domain/run.rs` sit uncompilable for a while: it went
unnoticed because the documented command never built it. Run `--workspace` after
touching anything in `crates/komo-core`.

## Coding style

Default Rust formatting (`cargo fmt`), `snake_case` for modules/files/functions, `PascalCase` for structs and enums. CLI subcommands stay short and verb-based. Prefer small modules with one responsibility; keep async database code close to the layer that owns it.

## Commit & PR style

Short imperative commit messages: `add file tool`, `wire llm client`. PRs include a concise description, commands run for verification, and terminal output when CLI behavior changes.

## Agent skills

### Issue tracker

Issues and PRDs live as local markdown under `.scratch/<feature-slug>/` (no remote tracker). See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-role vocabulary, used verbatim (`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
