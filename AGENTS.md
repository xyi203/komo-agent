# AGENTS.md

Guidance for coding agents working in this repository.
`CLAUDE.md` is a symlink to this file — edit `AGENTS.md` only.

komo is a personal-agent framework in Rust (DDD-style layers) plus a bun
workspace of JS/TS clients under `apps/`. Building needs `protoc`
(`brew install protobuf` — feishu websocket frames are protobuf).

## Commands

```bash
cargo check / build / fmt
cargo test --workspace             # REQUIRED: bare `cargo test` skips komo-core's ~70 tests
cargo test tools::time             # single module

komo init                          # scaffold ~/.komo (config.toml/.env/SOUL.md/USER.md; never overwrites)
cargo run -- chat                  # full-screen TUI (needs a terminal; scripts use the api channel)
cargo run -- gateway               # always-on process: sweeps + channels (feishu/telegram/wechat/HA)
komo gateway start|stop|restart|status   # macOS launchd supervision
komo upgrade [--no-restart]        # git pull --ff-only + cargo install + restart gateway
komo logs [-n N] [-f] [--stdout]   # tail gateway tracing log
komo doctor                        # config & gateway health
komo health                        # liveness probe (exit 0 = healthy; Docker HEALTHCHECK)

komo memory list|search|used|promote|reject|pin|triage|report|repair-scopes
komo memory used <id>              # which turns this memory shaped (run ledger; pruned with it)
komo wiki index [--rebuild]|search|status   # note-vault index (needs `[wiki]`; index is incremental)
komo dream [--apply]               # evidence-driven candidate consolidation (preview by default)
komo cron list|add|add-agent [--workspace DIR] [--grant c:m:v]|run|enable|disable|remove
komo run list|inspect|resume|rollback|prune   # run ledger (⟲ = recoverable)
komo skills list|install|inspect|promote|reject|protect|unprotect|enable|disable
komo skills archive|restore            # retire an active skill / bring back an archived or withdrawn one
komo skills audit [name]               # one skill's loads, or all ranked coldest-first
komo policy list|check|saved       # permission policy: config rules + job grants + saved grants
komo journey                       # learning timeline (memories + skills)
komo channel list|probe|setup      # channel inventory / verification / interactive setup
komo channel wechat login          # provision WeChat creds via QR (on the host)
komo pair approve|revoke|list      # admit chat senders
komo task list                     # kanban tasks
komo workday [YYYY-MM-DD]          # Chinese working-day check (holidays + 调休)
```

Logs: `init_tracing` in `main.rs` installs the subscriber (without it every
`info!` is a no-op). Gateway tees stderr into daily-rotated
`~/.komo/logs/gateway.YYYY-MM-DD.log` (what `komo logs` reads). Level via
`KOMO_LOG` (default `info,toasty=warn`; set `KOMO_LOG=debug` to see full tool
results and per-round token usage). Turns run in `run` spans, tool calls in `tool` spans,
matching the run ledger. The chat TUI logs to `~/.komo/logs/chat-tui.log`
instead (stderr would tear the alternate screen) and registers that path with
`komo_infra::logs::set_active`, which is how the `logs` tool finds the current
process's own log mid-conversation.

## Data & storage rules

| File | Contents | Durability |
|---|---|---|
| `~/.komo/state.db` | session *metadata*, todos, reminders, pairings, settings, run ledger, turn journal, inbox | disposable — delete freely |
| `~/.komo/sessions/` | transcripts — one append-only `.jsonl` per session | disposable |
| `~/.komo/kanban.db` | cross-session tasks | durable |
| `~/.komo/memory.db` | long-term memories | durable |
| `~/.komo/cron.db` | scheduled cron jobs | durable |
| `~/.komo/permissions.json` | saved approval grants | durable |
| `~/.komo/checkpoints/` | pre-images of files a run changed (7-day retention) | disposable |
| `~/.komo/session-index/` | episodic search index over transcripts | disposable — rebuilt on search |
| `~/.komo/tool-output/` | over-limit tool results + per-session `index.jsonl` (7-day retention) | disposable |
| `~/.komo/skills/` | skill files (filesystem is the source of truth) | durable |

Transcripts are **files, not rows** (`persistence/message_log.rs`), because they
are the one thing here that is purely appended — so they pay no schema cost: a
field added later reads as its default on every line written before it existed,
and a change deeper than that dispatches on the line's `v`. Session *metadata*
stays a row because it is *updated* (title, status, model).
`MessageRepository` is the log; `SessionRepository` reads the two together.
Rows left in the old `message_records` table move out on connect, once. Anything
that used to count messages in SQL must now go through the log — the review
sweep and `mark_reviewed`'s clamp are the two that do, and a missed one pins
every watermark at zero.

**The log records what happened; `fold` decides what it means.** A cancelled
turn and a mid-turn interjection used to rewrite the file (delete the user
message / edit it); both are now lines appended at the end, and one pure
function resolves them on read. That is also where the invariant a reader
depends on lives — user and assistant must alternate, because several providers
reject two consecutive user messages on replay. Keeping that true at each write
site took three separate patches; it is now one function, testable without a
database. **Add a new read path through `projected`, never `entries`.**

A **windowed** read (`find_windowed`, every turn) reads only the file's
tail — reading a whole conversation to discard all but its last few
messages costs IO and parsing on the reply path that grows for as long as
the session lives. It falls back to the full read when the tail does not
hold the window, so the window is never short; the fold rules resolve
against the most recent user message, which a window always contains.

Schema-change rules (toasty's `push_schema` runs only for **new** db files, and
is not idempotent):

- New table / non-additive change on disposable state → delete the affected
  file (`TaskRecord`→kanban.db, `CronJobRecord`→cron.db, anything else incl.
  `RunRecord`/`RunStepRecord`→state.db).
- **Column additions never need a reset**: `komo-infra/src/persistence/mod.rs::ensure_columns`
  ALTERs in place on connect. Extend `EXPECTED` in `memory_db.rs` for
  `MemoryRecord` columns, and the matching list in `db.rs::connect` for
  state.db (`SESSION_COLUMNS` / `RUN_COLUMNS` / `STEP_COLUMNS`). Columns must be
  NOT NULL + DEFAULT, or nullable.
  Durable data (memory.db) must **only** ever change additively.
- **A `Message` field change needs neither**: it is a JSONL line, not a column.

Turso/toasty invariants (`komo-infra`'s `persistence/`, `memory/memory_db.rs` —
the only places the ORM appears; model structs private to their file):

- Backend is Turso in MVCC `concurrent_writes` mode; no `rusqlite`. DB URL is
  `turso:<path>` / `turso::memory:`.
- MVCC rejects `AUTOINCREMENT` → every key is a `String` UUIDv7, never `#[auto]`.
- Conflicting commits fail and must be retried: wrap single-write mutations in
  `with_write_retry`; multi-write sequences in a real transaction *inside*
  `with_write_retry` (rollback + clean re-run, never double-apply).
- Legacy rusqlite files auto-migrate once (staged to `.sqlite-backup`, `.turso`
  marker prevents re-migration).

## Gateway ↔ CLI coexistence

Turso holds an exclusive cross-process lock per db file. While the gateway
runs, the CLI cannot open the dbs directly — every operator action goes through
`services/operator_control/`: probe `~/.komo/gateway.json` (rendezvous file) →
route over the loopback api channel (`infra/messaging/api.rs`,
`infra/gateway_client.rs`) or fall back to direct db open. **Both paths run the
same `operator_control/actions.rs::OperatorActions`**, so business logic can't
fork — add new operator actions there, not in the CLI or api handlers.

- `komo chat` → `POST /v1/chat/completions` with `X-Komo-Trusted` (loopback
  only): side-effecting tools auto-approve for the host operator.
  `X-Komo-Session-Id` **must be a UUID** and is the session id verbatim — 400
  otherwise. It used to be wrapped in an `api:` namespace that every client then
  stripped back off, but that wrapper was doing one undocumented thing: keeping
  a caller from addressing another ingress's session and inheriting its
  permission and memory scope. The UUID requirement is what replaces it.
- **Every api turn takes the same per-session slot a chat turn takes**
  (`GatewayDispatcher::claim_session`), so "one turn per session" holds *across*
  ingresses. Two clients on one session — a second TUI resuming it, the desktop
  app beside the terminal — used to run concurrent turns, and the later one
  assembled its history before the earlier had written a word of its answer, so
  it started over from the original question and re-ran everything still in
  flight. A chat channel queues; an HTTP caller waits, because it is owed its
  own reply. The wait is unbounded on purpose: refusing the message at a
  deadline throws away what the user typed.
- Cancel: `POST /api/interactions/{session}/cancel` flips the session's
  `CancelSignal`; `run_agent_loop` races every await against it. A running tool
  stops only if it claims `ToolContext::cancelled()` (shell kills its process
  group; web_fetch/web_search drop the request; fs tools deliberately run to
  completion so `apply_patch` never half-applies). Cancelled runs are Failed,
  **not** recoverable.
- api channel is loopback/ephemeral by default; `[channels.api] enabled = true`
  + `API_SERVER_KEY` widens it. `web_dir` serves the built SPA same-origin;
  `remote_interactive = true` lets keyed remote callers run interactive turns
  (`X-Komo-Trusted` stays loopback-only regardless). CORS grants loopback
  origins + Electron's `null` origin; bearer key remains the gate.

## Config

`~/.komo/config.toml` = runtime settings (provider/model/`models`/aux_model,
`schedule`, `briefing_schedule` + `briefing_workdays_only`, `dream_schedule`
(default nightly `0 3 * * *`, `"off"` disables), the two sweep kill switches
`briefing_schedule_enabled` / `dream_schedule_enabled` (default true; `false`
disables the sweep while leaving its cron in place, so
`KOMO_BRIEFING_SCHEDULE_ENABLED=false` / `KOMO_DREAM_SCHEDULE_ENABLED=false`
silence a deployment without rewriting config.toml), `[channels.*]`, `[policy]`
— `default_normal`, the `[[policy.rule]]` list, and `mode` (`ask` default /
`auto`, which routes an escalation through the aux reviewer first; an
unparseable value warns and stays `ask`, since a typo must never widen the
gate) —
`[memory]` — `embedding_model`/`embedding_url` for the Ollama backend behind
cross-language recall; no model = lexical-only —
`[wiki]` — `vault` (the note directory; absent = no `wiki_search`/`wiki_read`/`wiki_index`),
`backend` (`edge` default / `server`), `url` + `collection` for the server
backend, and its own `embedding_model`/`embedding_url` (falling back to
`[memory]`'s when unset); `QDRANT_API_KEY` lives in `.env` —
and `[mcp.servers.<name>]` — external MCP servers: `url`, `token_env` (names
the `.env` var, never the token), and a **required** `tools` allowlist
(or `all_tools = true`), closed by default because every mounted tool's schema
is re-sent every round).
`~/.komo/.env` = credentials only. Precedence: defaults < config.toml <
`KOMO_*` env. `KOMO_HOME` relocates the directory.

Resolution happens **once** in `crates/komo-config` into a `ConfigSnapshot`; problems
become `ConfigIssue`s (never abort resolution) checked by `validate_agent` /
`validate_gateway`. One deliberate warning, not a fatal: a missing model API key
(boots with `UnconfiguredLlm` that errors per call). **Never re-read config.toml
or call `std::env::var` in callers** — the only exception is `KOMO_HOME`.

Operator-authored prompt files (`agent/system_prompt.rs`, main agent only):
persona `~/.komo/SOUL.md`, profile `~/.komo/USER.md`, and **one instruction file
per scope, first found wins** — machine-wide `~/.komo/AGENTS.md` else
`~/.agents/AGENTS.md` (the latter under the real home, not `KOMO_HOME`, since
other agents share it), plus project `AGENTS.md` else `CLAUDE.md` else
`.cursorrules` from the working directory. Taking only the first match per scope
is what keeps a `CLAUDE.md`→`AGENTS.md` symlink from being injected twice. All of
them are head-capped and re-read on mtime change (no restart needed).

Channels (`[channels.feishu|telegram|wechat]`): behavior keys in
the table, credentials in `.env`. `allow_from` pre-trusts senders; everyone
else must pair (`komo pair approve <code>`; codes stored salted-hashed,
rate-limited, expire in 1h). WeChat is QR-login (creds in
`~/.komo/wechat/credentials.json`), DM-only, and can't deliver proactive output
until the user messages the bot after process start. `home_chat` is the
fallback for proactive output; a `/sethome` chat command override (db) wins.

Model menu: `models = [...]` declares what a session may switch to; entries may
be provider-qualified (`deepseek:deepseek-chat`) and `ModelConfig::menu()`
drops entries whose provider has no key (except the running `model`).
**A DeepSeek entry must name a v4-or-later model**: komo speaks only the
Responses API to DeepSeek, and the v3 models (`deepseek-chat`) have no
`/v1/responses` endpoint. Choice is
carried per turn in `X-Komo-Model`/`X-Komo-Effort`, validated against the menu,
stored on the session; `RoutingLlm` dispatches across providers. Effort levels
are per-provider (`Provider::efforts` ↔ `reasoning_params` must agree — there
is a test). **Invariant: every aux path (reviewer, delegate, recall, sweeps)
builds a synthetic `Session` with empty overrides** — that's what keeps a
conversation's model from leaking onto the aux model; preserve it when adding
aux callers.

The `codex` provider authenticates from the Codex CLI's OAuth file
(`~/.codex/auth.json`, auto-refreshed) instead of an env key, and requires
streaming — see `komo-infra/src/codex.rs`.

## Architecture

```
CLI/channel → AgentRuntime ─ run_agent_loop ─┬→ LlmClient::begin_turn → TurnDriver (ONE provider completion / round)
                                             └→ ToolExecutor::execute_round → tools   (loop until Step::Final)
                          ↘ MessageRepository · RunRepository (ledger) → Response
```

komo owns the tool loop **and its provider layer** (`crates/komo-provider`, no
LLM crate): one completion per round, `run_agent_loop` (`agent/runtime.rs`) is where
round-level control lives (`max_turns` budget, cancellation, clarify). Tool
errors return as outcome content the model can recover from; only a driver/LLM
error aborts the turn.

**Crate layout.** The lower half of the tree is split out of the binary so it
compiles in parallel and so an edit there does not rebuild everything (`src/` was
one 50k-line crate). Depend downward only:

```
komo-core      traits + value types, no I/O, no runtime — the GUI client reuses it
komo-config    config.toml + .env + KOMO_* → one ConfigSnapshot   (→ core)
komo-provider  wire formats + HTTP/SSE; references nothing else in komo
komo-mcp       MCP client over rmcp (Streamable HTTP); ditto — nothing komo
komo-wiki      note-vault vector index: edge (qdrant-edge, in-process) /
               server (Qdrant over gRPC) / lazy                        (→ core)
komo-infra     persistence · memory · skills · logs · workday ·
               permissions_store · codex · embedding         (→ core, config, provider)
komo-services  tool_execution · tool_output_store · memory_query ·
               memory_consolidation · memory_enrichment · clarify ·
               skill_registry · cron_actions · wiki_indexing ·
               session_indexing · episode ·
               diff/patch/search/file_mutation                (→ core, config)
komo-tools     every tool                      (→ core, infra, mcp, services)
komo-agent     runtime · gateway · daemon · interaction · system_prompt ·
               policy_approver · reviewer · llm · delegate
                                            (→ core, config, provider, infra, services)
komo (bin)     cli · tui · `infra/messaging` (channels) · `infra/gateway_client` ·
               `services/operator_control` — the wiring layer, plus what needs
               the agent above it; each `mod.rs` says why it stayed
```

Test-only constructors a dependent crate's tests need — `persistence::reset_test_db`,
`SkillRegistry::new`, `komo-tools`' fixtures — are behind each crate's
`test-support` feature, enabled only as a dev-dependency so they never ship.

Cron scheduling math (`next_occurrence_local`) lives in `komo-core`'s
`domain::cron`, and every job mutation goes through `komo-services`'
`cron_actions` — the `cron` tool, the gateway handlers and the CLI adapter all
call the same functions, which is what keeps validation from forking.

**Module map** (one line each; read the module for details):

- `domain/` — pure traits + value types, no I/O, no external crates
  (`Tool`, `LlmClient`/`TurnDriver`, repositories, policy engine, pairing).
- `komo-agent`'s `runtime` — session lifecycle + the tool loop; loads only a recent
  transcript window per turn (`find_windowed`); wraps each turn in a ledger
  `Run` (all ledger writes best-effort, never fail the turn).
- `crates/komo-provider` — komo's own provider layer, its own crate because it
  references nothing else in komo (so it compiles in parallel with the rest).
  One module per **wire format**
  (`Wire`), not per provider: `responses` (OpenAI / Codex / DeepSeek /
  OpenRouter) and `messages` (Anthropic, which serves no Responses endpoint).
  `transport` is the HTTP+SSE boundary where `error::LlmError` is built while the
  status, headers and provider error `code` are all still intact — retryability
  is `LlmError::is_retryable()` (exhaustive match) and the server's own
  `Retry-After` beats any local backoff. Every request streams; a stream that
  ends without its terminal frame is a retryable failure, never a short answer.
  A new provider is a base URL + auth mode, not new code.
- `komo-agent`'s `llm` — `ProviderLlm` over that layer; `assemble` builds the tiered
  system prompt once per turn (stable tier incl. `~/.komo/USER.md` and the
  machine-wide instruction file, then memory
  prefix from `MemoryEnricher` — main agent only). `RoutingLlm` = cross-provider
  dispatch. Reasoning blocks are echoed back verbatim each round, which is what
  carries a reasoning model's chain of thought across a tool loop.
- `services/tool_execution/` — `ToolExecutor::execute_round`: per call, claim
  ledger seq → redact args → run with panic catch + `tool` span →
  transient-retry (connection errors retry anything; ambiguous only
  `Tool::idempotent()`) → **settle**: an ambiguous failure the classifier
  declined to retry becomes `ToolError::Uncertain`, not `Failed` — "we don't
  know whether it landed" has to reach the *model*, or it re-issues the call
  itself and applies the effect twice. A wall-clock abort on a non-idempotent
  tool is the same case. `Uncertain` is never retried structurally (the retry
  arm matches `Failed` alone), and rides to the ledger as `RunStep.uncertain`
  via an `UncertainOutcome` marker in the error chain (the variant is gone by
  then) — `komo run inspect` prints `??` for it, because "did that go
  through?" has three answers, not two → bound the LLM-facing result via
  `services/tool_output_store.rs` (full text on disk, head+tail preview) →
  record `RunStep`. Policy is instance-owned `ToolExecutionConfig`;
  `Tool::max_duration()` overrides the per-call timeout (approval-gated tools
  must outlast the 5-min approval prompt, `APPROVAL_BOUND`).
  `Tool::call(Value, &ToolContext)` is the **only** tool entry point; the
  `SESSION` task-local serves the approvers only — tools take `ctx.session`.
- `komo-tools` — `time`, `shell` (own process group, hardline floor no approval
  unlocks, nested timeouts), `grep`/`glob` (ripgrep libraries in-process;
  policy runs over paths **before** content is read), `read`/`write` +
  `fs_common` (workspace-confined; `write_if_unchanged` guards the approval
  window), `edit` (exact match only, no fuzzy) / `apply_patch` (v2 envelope,
  one approval per batch, no rollback — reports exactly what landed),
  `web_fetch` (content-type gated, 256 KB download cap, deny-only network
  policy), `homeassistant` (`call_service` approval-gated; `BLOCKED_DOMAINS`
  hardline), `task`, `todo` (session-scoped, dies on `/new`), `memory`,
  `skill`, `cron`, `ask_user` (clarify), `logs` (tail of komo's own
  tracing log — file lookup shared with `komo logs` via `komo-infra`'s `logs`, same
  deny-only file-read gate as `read`), `wiki_read` (vault-confined by
  canonicalized prefix, `Risk::Safe` deny-only; reads the markdown, not the
  index, so a note edited since the last index run is served current).
- `session` + `komo-services`' `session_indexing` — **episodic memory**:
  hybrid search over komo's own transcripts, the third memory beside `memory.db`
  (semantic) and skills (procedural). `search` spans **every** stored
  conversation by default, because "why did we decide against rig?" is a
  question about *some* past session and requiring its id up front is requiring
  the answer as the input. It matches meaning as well as wording, over the same
  `ChunkIndex` the vault uses but its own collection (`~/.komo/session-index`) —
  transcripts are komo's own corpus and must not depend on `[wiki]` being
  configured. **A chunk is a turn, not a message**: "那就不用 rig 了" embeds into
  nothing alone, and its `ordinal` is its opening user message's `show` offset,
  so a hit is readable in full without translating coordinates. Indexing is
  incremental (append past the indexed chunk count) and happens **on the search
  path**, newest session first and budget-capped — a turn that never searches
  pays nothing, and a first search after a long gap does useful work instead of
  hanging. **Every failure degrades to the substring scan, never to "no
  matches"** — an empty answer reads as *the conversation never happened*, which
  is the one wrong thing this can say. Without `[memory] embedding_model` there
  is no index and `search` is the single-session scan it always was.
- `komo-agent`'s `reviewer` + `learning_coordinator`, `komo-core`'s
  `domain::episode`, `komo-services`' `episode` — the post-run extraction pass
  (docs/episode-learning-framework.md). Its unit is an **episode**: one finished
  `Run` plus its `RunStep`s, assembled on demand (`episode::assemble`) and never
  stored — the ledger is already the authority on both. A transcript alone could
  not say whether a command ran or what it returned (tool results are never
  persisted as messages), so an extractor reading one learns from the agent's
  own account of itself.
  **`Done` is not `Success`.** Execution status and goal outcome are separate
  axes (`OutcomeVerdict`); the deterministic assessment never reports success,
  because nothing observable at the end of a turn distinguishes "the goal was
  met" from "the agent stopped talking". Evidence carries its own strength and
  only the strongest kind present decides — a disagreement among peers resolves
  to `Unknown`, never a majority.
  Memory extractions leave here as `Observation`s and are applied by
  `MemoryConsolidator`, never written directly — the reviewer holds no memory
  store. It has **not** read any skill it proposes to change, so a proposal
  naming an existing active skill goes through a second aux call
  (`grounded_rewrite`) that is handed the real body and returns the complete
  replacement; failing to ground drops the proposal rather than writing the
  blind one. New skills need no second pass.
  **The watermark is `Run.learned`, per run** — not a per-session turn count: a
  count says how many turns there were, never which ones were new. A run the
  pass deliberately skips (cancelled turns, sweep sessions) is marked too, since
  "considered and declined" and "not yet considered" have to be different states
  or every sweep re-examines it forever. A *failed* pass marks nothing, so the
  next sweep retries it.
  **Learning is dispatched after `runs.finish`, never from inside the turn** —
  an episode assembled while its run is still open has no decided status, and
  `unlearned` would not offer it at all, so the turn would silently never be
  learned from. There is a regression test for exactly that.
  **Cancelled turns are audit, not lessons**: the work stopped part-way by the
  user's choice, so its silence is not evidence and its half-done steps are not
  a procedure worth keeping.
  **Sweep sessions are exempt** (`Session.origin` is `cron`/`briefing`) — a sweep restates
  facts the agent already knows, and each run's session counts as a fresh
  "independent occasion" to the consolidator, so extracting there would let the
  memory library corroborate itself on a timer. The guard lives in
  `LearningCoordinator` (`exempt_from_learning`), covering both triggers.
- `komo-agent`'s `delegate` — sub-agent as a real agent turn on its own session
  (`Session.origin = delegate`, which is what keeps it out of the session list); inherits the parent's ambient session context (approvals prompt the
  real conversation, cancel propagates); recursion blocked structurally
  (sub-agent tool set has `delegate: None`); each delegation is its own ledger
  run. The unattended cron runtime gets no `delegate`.
- `domain/policy.rs` + `komo-agent`'s `policy_approver` — permission policy. Ladder,
  strongest first: **tool hardline floor > config deny > saved grant > config
  allow / `default_normal` > ask**. Saved grants (`permissions.json`, written
  only by `PolicyApprover`) are never read unattended. **A `Risk::Dangerous`
  action is approved for the one call it was asked about and no further** —
  `/approve session` and `/approve always` narrow to `Once`, in
  `ApprovalState::resolve_scoped` for chat and in `cli/approver.rs` for the
  TTY, and the user is told. Widening an irreversible action pre-approves a
  *later* deletion nobody has seen. Unattended contexts (cron/briefing/sweeps) grant only through
  `unattended = true` allow rules **or the running job's own `grants`**
  (`CronJob.grants`, approved in the same prompt that created the job; carried
  into the turn by `with_job_grants`, scoped to that turn, revoked with the job).
  Full ladder: **tool hardline floor > config deny > job grant > saved grant >
  config allow / `default_normal` > ask**. **What marks a turn unattended is
  `SessionContext::origin`** (`SessionOrigin::Cron` / `Briefing`, set by the
  sweep that starts the turn), *not* the absence of an ambient session — those
  turns have a real session id, and reading a channel off it is what used to
  make the engine's unattended branch unreachable. Read-only actions (`read`, `web_fetch`) are
  deny-only — never prompted. Wholly-denied tools are dropped from the catalog
  at wiring (`drop_policy_denied`). Policy only tightens; hardline floors
  short-circuit inside the tool.
- `komo-agent`'s `auto_reviewer` — the `[policy] mode = "auto"` rung, sitting
  between the engine's `Ask` and the human (attended runtimes only; `mode =
  "ask"` is the default and omits the decorator entirely). An aux-model reviewer
  judges whether the action is plainly authorized by the operator's own latest
  message, and **may only allow or hand over — never deny**; refusal stays the
  operator's. Four structural properties, each a test: no deny;
  `Risk::Dangerous` never reviewed; unattended turns never reviewed (cron /
  briefing keep the "shrink the action set in advance" contract and don't wire
  it at all); fail-closed — model error, 20s timeout, unparseable verdict, or no
  operator message to judge against all mean "ask". Verdict parsing is
  deliberately strict: the word must lead the first line **and** be the only
  verdict named on it, because a line saying "ALLOW would be wrong; ASK" has
  not decided. The reviewer's trust boundary (only the operator's message
  authorizes; tool output and agent text never do) is the same rule the main
  prompt states in `system_prompt::TRUST_BOUNDARY_GUIDANCE` — one rule, so the
  agent and its reviewer cannot disagree on what authorization is. This reopens
  ADR 0002's "no LLM approver" half under that ADR's own stated trigger (MCP
  landed); the sandbox and credential-broker halves stand. See
  `docs/adr/0003-auto-policy-llm-reviewer.md`.
- `komo-mcp` + `komo-tools`' `mcp` — external MCP servers over Streamable HTTP
  (rmcp, client features only). `[mcp.servers.*]` is connected **once at
  wiring**: the catalog is immutable after that (`register` takes
  `Arc::get_mut`, and its byte-stable order is what keeps the provider prompt
  cache valid), so a server that is down at boot has no tools for the process's
  lifetime — and an unreachable one is a warning, never a fatal. Each mounted
  tool becomes `mcp__<server>__<tool>` (leaked to satisfy `Tool::name`'s
  `&'static str`; built once and `Arc`-shared across every executor). **Every
  MCP call is approval-gated** — `annotations.readOnlyHint` is server-authored,
  and the server is the party being gated; grant specific tools with
  `category = "mcp"`, `value = "<server>.<tool>"` rules. A `tools/call` that
  comes back with `isError` is returned as *content*, not a `ToolError`: the
  message is remote-controlled and the retry classifier falls back to substring
  matching, so an echoed "connection refused" must not re-fire a mutation.
- `domain/memory.rs` + `services/memory_query.rs` + `services/memory_consolidation.rs`
  + `services/memory_enrichment.rs` — three surfaces:
  L1 pinned block (manual `pin` only), L2 `memory` tool + operator CLI,
  L3 recall (fetch 15, inject ≤5, aux-screened above 5).
  **Truth and utility are different axes, on purpose.** `support_count` /
  `contradiction_count` / `last_confirmed_at` / `evidence` say whether a memory is
  *true*; `recall_count` / `last_used_at` say whether it keeps being *useful*.
  Promotion reads only the first set (`dream_verdict`: an explicit confirmation, or
  `DREAM_MIN_SUPPORT` independent occasions, and no unresolved conflict) and
  retention only the second (30-day-cold candidates archive). Deciding promotion on
  recall — as it once did — lets a wrong memory confirm itself by being retrieved:
  the thing retrieved is not the thing tested.
  **Refutation is not symmetric with support.** Support has to accumulate across
  independent occasions to promote; a candidate carrying an unresolved
  contradiction (`unresolved_refutation_at` — a conflict with no confirmation
  after it) is archived once nobody has ruled on it for
  `DREAM_REFUTED_FORGET_AGE_DAYS`, *regardless* of how warm retrieval keeps it.
  It can never promote anyway, so warmth would only keep a claim the user spoke
  against occupying a recall slot in every search about it.
  **`BeliefState` is a separate column from `status`**, not a new status value.
  Status is the triage pipeline every operator surface is built on; belief is
  `current` / `contested` / `superseded`, and only `current` may be injected
  (`is_injectable`, checked by `enrich` and `is_pinnable`). Retrieval stays
  belief-agnostic — an explicit `memory search` must surface a contested memory or
  the model cannot help settle it.
  **Every extracted observation goes through one seam** (`MemoryConsolidator`):
  find related claims, classify via aux as same/supports/contradicts/supersedes/
  unrelated, then record evidence, contest, supersede, or write a candidate. Every
  failure path lands a plain candidate — the pre-seam behavior. Evidence
  independence is **per session** (`record_evidence` drops a session it already
  counted), which is what stops one talkative conversation from corroborating
  itself; the list is capped at `EVIDENCE_CAP` while the counts keep rising.
  Reviewer extractions are always `candidate`, never pinned/active.
  **Both read paths share `MemoryQueryService`** — automatic recall and the
  model's own `memory search` build the same hybrid query, so a memory the model
  was handed is a memory it can find again (candidates included). Matching is
  **lexical ∪ semantic** (`RecallQuery`): shared terms, or
  cosine ≥ `RECALL_SEMANTIC_FLOOR` against the memory's embedding. The semantic
  arm is not optional polish — CJK bigrams and ASCII words can never be equal,
  so lexical-only recall structurally cannot match a Chinese question to an
  English memory. Embeddings come from `[memory] embedding_model` via
  `komo-infra`'s `embedding` (Ollama; a *multilingual* model, or the gap
  returns), are stored per memory with the model that produced them
  (`embedding_for` rejects a foreign vector), and are backfilled in the
  background from the read path — so every write path is covered by one
  implementation. Every embedding failure degrades to lexical, never to worse.
  Injected lines carry `/supported` and `/stale:Nd` markers off
  `vouched_at()` (last confirmation, else newest evidence, else creation — *not*
  `updated_at`, which is an edit clock), and the block header tells the model to
  confirm a stale memory before letting it drive an action.
  **Scope**: `write_scope()` channel-scopes a turn that has a correspondent
  (`SessionContext::channel`, filled from the session record in
  `run_agent_loop`), else writes `Global`. A local surface (TUI/desktop/web) has
  no correspondent, so it writes `Global` — it used to be modelled as a chat on
  an `api` platform whose chat id was a fresh uuid per conversation, which made
  every automated write unrecallable from the next turn and needed an
  `is_durable_channel` exception to undo. Memories written before that fix are
  repaired by `komo memory repair-scopes`.
- `domain/chunk_index.rs` + `komo-wiki` + `komo-services`' `wiki_indexing` +
  `komo-tools`' `wiki_search` / `wiki_read` / `wiki_index` — semantic search over the note vault
  (`[wiki] vault`), **pulled on demand, never auto-injected** like memory recall:
  a vault dwarfs the memory store, so a turn that does not search pays nothing.
  Two interchangeable backends behind `ChunkIndex` (the corpus-neutral index
  trait, shared with session search), chosen by `[wiki] backend`:
  `edge` (qdrant-edge, in-process, the default) and `server` (Qdrant over gRPC,
  for sharing one collection across processes). They speak the same data model,
  so an index built by one is readable by the other — but **nothing migrates**,
  and a switch leaves the new backend empty until `komo wiki index` refills it.
  Retrieval is hybrid (BM25 fused with dense), capped per note so one long file
  cannot crowd out a result set. **`wiki_search` finds, `wiki_read` widens**: a
  search hit is an isolated chunk, and a turn that needs the whole section asks
  for it by `path` + `heading` rather than making every query pay the context
  cost of the few that do. `wiki_read` shares the chunker's heading parser
  (`is_fence` / `parse_heading`), so it can never miss a heading search reported,
  and needs no index handle at all — which is why it survives a vector backend
  that failed to open. `LazyWikiIndex` opens the backend on first use
  and retries per call: wiring is one-shot, so an eager open that failed would
  cost `wiki_search` for the life of the process — and the usual causes (a NAS
  still booting, a local-network permission the launchd job lacks) get fixed
  while the gateway keeps running. The gateway holds the only handle, so
  `komo wiki` borrows it through `operator_control` rather than opening its own.
  Indexing is **incremental by mtime** (embedding is the whole cost of a run, so
  an unchanged file costs nothing) and `--rebuild` is the opt-out. **Nothing
  reindexes on a schedule** — there is no wiki sweep; a cron job with a
  `wiki:exact:refresh` grant is how you get one. Every indexing caller goes
  through one `WikiIndexRunner`: `wiki_index`, `komo wiki index`, and any job.
  Its `claim` is an RAII guard, so an abandoned run frees the slot instead of
  locking indexing out for the process's life. `wiki_index`'s three actions are
  three risk levels — `status` `Safe` (the diagnosis surface: an `indexed_by`
  that differs from the configured model is *the* index anomaly), `refresh`
  `Normal` and synchronous, `rebuild` `Dangerous` and **detached**: a rebuild
  `reset()`s the store before refilling it and outlives any `max_duration`, so
  running it inside the call would let a timeout abort it with the store already
  emptied. Its outcome is read back with `status`.
- `domain/checkpoint.rs` + `komo-services`' `checkpoint_store` — undoing a
  turn's **file** changes, the one thing a turn did that used to be final.
  Every other effect is already recoverable: a memory is a candidate, a skill is
  a candidate, a cron job can be removed, an ambiguous call is `Uncertain` so the
  model checks rather than repeats. `write`/`edit`/`apply_patch` produced final
  state. Now `file_mutation` keeps the bytes each file held **before the run
  first touched it** — inside the same per-path lock as the write, so the
  pre-image is exactly what that write replaced — and `komo run rollback <id>`
  puts them back. Recording is best-effort and happens *after* the mutation: a
  write the user asked for must never fail because a pre-image could not be
  filed. **A file whose content is not what the run left is skipped and named,
  never restored** — undoing one turn is the promise, and quietly undoing a
  later fix along with it is the failure mode. Operator CLI only, never a model
  tool: an agent that can undo its own turn can undo the turn that corrected it.
  Not a sandbox and not a workspace snapshot — the pre-image of exactly what
  changed, which is what a personal agent needs far more often than container
  isolation.
- `domain/run.rs` — run ledger: one `Run` per turn, one `RunStep` per call.
  `Run.memories` records **which stored memories reached that turn's prompt**
  (pinned and recall kept apart), carried out of prompt assembly on
  `TurnDriver::memories()` the same way `usage()` carries tokens. It answers
  the question `recall_count` cannot: not "is this memory useful" but "which
  memory produced *this* answer" — and, read the other way, which turns a
  memory you just corrected had already shaped. Ids only; the store stays the
  authority on content.
  The reverse direction — *which turns did this memory shape?*, the question
  asked right after correcting one — is a thin `run_memory_records` index
  written from the same value in the same `finish`, and dropped with its run
  by `prune`. Not answered by scanning runs: a `Run` carries two 4000-char
  fields, so reading thousands of them for one JSON column is the wrong
  query. **`memories` is written by `finish`, not `start`** — the enricher
  runs inside the turn, so at `start` there is nothing yet to record.
  `elapsed_ms` is the duration field (`started_at`/`ended_at` are whole
  seconds); 0 / empty `structured` read as *unknown/absent*, never
  instant/empty-object. Args redacted per-tool (`Tool::redact_args`); results
  truncated not scrubbed. `komo run resume` re-dispatches a *fresh* primed
  turn (the ledger is an audit record, not a checkpoint); `recoverable` is set
  only by crash reconciliation, cleared at-most-once, never auto-resumed.
- `domain/skill.rs` + `komo-infra`'s `skills` + `services/skill_registry.rs` —
  skills are `SKILL.md` files under `~/.komo/skills/` (active), `.candidates/`
  (proposals), `.archive/` (retired — `komo skills archive|restore`; nothing
  here ever deletes an active skill), and `.expired/` (proposals dreaming
  withdrew). Automated writes (`save` — reviewer +
  `skill learn`) only ever produce candidates; `install` is the human-in-the-loop
  exception that lands active. `protected` skills refuse even proposals.
  A candidate nobody rules on within `SKILL_CANDIDATE_EXPIRY_DAYS` is withdrawn
  by the dream sweep. **Age is the only signal there is** — a candidate cannot
  be loaded (dot dirs never enter the registry's scan), so unlike a memory
  candidate it accrues no usage to be judged on, and its clock is the
  `updated_at` frontmatter the renderer has always written. `.expired/` is kept
  apart from `.archive/` because `restore` dispatches on where a skill sits:
  archived → active, expired → **candidate**, never active — a proposal no human
  approved must not go live by way of a restore. Restoring restamps the file, or
  the next night's sweep withdraws it again before anyone can look.
  A `promote` that overwrites an active body rolls the old one into
  `.history/<name>/` — the automated path proposes *whole* bodies, so the
  overwrite has to be recoverable. `SkillRegistry` re-scans dirs on every query
  (no restart needed); only the capped prompt catalog is a startup snapshot
  (cache stability). That catalog — and **only** that catalog — is gated by
  `SkillOffer` (frontmatter `platforms:` / `requires_tools:`, evaluated per
  runtime at wiring against its own registered tool set): an always-on prompt
  line is the one place an irrelevant skill costs tokens every turn. It is never
  a load gate; `skill` view/list and every `komo skills` command ignore it.
  Usage is **derived**, never counted: `komo skills audit` rolls `skill view`
  ledger steps up per skill (`domain/run.rs`'s `skill_viewed`), so it reaches
  only as far back as the disposable `state.db` does. Each load is attributed
  to **how its turn ended** (`Run.outcome`), bucketed per *run* rather than per
  view — a skill loaded twice in one turn is one piece of evidence about that
  turn. `Unknown` is the honest majority and never counts as success: it is
  also where a skill that was loaded but never actually followed lands, since
  the ledger cannot see adoption. Failing turns are named individually, not
  summed into a count nobody reads.
- `komo-agent`'s `daemon` — `Maintenance` sweeps under `supervise` (circuit breaker
  after 5 failures). Sweep cron expressions are matched against **local time**
  via the same `next_occurrence_local` cron jobs use — never `Utc::now()`
  straight into croner, which silently shifts every schedule by the UTC offset.
  Sweeps: `ReviewSweep` (via the shared `LearningCoordinator`, which
  also serves the post-run trigger — the per-run watermark + in-flight guard
  prevent duplicate extraction), `ReminderSweep`, `CronJobSweep` (claim-before-run: a
  crash never re-fires a slot; a slot missed by more than the job's **own
  interval** is abandoned rather than fired at the wrong hour — `is_due` has no
  upper bound on lateness, and the host is a laptop. `--skip-missed` opts a job
  out of running late at all), `TaskSweep`, `BriefingSweep` (opt-in; aux-model
  runtime with read-only tools + deny-all unattended approver; degrades to
  tool-less `complete` on error; stamps a per-day watermark
  (`BriefingMarkRepository`, state.db settings) so a gateway restarted across
  today's slot catches up once at startup — `briefing_catchup_due`, same
  "asleep over a slot → run late, once" rule as cron jobs), `DreamSweep` (one
  governance cycle over both candidate pools — memories promote/archive by
  evidence, skill proposals lapse by age — previewed together by `komo dream`).
  `WorkdayGated` decorator gates a sweep to Chinese working days
  (`komo-infra`'s `workday`, cached per-year).
- `komo-agent`'s `gateway` + `interaction` — gateway hosts channels +
  sweeps. `GatewayDispatcher` owns turns (spawned per turn so `/approve` can
  arrive mid-turn; one turn per session). **`handle` is the only entry a channel
  may use**: it claims the message in the durable inbox (`domain/inbox.rs`,
  keyed `<platform>:<message_id>`) and drops redeliveries before anything else
  runs — chat platforms deliver at-least-once, and the gate has to cover
  commands too, since a redelivered `/approve` would approve twice. `dispatch`
  is the un-gated inner routine and stays private. Channels that have no
  platform message id use `InboundOrigin::local()`, which is never a duplicate.
  Chat commands: `/new` (rotate
  session, clear todos + approval state), `/approve [session|always]`,
  `/deny`, `/sethome`, `/wechat login`. `ChatApprover` suspends the turn on a
  oneshot (5-min timeout); no session in context ⇒ deny. `HomeNotifier`
  delivers all proactive output (sethome override > config `home_chat`,
  feishu first > macOS notification).
- `infra/messaging/` — channels: feishu (ws long connection on a dedicated
  thread), telegram (long polling, Markdown with plain-text fallback), wechat
  (iLink, DM-only, shared `WeChatBot` instance, in-memory reply tokens).
  A channel hands `GatewayDispatcher::handle` a **`ChannelPeer`** (platform +
  that platform's chat id), never a session id: which conversation that is
  belongs to the store (`SessionRepository::find_by_peer`, opening one on first
  contact). Session ids are UUIDs and carry nothing — they used to *be* the
  address (`feishu:{chat_id}`), which made every consumer re-derive it by
  splitting a string. Home Assistant is **not** a channel —
  it is reachable only through the `homeassistant` tool (agent pulls on
  demand); recurring device reactions belong in an HA automation written via
  the tool's `save_automation`, not in an event stream that costs an LLM turn
  per sensor tick.
- `cli/wiring.rs` — shared `AgentRuntime` construction (chat vs gateway differ
  only in `Approver`); register new tools here. Each runtime is a
  **`CapabilityProfile`** — scope, llm, tools, `max_turns`, `learns`,
  `resumable` — built by `RuntimeParts`, which holds what all of them share.
  The load-bearing field is `scope`: it used to be written twice per runtime,
  once per hook lookup, with nothing checking the two agreed or matched the
  executor's own scope, so a copy-pasted `Scope::MAIN` would hand a sweep the
  conversation's hooks. Adding a runtime is a profile, not a struct literal
  whose three real differences hide among nine identical fields.
- `tui/` — ratatui chat front end over gateway-or-in-process backends; state +
  key handling terminal-free in `tui/app.rs`. `komo resume <id>` (or the
  compatible `komo session resume <id>`) re-enters a session by its UUID and
  hydrates the transcript. Input:
  Enter sends, Shift/Alt-Enter (kitty protocol) or Ctrl-J newline, **Esc stops
  the turn in flight** (nothing when idle — a stop key that sometimes discards the
  draft is worse than one extra keystroke; under the approval modal Esc keeps
  meaning "deny"). Local turns carry a `CancelState` signal on their
  `SessionContext`; remote turns cancel over
  `POST /api/interactions/{session}/cancel`, which also denies a pending approval
  and answers a pending `ask_user` — a turn parked on either never reaches
  another await, so the signal alone would not reach it. `tui/paste.rs`
  holds both paste mechanisms — a chip folds a ≥4-line / >10 KB paste to a label
  (`input` still holds the full text; the chip's byte range is what keeps
  rendering off the folded content) and `coalesce_rapid_keys` rebuilds a paste
  that a terminal without bracketed paste delivered as keystrokes. Input events
  go through a channel so a batch can be collected before it is interpreted.
- `cron` (`~/.komo/cron.db`, `CronJobSweep`) — two job modes: **command**
  (operator-authored, runs directly, no approver) and **agent** (unattended
  turn on `cron_runtime`, side effects need `unattended = true` policy rules).
  Chat-created jobs (`tools/cron.rs`) are approval-gated at creation; a
  command job from chat is `Risk::Dangerous`. An agent job declares the actions
  it needs as `grants`, approved in that **same** prompt (which is why a
  grant-carrying `add` drops the `cron:add` scope key) — narrower than a global
  `unattended` rule and revoked when the job is removed. A job's lifecycle is a
  **stored status** (`active`/`paused`/`done` — the sole authority, no enabled
  flag); a `@at YYYY-MM-DD HH:MM` schedule is a one-shot that completes (`done`)
  at claim time and keeps its row as the queryable record — `last_output` holds
  every run's delivered body and `last_run_session` links an agent run to its
  ledger transcript, so "what did that job do" outlives the notification.
  `enable`/`run` refuse a `done` job. An agent job may also name a
  **`workspace`** — the directory its file and shell tools are confined to,
  installed on the turn's `SessionContext::workspace_root` by the sweep. It is
  canonicalized and proven to exist **when the job is created**, while the
  person who typed it is still there: resolved late it would fail at 03:00 as a
  permission refusal on every file the turn touches, which reads like a policy
  problem rather than a typo. It shares the `workdir` column with a command
  job's cwd (same question of a process and of a turn, and cron.db is durable)
  but is a different guarantee — a confinement boundary, not a convenience.
  Recurring *work* = cron job, recurring *message* = reminder, one-shot
  scheduled work = `@at` job.
- `apps/` — bun workspace: `apps/app` (shared React renderer) mounted by
  `apps/desktop` (Electron) and `apps/web` (SPA served via `web_dir`). Talks
  to the gateway over HTTP only (`HttpKomoClient`); feature-first layout;
  react-query for server state, zustand for client state; thread is
  assistant-ui over an async-generator adapter. Components may only use
  semantic theme tokens — `bun run lint` fails on raw colors. Commands:
  `cd apps && bun install`, `bun run check` (typecheck + lint + fmt + test).
  Conventions: `apps/app/README.md`.

## Extension points

- **Add a tool**: implement `Tool` in `crates/komo-tools/src/`, register in `cli/wiring.rs`
  (and add it to `tool_execution::policy_scope` if it should be policy-filterable).
- **Add an MCP server**: config only — an `[mcp.servers.<name>]` table with a
  `tools` allowlist. No code; that is the point of `komo-mcp` being generic.
- **Swap LLM provider**: implement `LlmClient` (`domain/llm.rs`), construct in
  `komo-agent`'s `llm::build_llm`.
- **Swap persistence**: implement the repository traits; `agent/`/`domain/`
  need no changes.
- **Add a provider**: an entry in `Provider` plus its base URL / auth / wire in
  `infra/llm.rs` (`wire_for`, `endpoint_url`, `build_provider_llm`). A new *wire
  format* — only if it speaks neither Responses nor Messages — is a module in
  `crates/komo-provider` and a `Wire` variant.
- **Agent-loop control**: add round-level control points in `komo-agent`'s `run_agent_loop`;
  extend `TurnDriver`/`Step`. Clarify (`tools/ask_user.rs` +
  `services/clarify.rs`) is the sentinel-tool reference.
- **Scheduled action**: implement `Maintenance`, construct in `cli/gateway.rs`.
- **Gateway ingress**: implement `Channel`, `add_channel` in `cli/gateway.rs`,
  gate behind a `[channels.*]` declaration — feishu is the reference.

## Testing

Tests live beside the code (`#[cfg(test)] mod tests`, `#[tokio::test]` for
async), named by behavior. **Always `cargo test --workspace`** — the bare root
command skips `crates/komo-core`.

## Coding style

`cargo fmt` defaults; `snake_case` modules/functions, `PascalCase` types. Small
modules, one responsibility; keep async db code in the layer that owns it. CLI
subcommands short and verb-based.

## Commit & PR style

Short imperative commits (`add file tool`). PRs: concise description, commands
run for verification, terminal output when CLI behavior changes.

## Repo docs

- Issues/PRDs: local markdown under `.scratch/<feature-slug>/` — `docs/agents/issue-tracker.md`
- Triage labels: `needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix` — `docs/agents/triage-labels.md`
- Domain docs: `CONTEXT.md` + `docs/adr/` — `docs/agents/domain.md`
- Long-form design rationale (archived old AGENTS.md): `docs/agents/architecture-notes.md`
