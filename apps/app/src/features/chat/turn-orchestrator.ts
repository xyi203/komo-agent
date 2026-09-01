// One chat turn, end to end, with no React in sight.
//
// A turn is a single HTTP request that can block server-side for minutes: the
// gateway suspends it when a tool needs approval or the agent asks a question,
// and both are resolved out-of-band. So while the request is in flight we poll
// the interactions endpoint and surface whatever it reports; the same request
// eventually returns the final reply. Tool-call frames arrive live on the
// stream and fold into an activity list.
//
// Interrupting is part of that, and it takes three things — anything less looks
// like a stop button that doesn't stop:
//   1. abort the request, so the stream and the poll stop locally;
//   2. POST the gateway's cancel endpoint, so the *agent* stops too — the turn
//      runs on a spawned task server-side, so hanging up alone leaves it going;
//   3. surface an AbortError, so the runtime marks the message cancelled rather
//      than failed.
// The one thing it cannot stop is a tool call already executing; that finishes.
//
// Everything here is injectable (client + sleep), which is what makes the
// timing behaviour testable — see turn-orchestrator.test.ts.

import type { KomoClient, TurnEvent } from "@/shared/api/types";
import { INTERACTIONS_BACKOFF_MS, POLL } from "@/shared/config";
import { sleep as realSleep } from "@/shared/lib/async";
import type { Interactions, Mode, PendingApproval } from "@/shared/types";
import { interactionsPath } from "./api";

/** One tool call as the live feed knows it. */
export interface ToolActivity {
  seq: number;
  name: string;
  args: string;
  done: boolean;
  ok?: boolean;
  summary?: string;
  /** Unix ms, from the gateway — so a duration is measured against the clock the
   *  call actually started on, not against when its frame reached the browser. */
  startedAtMs: number;
  /** The gateway's monotonic measurement. Absent while the call runs. */
  elapsedMs?: number;
}

/** Fold one streamed event into the activity list. Pure: a started event
 *  replaces any earlier entry with the same seq, a finished event marks it. */
export function foldToolEvent(tools: ToolActivity[], event: TurnEvent): ToolActivity[] {
  if (event.type === "tool_started") {
    return [
      ...tools.filter((t) => t.seq !== event.seq),
      {
        seq: event.seq,
        name: event.name,
        args: event.args,
        done: false,
        startedAtMs: event.started_at_ms,
      },
    ];
  }
  return tools.map((t) =>
    t.seq === event.seq
      ? { ...t, done: true, ok: event.ok, summary: event.summary, elapsedMs: event.elapsed_ms }
      : t,
  );
}

export interface TurnHooks {
  /** The whole activity list, on every change. */
  onTools?: (tools: ToolActivity[]) => void;
  onApproval?: (approval: PendingApproval | null) => void;
  onQuestion?: (question: string | null) => void;
}

export interface TurnDeps {
  client: KomoClient;
  /** The runtime's per-run signal. Aborted when the user hits stop. */
  signal?: AbortSignal;
  /** Interval between interaction polls (overridden in tests). */
  pollMs?: number;
  /** Abortable delay (overridden in tests). */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}

export interface TurnRequest {
  session: string;
  message: string;
  mode: Mode;
  workspace?: string;
  /** Per-session model / reasoning effort (empty = gateway/provider default).
   *  Sent on every turn: the gateway validates and stores them on the session,
   *  which is how the choice travels with the conversation. */
  model?: string;
  effort?: string;
}

export interface TurnResult {
  reply: string;
  tools: ToolActivity[];
}

/** Tell the gateway to stop the turn. Fire-and-forget: the request is already
 *  aborted by the time this runs, so there is nobody to report a failure to —
 *  and a failed cancel just means the turn finishes on its own. */
function requestServerCancel(session: string, client: KomoClient): void {
  void client.api({ path: `${interactionsPath(session)}/cancel`, method: "POST" }).catch(() => {});
}

/** Poll for pending approvals/questions until aborted. A single failure is
 *  transient (the gateway is busy), so back off and keep going; only an
 *  exhausted backoff gives up — dropping the poll silently would leave an
 *  approval prompt invisible for the rest of the turn. */
async function pollInteractions(
  session: string,
  hooks: TurnHooks,
  deps: Required<Pick<TurnDeps, "client" | "pollMs" | "sleep">>,
  signal: AbortSignal,
): Promise<void> {
  const path = interactionsPath(session);
  let failures = 0;
  while (!signal.aborted) {
    const res = await deps.client.api<Interactions>({ path });
    if (signal.aborted) return;
    if (res.ok && res.data) {
      failures = 0;
      hooks.onApproval?.(res.data.approval ?? null);
      hooks.onQuestion?.(res.data.question ?? null);
    } else if (++failures > INTERACTIONS_BACKOFF_MS.length) {
      return;
    }
    const delay = failures === 0 ? deps.pollMs : INTERACTIONS_BACKOFF_MS[failures - 1];
    await deps.sleep(delay, signal);
  }
}

/** Run one turn. Throws when the request itself fails; tool errors are part of
 *  the returned activity list, not exceptions. */
export async function runTurn(
  req: TurnRequest,
  hooks: TurnHooks,
  deps: TurnDeps,
): Promise<TurnResult> {
  const resolved = {
    client: deps.client,
    pollMs: deps.pollMs ?? POLL.interactions,
    sleep: deps.sleep ?? realSleep,
  };

  let tools: ToolActivity[] = [];
  hooks.onTools?.(tools);

  const controller = new AbortController();
  const stopOnAbort = () => {
    controller.abort();
    requestServerCancel(req.session, resolved.client);
  };
  deps.signal?.addEventListener("abort", stopOnAbort, { once: true });
  const poll = pollInteractions(req.session, hooks, resolved, controller.signal).catch(() => {
    /* a poll must never fail the turn */
  });

  try {
    const res = await resolved.client.chat(
      {
        header: req.session,
        message: req.message,
        mode: req.mode,
        workspace: req.workspace,
        model: req.model,
        effort: req.effort,
      },
      {
        onToolEvent: (event) => {
          tools = foldToolEvent(tools, event);
          hooks.onTools?.(tools);
        },
        signal: deps.signal,
      },
    );
    // An interrupt must surface as an AbortError: that is what tells the runtime
    // the message was *cancelled* rather than failed (anything else renders as
    // an error bubble).
    if (deps.signal?.aborted) throw new DOMException("turn cancelled", "AbortError");
    if (!res.ok) throw new Error(res.error || "Request failed");
    return { reply: res.reply ?? "", tools };
  } finally {
    deps.signal?.removeEventListener("abort", stopOnAbort);
    controller.abort();
    await poll;
    hooks.onApproval?.(null);
    hooks.onQuestion?.(null);
  }
}
