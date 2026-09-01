import { describe, expect, it } from "vitest";

import type { KomoApiResponse, KomoChatRequest, KomoClient, TurnEvent } from "@/shared/api/types";
import type { Interactions, PendingApproval } from "@/shared/types";
import { foldToolEvent, runTurn, type ToolActivity } from "./turn-orchestrator";

const STARTED_AT = 1_700_000_000_000;

const started = (seq: number, name = "shell"): TurnEvent => ({
  type: "tool_started",
  seq,
  name,
  args: '{"cmd":"ls"}',
  started_at_ms: STARTED_AT,
});
const finished = (seq: number, ok = true): TurnEvent => ({
  type: "tool_finished",
  seq,
  name: "shell",
  ok,
  summary: ok ? "done" : "failed",
  elapsed_ms: 40,
});

const approval: PendingApproval = { summary: "run ls", detail: null, risk: "normal" };

/** A client whose chat() resolves only when the test says so, and whose api()
 *  replays a scripted sequence of interaction responses. */
function harness(options: {
  interactions?: KomoApiResponse<Interactions>[];
  onChat?: (emit: (event: TurnEvent) => void) => Promise<void>;
  chatResult?: { ok: boolean; reply?: string; error?: string };
}) {
  const script = [...(options.interactions ?? [])];
  let polls = 0;
  let seenSignal: AbortSignal | undefined;
  const requests: { path: string; method?: string }[] = [];
  const client: KomoClient = {
    connect: async () => ({ connected: true }),
    api: async (req) => {
      requests.push({ path: req.path, method: req.method });
      if (req.path.endsWith("/cancel")) return { ok: true, status: 200 } as KomoApiResponse<never>;
      polls++;
      const next = script.shift() ?? {
        ok: true,
        status: 200,
        data: { approval: null, question: null },
      };
      return next as KomoApiResponse<never>;
    },
    chat: async (_req: KomoChatRequest, chatOptions) => {
      seenSignal = chatOptions?.signal;
      await options.onChat?.((event) => chatOptions?.onToolEvent?.(event));
      return options.chatResult ?? { ok: true, reply: "hi" };
    },
  };
  return {
    client,
    polls: () => polls,
    seenSignal: () => seenSignal,
    requests: () => requests,
  };
}

/** A sleep the test drives by hand: the poll loop advances exactly one
 *  iteration per `tick()`, so nothing spins while the turn is gated. */
function controlledClock() {
  let waiters: (() => void)[] = [];
  const sleep = (_ms: number, signal?: AbortSignal) => {
    if (signal?.aborted) return Promise.resolve();
    return new Promise<void>((resolve) => {
      waiters.push(resolve);
      signal?.addEventListener("abort", () => resolve(), { once: true });
    });
  };
  /** Release every pending sleep, then let the awakened code run. */
  const tick = async () => {
    const pending = waiters;
    waiters = [];
    for (const wake of pending) wake();
    await flush();
  };
  return { sleep, tick };
}

/** Drain pending microtasks (a poll's `await client.api(...)`). */
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("foldToolEvent", () => {
  it("appends a started call", () => {
    expect(foldToolEvent([], started(1))).toEqual([
      { seq: 1, name: "shell", args: '{"cmd":"ls"}', done: false, startedAtMs: STARTED_AT },
    ]);
  });

  it("marks the matching call finished, leaving others alone", () => {
    const tools = foldToolEvent(foldToolEvent([], started(1)), started(2, "time"));
    const done = foldToolEvent(tools, finished(1));
    expect(done[0]).toMatchObject({ seq: 1, done: true, ok: true, summary: "done" });
    expect(done[1]).toMatchObject({ seq: 2, done: false });
  });

  it("replaces a re-started seq rather than duplicating it", () => {
    const tools = foldToolEvent(foldToolEvent([], started(1)), started(1, "time"));
    expect(tools).toHaveLength(1);
    expect(tools[0].name).toBe("time");
  });

  it("ignores a finish for an unknown seq", () => {
    expect(foldToolEvent([], finished(9))).toEqual([]);
  });
});

describe("runTurn", () => {
  it("returns the reply and the calls made during the turn", async () => {
    const { client } = harness({
      onChat: async (emit) => {
        emit(started(1));
        emit(finished(1));
      },
    });
    const seen: ToolActivity[][] = [];
    const result = await runTurn(
      { session: "019fad15-8199-7461-9d48-0a6c779f1c8d", message: "hi", mode: "interactive" },
      { onTools: (tools) => seen.push(tools) },
      { client, sleep: controlledClock().sleep },
    );
    expect(result.reply).toBe("hi");
    expect(result.tools).toMatchObject([{ seq: 1, done: true, ok: true }]);
    // Reset, started, finished — the strip updates live.
    expect(seen.map((t) => t.length)).toEqual([0, 1, 1]);
  });

  it("sends the session id as the header, unchanged", async () => {
    // One form end to end: the gateway rejects anything that is not a UUID, so
    // there is nothing to add on the way out or strip on the way back.
    let header = "";
    const client: KomoClient = {
      connect: async () => ({ connected: true }),
      api: async () =>
        ({ ok: true, status: 200, data: { approval: null, question: null } }) as never,
      chat: async (req) => {
        header = req.header;
        return { ok: true, reply: "" };
      },
    };
    const session = "019fad15-8199-7461-9d48-0a6c779f1c8d";
    await runTurn(
      { session, message: "hi", mode: "trusted" },
      {},
      { client, sleep: controlledClock().sleep },
    );
    expect(header).toBe(session);
  });

  it("surfaces a pending approval while the turn is in flight, then clears it", async () => {
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const clock = controlledClock();
    const { client } = harness({
      interactions: [{ ok: true, status: 200, data: { approval, question: null } }],
      onChat: async () => gate,
    });
    const approvals: (PendingApproval | null)[] = [];
    const turn = runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      { onApproval: (a) => approvals.push(a) },
      { client, sleep: clock.sleep },
    );
    await flush();
    expect(approvals).toEqual([approval]);
    release();
    await turn;
    expect(approvals.at(-1)).toBeNull();
  });

  it("surfaces a clarify question the same way", async () => {
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const clock = controlledClock();
    const { client } = harness({
      interactions: [{ ok: true, status: 200, data: { approval: null, question: "哪个环境？" } }],
      onChat: async () => gate,
    });
    const questions: (string | null)[] = [];
    const turn = runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      { onQuestion: (q) => questions.push(q) },
      { client, sleep: clock.sleep },
    );
    await flush();
    expect(questions).toEqual(["哪个环境？"]);
    release();
    await turn;
    expect(questions.at(-1)).toBeNull();
  });

  it("keeps polling after a transient interaction failure", async () => {
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const clock = controlledClock();
    const { client } = harness({
      interactions: [
        { ok: false, status: 0, error: "网络抖动" },
        { ok: true, status: 200, data: { approval, question: null } },
      ],
      onChat: async () => gate,
    });
    const approvals: (PendingApproval | null)[] = [];
    const turn = runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      { onApproval: (a) => approvals.push(a) },
      { client, sleep: clock.sleep },
    );
    await flush();
    expect(approvals).toEqual([]);
    // The failure must not kill the loop — otherwise a prompt raised after it
    // would never reach the user and the turn would hang until the server
    // timeout.
    await clock.tick();
    expect(approvals).toEqual([approval]);
    release();
    await turn;
  });

  it("gives up polling once the backoff is exhausted", async () => {
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const clock = controlledClock();
    const { client, polls } = harness({
      interactions: Array.from({ length: 30 }, () => ({
        ok: false as const,
        status: 0,
        error: "down",
      })),
      onChat: async () => gate,
    });
    const turn = runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      {},
      { client, sleep: clock.sleep },
    );
    await flush();
    for (let i = 0; i < 10; i++) await clock.tick();
    // 5 backoff steps + the poll that exhausted them.
    expect(polls()).toBe(6);
    release();
    await turn;
  });

  it("throws when the request fails, and still stops polling", async () => {
    const clock = controlledClock();
    const { client, polls } = harness({ chatResult: { ok: false, error: "HTTP 500" } });
    await expect(
      runTurn(
        { session: "s", message: "hi", mode: "interactive" },
        {},
        { client, sleep: clock.sleep },
      ),
    ).rejects.toThrow("HTTP 500");
    const after = polls();
    await clock.tick();
    expect(polls()).toBe(after);
  });

  it("hands the caller's abort signal to the client, so stopping reaches the socket", async () => {
    const controller = new AbortController();
    const { client, seenSignal } = harness({});
    await runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      {},
      { client, sleep: controlledClock().sleep, signal: controller.signal },
    );
    expect(seenSignal()).toBe(controller.signal);
  });

  it("throws an AbortError when interrupted, so the runtime marks it cancelled", async () => {
    const controller = new AbortController();
    const clock = controlledClock();
    const { client } = harness({
      // The request rejects the way an aborted fetch does; what matters is the
      // error the orchestrator surfaces afterwards.
      chatResult: { ok: false, error: "The operation was aborted." },
      onChat: async () => {
        controller.abort();
      },
    });
    const turn = runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      {},
      { client, sleep: clock.sleep, signal: controller.signal },
    );
    await expect(turn).rejects.toMatchObject({ name: "AbortError" });
  });

  it("stops polling as soon as the caller aborts, without waiting for the request", async () => {
    const controller = new AbortController();
    const clock = controlledClock();
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client, polls } = harness({ onChat: async () => gate });
    const turn = runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      {},
      { client, sleep: clock.sleep, signal: controller.signal },
    );
    await flush();
    const before = polls();
    controller.abort();
    await clock.tick();
    expect(polls()).toBe(before);
    release();
    await expect(turn).rejects.toMatchObject({ name: "AbortError" });
  });

  it("tells the gateway to stop, since hanging up alone leaves the agent running", async () => {
    const controller = new AbortController();
    const clock = controlledClock();
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client, requests } = harness({ onChat: async () => gate });
    const turn = runTurn(
      { session: "019fad15-8199-7461-9d48-0a6c779f1c8d", message: "hi", mode: "interactive" },
      {},
      { client, sleep: clock.sleep, signal: controller.signal },
    );
    await flush();
    controller.abort();
    await flush();
    expect(requests()).toContainEqual({
      path: "/api/interactions/019fad15-8199-7461-9d48-0a6c779f1c8d/cancel",
      method: "POST",
    });
    release();
    await expect(turn).rejects.toMatchObject({ name: "AbortError" });
  });

  it("reports a failed tool call in the result instead of throwing", async () => {
    const { client } = harness({
      onChat: async (emit) => {
        emit(started(1));
        emit(finished(1, false));
      },
    });
    const result = await runTurn(
      { session: "s", message: "hi", mode: "interactive" },
      {},
      { client, sleep: controlledClock().sleep },
    );
    expect(result.tools[0]).toMatchObject({ done: true, ok: false, summary: "failed" });
  });
});
