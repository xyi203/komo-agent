import { describe, expect, it } from "vitest";

import { sessionLabel } from "./labels";

describe("sessionLabel", () => {
  it("has nothing to say about a session id", () => {
    // The common case, and the reason this returns null: every session id is a
    // UUID, so the row headlines the title or the time instead.
    expect(sessionLabel("019fdfc7-f1df-7610-9abc-0123456789ab")).toBeNull();
    expect(sessionLabel("0b7a6530-c869-4531-b0de-0123456789ab")).toBeNull();
  });

  it("passes a short id from an older komo through untouched", () => {
    expect(sessionLabel("feishu:oc_123")).toBe("feishu:oc_123");
  });

  it("elides a long one", () => {
    expect(sessionLabel(`telegram:${"9".repeat(40)}`)).toBe("telegram:99999999999…");
  });
});
