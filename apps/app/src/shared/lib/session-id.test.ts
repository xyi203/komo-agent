import { describe, expect, it } from "vitest";

import { newSessionId } from "./session-id";

const BARE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

describe("newSessionId", () => {
  // The gateway rejects a session header that is not a UUID, so this is the one
  // shape it will accept — and the same string `komo resume` takes.
  it("is a bare uuid, with nothing wrapped around it", () => {
    expect(newSessionId()).toMatch(BARE_UUID);
  });

  it("is unique per call", () => {
    expect(newSessionId()).not.toBe(newSessionId());
  });
});
