import { describe, expect, it } from "vitest";

import { sessionLabel } from "./labels";

describe("sessionLabel", () => {
  it("labels a desktop session with the last 6 hex of its uuid", () => {
    expect(sessionLabel("api:gui-desktop-0198f0d1-9e3a-7c11-8a2b-1c2d3e4f5a6b")).toBe(
      "桌面会话 4f5a6b",
    );
  });

  it("labels a browser session", () => {
    expect(sessionLabel("api:gui-web-0198f0d1-9e3a-7c11-8a2b-aabbccddeeff")).toBe(
      "浏览器会话 ddeeff",
    );
  });

  it("still recognises the pre-rename electron prefix", () => {
    expect(sessionLabel("api:gui-electron-0198f0d1-9e3a-7c11-8a2b-1c2d3e4f5a6b")).toBe(
      "桌面会话 4f5a6b",
    );
  });

  it("passes a short foreign id through untouched", () => {
    expect(sessionLabel("feishu:oc_123")).toBe("feishu:oc_123");
  });

  it("elides a long foreign id", () => {
    expect(sessionLabel(`telegram:${"9".repeat(40)}`)).toBe("telegram:99999999999…");
  });

  it("has nothing to say about a bare uuid", () => {
    // The common case, and the reason this returns null: the row headlines the
    // time instead.
    expect(sessionLabel("api:019fdfc7-f1df-7610-9abc-0123456789ab")).toBeNull();
    expect(sessionLabel("0b7a6530-c869-4531-b0de-0123456789ab")).toBeNull();
  });
});
