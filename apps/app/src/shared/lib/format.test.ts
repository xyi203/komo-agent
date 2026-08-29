import { describe, expect, it } from "vitest";

import { fmtTs } from "./format";

describe("fmtTs", () => {
  it("renders local MM-DD HH:MM with zero padding", () => {
    // Built from local parts, so the expectation holds in any TZ the suite runs
    // in — the point of the assertion is the padding and the separator, and a
    // UTC-based formatter fails it everywhere except UTC itself.
    const local = new Date(2026, 6, 5, 3, 7);
    expect(fmtTs(local.getTime() / 1000)).toBe("07-05 03:07");
  });

  it("reads the epoch on the local clock", () => {
    const local = new Date(0);
    const p = (n: number) => String(n).padStart(2, "0");
    expect(fmtTs(0)).toBe(
      `${p(local.getMonth() + 1)}-${p(local.getDate())} ${p(local.getHours())}:${p(local.getMinutes())}`,
    );
  });
});
