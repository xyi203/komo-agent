import { describe, expect, it } from "vitest";

import type { SessionSummary, WorkspaceInfo } from "@/shared/types";
import { groupByWorkspace } from "./grouping";

function session(id: string, workspace: string | undefined, created_at: number): SessionSummary {
  return { id, workspace, created_at, messages: 2, user_turns: 1 };
}

const workspaces: WorkspaceInfo[] = [
  { id: "__default__", name: "komo", path: "/repo/komo" },
  { id: "notes", name: "notes", path: "/ws/notes" },
];

describe("groupByWorkspace", () => {
  it("puts each session under its own workspace", () => {
    const groups = groupByWorkspace(
      [session("a", "notes", 3), session("b", "__default__", 2), session("c", "notes", 1)],
      workspaces,
    );
    expect(groups.map((g) => [g.workspace, g.entries.map((e) => e.id)])).toEqual([
      ["notes", ["a", "c"]],
      ["__default__", ["b"]],
    ]);
  });

  it("orders groups by their most recent session, not by name", () => {
    // "notes" sorts after "__default__" alphabetically but holds the newest
    // conversation, so it must lead — the sidebar is a recency list.
    const groups = groupByWorkspace(
      [session("old", "__default__", 10), session("new", "notes", 99)],
      workspaces,
    );
    expect(groups.map((g) => g.workspace)).toEqual(["notes", "__default__"]);
  });

  it("labels a workspace by its catalog name, and an unlisted folder by its directory", () => {
    // `L2Zvby9iYXI` decodes to "/foo/bar", so the group reads as "bar". `Zm9v`
    // decodes to "foo" — not an absolute path, so the id stays opaque.
    const groups = groupByWorkspace(
      [
        session("a", "__default__", 3),
        session("b", "folder:L2Zvby9iYXI", 2),
        session("c", "folder:Zm9v", 1),
      ],
      workspaces,
    );
    expect(groups.map((g) => g.label)).toEqual(["komo", "bar", "folder:Zm9v"]);
  });

  it("treats a session from a pre-workspace gateway as the default", () => {
    const groups = groupByWorkspace([session("legacy", undefined, 1)], workspaces);
    expect(groups).toHaveLength(1);
    expect(groups[0]?.workspace).toBe("__default__");
    expect(groups[0]?.label).toBe("komo");
  });

  it("has no groups when there are no sessions", () => {
    expect(groupByWorkspace([], workspaces)).toEqual([]);
  });
});
