// Turning a workspace *id* into something a person can read.
//
// Three surfaces ask this question — the sidebar's group headers, the shell
// header, and the composer's picker once a conversation has locked its choice —
// and each used to answer it on its own, falling back to the raw id. That
// fallback is only harmless for a catalog id: a folder workspace is
// `folder:<base64url path>` (see features/workspaces/WorkspacePicker's
// `encodeFolder`, and `folder_workspace_path` on the gateway side), so the raw
// id reads as a wall of base64 exactly where the operator is scanning for which
// project a conversation belongs to.

import type { WorkspaceInfo } from "@/shared/types";

/** The gateway's id for "wherever komo itself lives". */
export const DEFAULT_WORKSPACE = "__default__";

/** The absolute path inside a `folder:` id, or null for any other id.
 *
 *  Deliberately total: an id from a newer client, a truncated one, or one that
 *  simply isn't base64 must degrade to "not a folder id" rather than throw on a
 *  render path. */
export function decodeFolderPath(id: string): string | null {
  const encoded = id.startsWith("folder:") ? id.slice("folder:".length) : null;
  if (!encoded) return null;
  try {
    const binary = atob(encoded.replaceAll("-", "+").replaceAll("_", "/"));
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    const path = new TextDecoder(undefined, { fatal: true }).decode(bytes);
    return path.startsWith("/") ? path : null;
  } catch {
    return null;
  }
}

/** The last segment of a path — what a person calls that directory. */
function basename(path: string): string {
  const trimmed = path.replace(/\/+$/, "");
  return trimmed.slice(trimmed.lastIndexOf("/") + 1) || trimmed || path;
}

/** How a workspace id should read on screen.
 *
 *  Catalog name first (the gateway's own naming wins), then the folder path's
 *  last segment, then the id itself — which by then can only be a catalog id
 *  this client hasn't loaded yet, and those are already human-shaped. */
export function workspaceLabel(id: string, workspaces: WorkspaceInfo[]): string {
  const known = workspaces.find((workspace) => workspace.id === id);
  if (known) return known.name;
  if (id === DEFAULT_WORKSPACE) return "默认 workspace";
  const path = decodeFolderPath(id);
  return path ? basename(path) : id;
}

/** The full path behind a workspace id, when one is knowable — for `title`
 *  attributes, where the whole location is what disambiguates two folders that
 *  share a basename. */
export function workspacePath(id: string, workspaces: WorkspaceInfo[]): string | null {
  return workspaces.find((workspace) => workspace.id === id)?.path ?? decodeFolderPath(id);
}
