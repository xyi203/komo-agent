// Session ids are opaque; this is what a row can say about one before anybody
// has titled it. The `gui-<host>-` prefix is our own convention (see
// shared/lib/session-id.ts) — `electron` is the pre-rename form and still
// appears in existing sessions, so it stays recognised.

const HOST_LABEL: Record<string, string> = {
  desktop: "桌面",
  electron: "桌面",
  web: "浏览器",
};

const GUI_ID = /^gui-(desktop|electron|web)-.*?([0-9a-f]{4,})$/i;
/** A bare UUID, in any version — the shape the api channel mints for a session
 *  nobody named. */
const BARE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function bareId(id: string): string {
  return id.replace(/^api:/, "");
}

/** A name the session id itself carries, or **null** when it carries none.
 *
 *  Null rather than a truncated id, because the caller has better material.
 *  Most sessions here are bare UUIDs, and headlining one spends a row's most
 *  prominent line on `019fdfc7-f1df-7610-9…` — twenty of those in a column are
 *  indistinguishable, while the thing that actually separates them (when it was
 *  opened) sits below in small grey text. A channel id (`telegram:…`,
 *  `homeassistant:events`) is a different case: it names a real correspondent,
 *  so it stays the headline. */
export function sessionLabel(id: string): string | null {
  const bare = bareId(id);
  const match = bare.match(GUI_ID);
  if (match) {
    const host = HOST_LABEL[match[1].toLowerCase()] ?? "";
    return `${host}会话 ${match[2].slice(-6)}`;
  }
  if (BARE_UUID.test(bare)) return null;
  return bare.length > 22 ? `${bare.slice(0, 20)}…` : bare;
}
