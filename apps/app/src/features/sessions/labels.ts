// What a session row can say about itself before anybody has titled it.

/** A bare UUID — the shape every session id has. */
const BARE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** A name the session id itself carries, or **null** when it carries none.
 *
 *  Null rather than a truncated id, because the caller has better material.
 *  Session ids are UUIDs, and headlining one spends a row's most prominent line
 *  on `019fdfc7-f1df-7610-9…` — twenty of those in a column are
 *  indistinguishable, while the thing that actually separates them (when it was
 *  opened) sits below in small grey text. Anything that is *not* a UUID is a
 *  session from an older komo, so it still gets shown rather than blanked. */
export function sessionLabel(id: string): string | null {
  if (BARE_UUID.test(id)) return null;
  return id.length > 22 ? `${id.slice(0, 20)}…` : id;
}
