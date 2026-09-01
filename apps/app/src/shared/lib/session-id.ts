// A session id is a UUID and nothing else. It is what the client generates,
// what `X-Komo-Session-Id` carries, what the gateway stores, and what
// `komo resume` takes — one form, with nothing added on the way in and nothing
// stripped on the way out.
//
// It used to be `api:gui-<host>-<uuid>`: an `api:` namespace the gateway
// re-prepended and every client stripped back off, wrapped around a
// `gui-<host>-` tag that existed only to label a row in the session list. The
// gateway now rejects anything that is not a UUID, because that wrapper was the
// only thing keeping a client from addressing another channel's conversation.

export function newSessionId(): string {
  return crypto.randomUUID();
}
