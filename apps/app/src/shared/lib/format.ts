/** Coarse "how long ago" from unix seconds, in the local clock.
 *
 *  Memory is read in spans, not timestamps: what matters about a memory is that
 *  it was learned three weeks ago and has not been touched since, never that it
 *  landed at 14:07. */
export function fmtAgo(ts: number): string {
  const seconds = Math.max(0, Math.floor(Date.now() / 1000) - ts);
  if (seconds < 60) return "刚刚";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} 分钟前`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} 小时前`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days} 天前`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months} 个月前`;
  return `${Math.floor(months / 12)} 年前`;
}

/** Local `MM-DD HH:MM` from unix seconds.
 *
 *  Local, not UTC: every caller renders a wall-clock moment the operator lived
 *  through — when a session was opened, when a run started, when a task is due.
 *  Read east of Greenwich a UTC clock is wrong by the offset and, past the
 *  offset each evening, wrong about the *day* too, which reads as a stale list
 *  rather than a timezone. `fmtAgo` above has always measured in local elapsed
 *  time; this is the same clock. */
export function fmtTs(ts: number): string {
  const d = new Date(ts * 1000);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}
