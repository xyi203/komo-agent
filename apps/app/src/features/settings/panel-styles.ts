// Layout-only class strings shared by the dashboard panels. Anything
// button- or chip-shaped goes through <Button> / <Badge> instead.

export const ROW =
  "flex items-center gap-3 rounded-md border border-border bg-card px-3 py-2.5 text-sm shadow-xs";

export const PANEL = "flex flex-col gap-2";

// One line of the settings definition list: a label (with an optional
// explanation) on the left, its value on the right. The general tab's own rows
// and the status readouts folded into it share it, so a value that arrives from
// the gateway sits on the same baseline as a control.
export const FIELD = "flex items-center justify-between gap-4 border-b border-border py-3";
