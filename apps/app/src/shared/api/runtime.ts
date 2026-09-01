// The active `KomoClient` and host tag, installed once by `installHost()`
// before the app renders. A module singleton mirrors the shape of the thing it
// models: there is exactly one gateway connection per window, so threading it
// through every component would be ceremony.

import type { FolderPicker } from "../types";
import type { KomoClient } from "./types";

let client: KomoClient | null = null;
/** Which shell is running the renderer. */
export type HostTag = "desktop" | "web";

let tag: HostTag = "web";
let folderPicker: FolderPicker | null = null;

export function installClient(next: KomoClient, host: HostTag, picker?: FolderPicker): void {
  client = next;
  tag = host;
  folderPicker = picker ?? null;
}

export function getClient(): KomoClient {
  if (!client) throw new Error("KomoClient not installed — call installHost() before render");
  return client;
}

export function hostTag(): HostTag {
  return tag;
}

/** The host's native directory dialog, or null when it has none. */
export function getFolderPicker(): FolderPicker | null {
  return folderPicker;
}
