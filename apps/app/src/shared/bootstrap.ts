// The one call a host makes before rendering: install its `KomoClient` +
// platform tag, seed the first session id, and apply the persisted theme
// (before first paint, so there's no light→dark flash).

import { installClient } from "./api/runtime";
import type { KomoClient } from "./api/types";
import type { HostTag } from "./api/runtime";
import { newSessionId } from "./lib/session-id";
import { applyTheme } from "./lib/theme";
import { useAppStore } from "./store";
import type { FolderPicker } from "./types";

export function installHost({
  client,
  tag,
  chooseFolder,
}: {
  client: KomoClient;
  tag: HostTag;
  /** Optional: a native directory dialog, when the host has OS access. Supplying
   *  it is what puts "选择其他文件夹…" in the workspace picker. */
  chooseFolder?: FolderPicker;
}): void {
  installClient(client, tag, chooseFolder);
  const store = useAppStore.getState();
  if (!store.session) {
    const session = newSessionId();
    useAppStore.setState({
      session,
    });
  }
  applyTheme(useAppStore.getState().theme);
}
