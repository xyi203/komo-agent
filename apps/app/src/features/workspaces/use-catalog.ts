import { useQuery } from "@tanstack/react-query";

import { qk } from "@/shared/api/query-keys";
import { useConnection } from "@/shared/api/use-connection";
import { useAppStore } from "@/shared/store";
import type { WorkspaceInfo } from "@/shared/types";
import { fetchWorkspaces } from "./api";

/** Every workspace this client can name: the gateway's catalog plus the folders
 *  picked here through the host's directory dialog.
 *
 *  The catalog wins on a collision — a folder picked earlier that has since
 *  appeared under the gateway's workspace home should read as the catalog entry.
 *  Three surfaces need this list and each used to merge it itself. */
export function useWorkspaceCatalog(): { workspaces: WorkspaceInfo[]; isPending: boolean } {
  const { connected } = useConnection();
  const picked = useAppStore((s) => s.pickedWorkspaces);
  const query = useQuery({ queryKey: qk.workspaces, queryFn: fetchWorkspaces, enabled: connected });
  const workspaces = [...(query.data ?? []), ...Object.values(picked)].filter(
    (item, index, all) => all.findIndex((candidate) => candidate.id === item.id) === index,
  );
  return { workspaces, isPending: query.isPending };
}
