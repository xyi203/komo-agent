import { FolderIcon } from "lucide-react";

import { getFolderPicker } from "@/shared/api/runtime";
import { useAppStore } from "@/shared/store";
import { workspaceLabel, workspacePath } from "@/shared/lib/workspace";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/shared/ui/select";
import { useWorkspaceCatalog } from "./use-catalog";

/** Sentinel value for the "pick a folder" row — never a workspace id. */
const CHOOSE_FOLDER = "__choose_folder__";

/** Encode an absolute path as an opaque `folder:` workspace id.
 *
 *  The gateway resolves catalog ids by name and only decodes this form for a
 *  loopback caller (`resolve_folder_workspace` in infra/messaging/api.rs).
 *  base64url is what makes an arbitrary Unicode path safe to carry in the
 *  ASCII-only `X-Komo-Workspace` header. */
export function encodeFolder(path: string): string {
  const bytes = new TextEncoder().encode(path);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return `folder:${btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "")}`;
}

export function WorkspacePicker({
  workspace,
  onWorkspaceChange,
  locked = false,
}: {
  workspace: string;
  onWorkspaceChange: (workspace: string) => void;
  /** The conversation has started, so its workspace is fixed. Renders the choice
   *  as a static label rather than a disabled control: the gateway binds a
   *  session's workspace at creation and ignores every later request, so an
   *  interactive-looking picker here would promise something it can't do. */
  locked?: boolean;
}) {
  const addWorkspace = useAppStore((s) => s.addWorkspace);
  const chooseFolder = getFolderPicker();
  const { workspaces: items, isPending } = useWorkspaceCatalog();
  const current = items.find((item) => item.id === workspace);
  const currentLabel = workspaceLabel(workspace, items);
  const currentPath = workspacePath(workspace, items);
  // Base UI deliberately renders a SelectValue's raw value unless the root is
  // given an item catalogue. Folder ids are base64url-encoded paths, so showing
  // the raw value makes a selected folder look like garbled text.
  const currentFallback = current ? [] : [{ value: workspace, label: currentLabel }];
  const selectItems = [
    ...currentFallback,
    ...items.map((item) => ({ value: item.id, label: item.name })),
    ...(chooseFolder ? [{ value: CHOOSE_FOLDER, label: "选择其他文件夹…" }] : []),
  ];

  const choose = async () => {
    const folder = await chooseFolder?.();
    if (!folder) return;
    const selected = { id: encodeFolder(folder.path), name: folder.name, path: folder.path };
    addWorkspace(selected);
    onWorkspaceChange(selected.id);
  };

  if (locked) {
    return (
      <span
        className="inline-flex min-w-0 items-center gap-1.5 text-xs text-muted-foreground"
        title={currentPath ? `${currentLabel} · ${currentPath}` : currentLabel}
      >
        <FolderIcon className="size-3.5 shrink-0" />
        <span className="truncate">{currentLabel}</span>
      </span>
    );
  }

  return (
    <Select
      items={selectItems}
      value={workspace}
      onValueChange={(value) => {
        if (value === CHOOSE_FOLDER) void choose();
        else if (value) onWorkspaceChange(value);
      }}
    >
      <SelectTrigger
        size="sm"
        showIndicator={false}
        className="min-w-0 max-w-[min(12rem,100%)] pr-2.5"
        title="选择 workspace"
      >
        <FolderIcon className="size-4" />
        <SelectValue
          className="min-w-0"
          placeholder={isPending ? "加载 workspace…" : "选择 workspace"}
        />
      </SelectTrigger>
      <SelectContent>
        {items.map((item) => (
          <SelectItem key={item.id} value={item.id} title={item.path}>
            {item.name}
          </SelectItem>
        ))}
        {chooseFolder && <SelectItem value={CHOOSE_FOLDER}>选择其他文件夹…</SelectItem>}
      </SelectContent>
    </Select>
  );
}
