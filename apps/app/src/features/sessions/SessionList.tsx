import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArchiveIcon,
  ArchiveRestoreIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  FolderIcon,
  LeafIcon,
  PencilIcon,
  PlusIcon,
  SettingsIcon,
  SlidersHorizontalIcon,
  Trash2Icon,
} from "lucide-react";

import { qk } from "@/shared/api/query-keys";
import { useConnection } from "@/shared/api/use-connection";
import { POLL } from "@/shared/config";
import { fmtTs } from "@/shared/lib/format";
import { cn } from "@/shared/lib/utils";
import { useAppStore, useSession } from "@/shared/store";
import type { SessionSummary } from "@/shared/types";
import { Button } from "@/shared/ui/button";
import { IconButton } from "@/shared/ui/icon-button";
import { Input } from "@/shared/ui/input";
import { KomoLogo } from "@/shared/ui/komo-logo";
import { Popover, PopoverContent, PopoverTitle, PopoverTrigger } from "@/shared/ui/popover";
import { fetchSessions, renameSession, setSessionStatus } from "./api";
import { DEFAULT_WORKSPACE, groupByWorkspace } from "./grouping";
import { sessionLabel } from "./labels";
import { useWorkspaceCatalog } from "@/features/workspaces/use-catalog";

const ROW = "group flex w-full items-center rounded-md px-2.5 py-1.5 transition-colors";

export function SessionList({
  mobileOpen,
  onMobileOpenChange,
  onOpenSettings,
  view,
  onViewChange,
}: {
  mobileOpen: boolean;
  onMobileOpenChange: (open: boolean) => void;
  onOpenSettings: () => void;
  /** Which surface the main area is showing. */
  view: "chat" | "memory";
  onViewChange: (view: "chat" | "memory") => void;
}) {
  const { connected } = useConnection();
  const session = useSession();
  const openSession = useAppStore((s) => s.openSession);
  const startNewSession = useAppStore((s) => s.startNewSession);
  const setModelChoice = useAppStore((s) => s.setModelChoice);
  const qc = useQueryClient();
  const [editingId, setEditingId] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [collapsed, setCollapsed] = useState(false);
  const [filter, setFilter] = useState<"active" | "archive" | "all">("active");

  const query = useQuery({
    queryKey: qk.sessions,
    queryFn: fetchSessions,
    refetchInterval: POLL.sessions,
    enabled: connected,
  });
  const sessions = query.data ?? [];
  const { workspaces } = useWorkspaceCatalog();

  const invalidate = () => void qc.invalidateQueries({ queryKey: qk.sessions });

  const rename = useMutation({
    mutationFn: ({ id, title }: { id: string; title: string }) => renameSession(id, title),
    onSettled: invalidate,
  });

  const restatus = useMutation({
    mutationFn: ({ id, status }: { id: string; status: string }) => setSessionStatus(id, status),
    onSuccess: (_data, { id, status }) => {
      // Leaving the open session (deleted/archived) → drop into a fresh one.
      if (id === session && status !== "active") startNewSession();
    },
    onSettled: invalidate,
  });

  const commitRename = (id: string) => {
    const title = draft.trim();
    setEditingId(null);
    rename.mutate({ id, title });
  };

  const remove = (id: string) => {
    if (!window.confirm("删除该会话？（软删除，从列表移除）")) return;
    restatus.mutate({ id, status: "deleted" });
  };

  const visibleSessions = sessions.filter((item) =>
    filter === "all"
      ? true
      : filter === "archive"
        ? item.status === "archive"
        : item.status !== "archive",
  );
  const groupedSessions = groupByWorkspace(visibleSessions, workspaces);

  // Adopt the model the active conversation last ran on, whenever this client has
  // no choice of its own for it. That covers both entry paths with one rule —
  // clicking a session the client has never seen, and reopening the app on a
  // persisted session id — so the composer can't show the default while the
  // server holds something else and then reset it on the next turn.
  //
  // Guarded on "no local entry", so it seeds once and never fights a switch the
  // user just made (which is local until a turn persists it).
  const activeRow = sessions.find((item) => item.id === session);
  useEffect(() => {
    if (!activeRow || useAppStore.getState().sessionModels[activeRow.id]) return;
    setModelChoice(activeRow.id, {
      model: activeRow.model ?? "",
      effort: activeRow.effort ?? "",
    });
  }, [activeRow, setModelChoice]);

  const renderRow = (item: SessionSummary) => {
    const isOpen = item.id === session;
    const isArchived = item.status === "archive";
    // One line, and it is whatever names the conversation: the title — which
    // the gateway now derives from the opening message for anything nobody
    // renamed — else a name the id carries (`homeassistant:events`), else the
    // time it was opened. A row earns its second line by saying something the
    // first cannot, and "1 轮 · 08-08 13:10" never did; dropping it roughly
    // doubles how many conversations are reachable without scrolling.
    const name = item.title?.trim() || sessionLabel(item.id);
    const headline = name ?? fmtTs(item.created_at);
    const tint = isOpen
      ? "bg-primary/10 ring-1 ring-primary/20"
      : "hover:bg-sidebar-accent hover:text-sidebar-accent-foreground";

    if (editingId === item.id) {
      return (
        <div key={item.id} className={cn(ROW, tint)}>
          <Input
            autoFocus
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") commitRename(item.id);
              else if (e.key === "Escape") setEditingId(null);
            }}
            onBlur={() => commitRename(item.id)}
            className="h-7 flex-1 text-sm"
          />
        </div>
      );
    }

    return (
      <div key={item.id} className={cn(ROW, tint)}>
        <div className="flex min-w-0 flex-1 items-center gap-1">
          <button
            type="button"
            className="min-w-0 flex-1 text-left"
            onClick={() => {
              openSession(item.id, item.workspace ?? DEFAULT_WORKSPACE);
              onViewChange("chat");
              onMobileOpenChange(false);
            }}
            // The row truncates in CSS, so the tooltip carries the whole
            // headline as well as the id — which stays the thing you copy to
            // reach the conversation from `komo run` or the ledger.
            title={`${headline}\n${item.id}`}
          >
            <span className={cn("block truncate text-sm", !name && "tabular-nums")}>
              {headline}
            </span>
          </button>
          <div className="flex shrink-0 gap-0.5 opacity-0 transition-opacity group-hover:opacity-100">
            <IconButton
              title="重命名"
              onClick={() => {
                setDraft(item.title ?? "");
                setEditingId(item.id);
              }}
            >
              <PencilIcon className="size-3.5" />
            </IconButton>
            {isArchived ? (
              <IconButton
                title="取消归档"
                onClick={() => restatus.mutate({ id: item.id, status: "active" })}
              >
                <ArchiveRestoreIcon className="size-3.5" />
              </IconButton>
            ) : (
              <IconButton
                title="归档"
                onClick={() => restatus.mutate({ id: item.id, status: "archive" })}
              >
                <ArchiveIcon className="size-3.5" />
              </IconButton>
            )}
            <IconButton title="删除" danger onClick={() => remove(item.id)}>
              <Trash2Icon className="size-3.5" />
            </IconButton>
          </div>
        </div>
      </div>
    );
  };

  return (
    <aside
      className={cn(
        "relative flex min-h-0 shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground transition-[width] duration-200 max-sm:absolute max-sm:z-40 max-sm:h-full max-sm:shadow-xl max-sm:transition-transform",
        collapsed ? "w-14" : "w-[264px]",
        mobileOpen ? "max-sm:translate-x-0" : "max-sm:-translate-x-full",
      )}
    >
      <div
        className={cn(
          "flex h-14 shrink-0 items-center border-b border-sidebar-border/75",
          collapsed ? "justify-center px-2" : "gap-2.5 px-4",
        )}
      >
        <KomoLogo className="size-7 shrink-0" />
        {!collapsed && (
          <div className="flex min-w-0 flex-col leading-none">
            <span className="font-semibold tracking-tight">komo</span>
            <span className="mt-1 text-[10px] font-medium tracking-wide text-muted-foreground">
              PERSONAL AGENT
            </span>
          </div>
        )}
        {!collapsed && <span className="flex-1" />}
        {!collapsed && (
          <span
            className={cn(
              "size-2 rounded-full transition-colors duration-(--duration-base) ease-(--ease-komo)",
              connected
                ? "bg-success shadow-[0_0_0_3px_color-mix(in_oklch,var(--success)_18%,transparent)]"
                : "bg-destructive",
            )}
            title={connected ? "已连接" : "未连接"}
          />
        )}
        <Button
          variant="ghost"
          size="icon-xs"
          className={cn(
            collapsed &&
              "absolute -right-3 top-3 z-10 rounded-full border border-border bg-background shadow-sm",
          )}
          title={collapsed ? "展开侧边栏" : "折叠侧边栏"}
          onClick={() => setCollapsed((value) => !value)}
        >
          {collapsed ? <ChevronRightIcon /> : <ChevronLeftIcon />}
        </Button>
      </div>

      <div
        className={cn(
          "flex flex-col gap-1.5 border-b border-sidebar-border/75 py-3",
          collapsed ? "items-center px-2" : "px-3",
        )}
      >
        {/* New session only switches the active id — it does NOT add a row. Its
            workspace is chosen in the composer (above the input, while the
            conversation is still empty) and persisted with the first message. */}
        <Button
          className={collapsed ? "size-9 px-0" : "w-full justify-start shadow-sm"}
          onClick={() => {
            startNewSession();
            onViewChange("chat");
            onMobileOpenChange(false);
          }}
          title="新建会话"
        >
          <PlusIcon />
          {!collapsed && <span>新建会话</span>}
        </Button>
        <Popover>
          <PopoverTrigger
            aria-label="筛选会话"
            className={cn(
              "inline-flex h-9 items-center justify-center gap-2 rounded-md text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground",
              collapsed ? "w-9" : "w-full",
            )}
          >
            <SlidersHorizontalIcon className="size-4" />
            {!collapsed && <span>筛选会话</span>}
          </PopoverTrigger>
          <PopoverContent side="right" align="start" className="w-52 gap-1 p-2">
            <PopoverTitle className="px-2 py-1 text-xs text-muted-foreground">
              会话状态
            </PopoverTitle>
            {(
              [
                ["active", "进行中"],
                ["archive", "已归档"],
                ["all", "全部会话"],
              ] as const
            ).map(([value, label]) => (
              <Button
                key={value}
                variant={filter === value ? "secondary" : "ghost"}
                className="w-full justify-start"
                onClick={() => setFilter(value)}
              >
                <span
                  className={cn(
                    "size-2 rounded-full",
                    filter === value ? "bg-primary" : "bg-muted-foreground/30",
                  )}
                />
                {label}
              </Button>
            ))}
          </PopoverContent>
        </Popover>
      </div>

      {!collapsed && (
        <div className="flex min-h-0 flex-1 flex-col gap-0.5 overflow-y-auto px-2 py-3">
          {!connected ? (
            <div className="px-3 py-3 text-sm text-muted-foreground">未连接</div>
          ) : query.isPending ? (
            <div className="px-3 py-3 text-sm text-muted-foreground">加载中…</div>
          ) : visibleSessions.length === 0 ? (
            <div className="px-3 py-3 text-sm text-muted-foreground">没有符合条件的会话</div>
          ) : (
            groupedSessions.map((group) => (
              <section key={group.workspace} className="pt-4 first:pt-0">
                <h2
                  className="flex items-center gap-1.5 px-3 pb-1.5 text-[11px] font-semibold tracking-wide text-muted-foreground"
                  title={group.label}
                >
                  <FolderIcon className="size-3 shrink-0" />
                  <span className="truncate">{group.label}</span>
                  <span className="shrink-0 tabular-nums opacity-60">{group.entries.length}</span>
                </h2>
                {group.entries.map(renderRow)}
              </section>
            ))
          )}
        </div>
      )}

      <div
        className={cn(
          "mt-auto border-t border-sidebar-border p-2",
          collapsed && "flex justify-center",
        )}
      >
        <div className={cn("flex flex-col gap-0.5", collapsed && "items-center")}>
          <Button
            variant={view === "memory" ? "secondary" : "ghost"}
            className={collapsed ? "size-9 px-0" : "w-full justify-start"}
            onClick={() => {
              onViewChange(view === "memory" ? "chat" : "memory");
              onMobileOpenChange(false);
            }}
            title="记忆"
          >
            <LeafIcon />
            {!collapsed && <span>记忆</span>}
          </Button>
          <Button
            variant="ghost"
            className={collapsed ? "size-9 px-0" : "w-full justify-start"}
            onClick={() => {
              onOpenSettings();
              onMobileOpenChange(false);
            }}
            title="设置"
          >
            <SettingsIcon />
            {!collapsed && <span>设置</span>}
          </Button>
        </div>
      </div>
    </aside>
  );
}
