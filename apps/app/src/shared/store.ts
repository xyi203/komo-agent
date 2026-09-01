// Client-side state: the active session, the workspace and model bound to it,
// per-workspace trust, and theme. Server-side state lives in react-query.
//
// Reopening the UI returns to the same conversation. A workspace is chosen while
// the conversation is still empty and then travels with that session forever
// (the gateway locks it in when the session row is created). The model is
// switchable at any time, so it is tracked per session and sent with every turn.

import { create } from "zustand";
import { persist } from "zustand/middleware";

import type { Mode, WorkspaceInfo } from "./types";
import { applyTheme, initialTheme, type Theme } from "./lib/theme";
import { newSessionId } from "./lib/session-id";

/** A session's model choice. Either field empty = fall back to the gateway /
 *  provider default. */
export interface ModelChoice {
  model: string;
  effort: string;
}

const NO_CHOICE: ModelChoice = { model: "", effort: "" };

export interface AppStore {
  session: string;
  workspace: string;
  workspaceModes: Record<string, Mode>;
  /** Per-session model/effort, keyed by session id. The gateway also stores this
   *  on the session row (so another client sees it); this is the local copy the
   *  picker edits, including for a session that has no row yet. */
  sessionModels: Record<string, ModelChoice>;
  /** Folders picked through the host's native dialog, keyed by workspace id.
   *  They are not in the gateway's catalog, so the client is what remembers
   *  them — hence persisted, like every other workspace-keyed slice here. */
  pickedWorkspaces: Record<string, WorkspaceInfo>;
  theme: Theme;
  openSession: (id: string, workspace: string) => void;
  startNewSession: () => void;
  /** Rebind the active (still empty) session's workspace. The composer only
   *  offers this before the first message; the gateway is the real lock. */
  setWorkspace: (id: string) => void;
  addWorkspace: (workspace: WorkspaceInfo) => void;
  setModelChoice: (session: string, choice: ModelChoice) => void;
  setMode: (workspace: string, mode: Mode) => void;
  toggleTheme: () => void;
}

export const useAppStore = create<AppStore>()(
  persist(
    (set) => ({
      // Seeded by `installHost()` before the first render — the host tag isn't
      // known when this module is imported.
      session: "",
      workspace: "__default__",
      workspaceModes: {},
      sessionModels: {},
      pickedWorkspaces: {},
      theme: initialTheme(),
      openSession: (session, workspace) => set({ session, workspace }),
      // A new conversation inherits the current workspace and model, which is
      // what "start another one like this" should mean; both stay editable until
      // the first message for the workspace, and always for the model.
      startNewSession: () =>
        set((s) => {
          const session = newSessionId();
          return {
            session,
            sessionModels: {
              ...s.sessionModels,
              [session]: s.sessionModels[s.session] ?? NO_CHOICE,
            },
          };
        }),
      setWorkspace: (workspace) => set({ workspace }),
      // Picking a folder only registers its display name; selecting it is the
      // caller's move, so an open conversation cannot be rebound by accident.
      addWorkspace: (workspace) =>
        set((s) => ({
          pickedWorkspaces: { ...s.pickedWorkspaces, [workspace.id]: workspace },
        })),
      setModelChoice: (session, choice) =>
        set((s) => ({ sessionModels: { ...s.sessionModels, [session]: choice } })),
      setMode: (workspace, mode) =>
        set((s) => ({ workspaceModes: { ...s.workspaceModes, [workspace]: mode } })),
      toggleTheme: () =>
        set((s) => {
          const theme: Theme = s.theme === "dark" ? "light" : "dark";
          applyTheme(theme);
          return { theme };
        }),
    }),
    {
      name: "komo.app",
      partialize: (s) => ({
        session: s.session,
        workspace: s.workspace,
        workspaceModes: s.workspaceModes,
        sessionModels: s.sessionModels,
        pickedWorkspaces: s.pickedWorkspaces,
        theme: s.theme,
      }),
    },
  ),
);

export const useSession = () => useAppStore((s) => s.session);
export const useWorkspace = () => useAppStore((s) => s.workspace);
export const useMode = (workspace?: string) =>
  useAppStore((s) => s.workspaceModes[workspace ?? s.workspace] ?? "interactive");
export const useModelChoice = (session: string) =>
  useAppStore((s) => s.sessionModels[session] ?? NO_CHOICE);
export const useTheme = () => useAppStore((s) => s.theme);
