import { useConnection } from "@/shared/api/use-connection";
import { useAppStore, useMode, useWorkspace } from "@/shared/store";
import { Badge } from "@/shared/ui/badge";
import { Switch } from "@/shared/ui/switch";
import { FIELD } from "../panel-styles";
import { StatusTab } from "./status";

export function GeneralTab() {
  const { connected } = useConnection();
  const workspace = useWorkspace();
  const mode = useMode(workspace);
  const setMode = useAppStore((s) => s.setMode);
  return (
    <div className="flex flex-col">
      <label className={FIELD}>
        <div>
          <div className="text-sm">信任模式（自动批准）</div>
          <div className="mt-0.5 text-xs text-muted-foreground">
            开启后副作用工具自动批准（等同 komo chat）；关闭则弹出审批。
          </div>
        </div>
        <Switch
          checked={mode === "trusted"}
          onCheckedChange={(v) => setMode(workspace, v ? "trusted" : "interactive")}
        />
      </label>

      <div className={FIELD}>
        <div>
          <div className="text-sm">连接状态</div>
          <div className="mt-0.5 text-xs text-muted-foreground">komo gateway 的实时连接。</div>
        </div>
        <Badge variant={connected ? "ok" : "warn"}>{connected ? "已连接" : "未连接"}</Badge>
      </div>

      {connected && <StatusTab />}
    </div>
  );
}
