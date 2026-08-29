import type { ReactNode } from "react";

import { qk } from "@/shared/api/query-keys";
import { Badge } from "@/shared/ui/badge";
import { ErrorLine } from "@/shared/ui/error-line";
import { Loading } from "@/shared/ui/loading";
import { fetchStatus } from "../api";
import { FIELD } from "../panel-styles";
import { usePanelQuery } from "../use-panel-query";

function Field({ label, hint, children }: { label: string; hint: string; children: ReactNode }) {
  return (
    <div className={FIELD}>
      <div className="min-w-0">
        <div className="text-sm">{label}</div>
        <div className="mt-0.5 text-xs text-muted-foreground">{hint}</div>
      </div>
      <div className="min-w-0 text-right text-sm">{children}</div>
    </div>
  );
}

/** A real count, and the only shape that earns the big-number treatment. */
function Metric({ value, label }: { value: number; label: string }) {
  return (
    <div className="rounded-xl border border-border bg-card p-3.5 text-center">
      <div className="text-[22px] font-bold tabular-nums text-foreground">{value}</div>
      <div className="mt-1 text-xs text-muted-foreground">{label}</div>
    </div>
  );
}

/** The gateway's own vitals, folded into the general tab below its controls.
 *
 *  Split by what the value *is*, not by where it came from. A version string
 *  and a chat id are identifiers: they are read once, they are long, and set in
 *  a centred 22px card they were truncated to `0.1.0+183…` and `telegram…` —
 *  the two values a person would have come here to copy. They belong on the
 *  same label/value line as the controls above. Only the two counts are
 *  quantities worth glancing at, and those keep the cards. */
export function StatusTab() {
  const query = usePanelQuery(qk.status, fetchStatus);
  if (query.isPending) return <Loading />;
  if (query.error) return <ErrorLine error={query.error} />;
  const status = query.data!;
  return (
    <div className="flex flex-col">
      <Field label="版本" hint="正在运行的 komo 构建。">
        <span className="font-mono text-xs break-all">{status.version}</span>
      </Field>

      <Field label="Home 聊天" hint="主动推送（简报、提醒、定时任务）发往这里。">
        {status.home_chat ? (
          <span className="font-mono text-xs break-all">{status.home_chat}</span>
        ) : (
          <span className="text-muted-foreground">未设置</span>
        )}
      </Field>

      <Field label="渠道" hint="已启用的接入方式。">
        {status.channels.length === 0 ? (
          <span className="text-muted-foreground">无</span>
        ) : (
          <span className="flex flex-wrap justify-end gap-1.5">
            {status.channels.map((channel) => (
              <Badge key={channel} variant="secondary">
                {channel}
              </Badge>
            ))}
          </span>
        )}
      </Field>

      <div className="grid grid-cols-2 gap-2.5 pt-4">
        <Metric value={status.open_tasks} label="开放任务" />
        <Metric value={status.sessions} label="会话数" />
      </div>
    </div>
  );
}
