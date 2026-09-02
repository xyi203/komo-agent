# Bot 运行时：持久化等待、触发器与任务

范围：komo 从「个人聊天 Agent」转到「7×24 常驻的个人 AI Bot 运行时」需要补的运行时原语——
一个 turn 如何跨小时/跨天地等待并恢复、什么事件能唤醒它、routine 如何从 cron 泛化、Task 放在哪。
不含：provider 层、memory 治理、skills、apps/ 客户端、多 Bot 编排。

依据三份材料，按可信度排序：

1. 对现有代码的逐条核对（§1）。
2. `.scratch/turn-durability/PRD.md`（主仓）——session 权威事件日志。本 PRD 的一切等待/恢复都建在
   它的第一、二批之上，不另起一套持久化。
3. Grok Bot 0.18 渲染层契约（`/Users/xiangyi/01-code/grok-bot/frontend/src/recovered/`）。
   **只有前端**，后端 coordinator 源码不在本地；从 UI 契约反推数据模型，看得到形状，看不到实现。
   每处引用都标了文件。

另有一份外部架构建议（Bot → Task → Wakeup/Resume，Session 降级为执行轨迹）。它的定位判断被采纳，
它的 Task 表、BotId、独立 Routine 模型被本 PRD 否决，理由见 §2。

---

## 0. 重新定义

komo 是一个 7×24 运行的个人 AI Bot 运行时：Bot 拥有长期身份（SOUL.md / USER.md）、记忆、工作区、
routines，可以被消息、定时器和外部事件唤醒，持续执行跨小时/跨天的工作，并在必要时委派临时子 agent。

这句话里今天缺的只有一个词：**唤醒**。一个 turn 现在最多活 5 分钟（审批等待）或 10 分钟（提问等待），
进程一重启就没了；turn 结束后没有任何东西能在「对方回复了」「CI 跑完了」「两小时后」把它叫回来。
其他成分——身份、记忆、工作区、cron、channel、审计——都已经在。

---

## 1. 现状核对

| 能力 | 现状 | 缺口 |
|---|---|---|
| 审批等待 | 内存 `oneshot`，`APPROVAL_TIMEOUT = 300s`，超时即拒绝（`komo-agent/src/interaction.rs`）；`approval/requested`/`resolved` 已是 durable 事件（turn-durability 1.5） | 等待不跨进程；没有 `expired` 结局；无人值守 turn 根本到不了人 |
| 提问等待 | `ask_user` + `ClarifyState`，`CLARIFY_TIMEOUT = 600s`，每 turn 2 次；下一条用户消息即答案，新问题取代旧问题（`komo-services/src/clarify.rs`） | 同上：内存态，不跨进程；问题本身不是事件 |
| 定时唤醒 | `CronJob.next_run_at` + `CronJobSweep`（claim-before-run、晚到裁决、`@at` 一次性） | 只能**开始新 turn**，不能**继续挂起的 turn**；trigger 只有 cron 表达式 |
| 事件唤醒 | 无 | 全部 |
| 后台任务 | `delegate` 同步阻塞在父 turn 内；`shell` 有 `max_duration` | turn 不能「起个长任务然后先走」 |
| 精确恢复 | `resume_interrupted` 崩溃后重派；turn-durability 第二批 2.2「精确 resume」在做 | 精确 resume 是挂起/恢复的机械基础，本 PRD 依赖它 |
| Task | `domain/task.rs` kanban：`inbox/todo/waiting/done/cancelled` + `waiting_on` + `due_at`，`TaskSweep` 到期投递 | 是承诺清单，不是执行单元；名字已占用 |
| Routine | `CronJob {schedule, action: Command|Agent, status, catch_up, grants, workspace, last_output, last_run_session}`（`domain/cron.rs`） | 已是 Routine 的 70%：缺 trigger 泛化、缺逐次 run 历史 |
| 身份 | 单 persona（SOUL.md）、单 config、`CapabilityProfile` 按 runtime 分（main/cron/briefing/delegate） | 无 BotId，也不需要（§2 D1） |
| 工作区 | `Session.workspace`、`CronJob.workspace`、`SessionContext::workspace_root`、checkpoints | 缺 per-task artifacts 目录；浏览器登录态在没有 browser 工具前不可执行 |
| 通知 | `HomeNotifier`：sethome > `home_chat`，feishu 优先 | 全局一把，无 per-routine 策略 |

---

## 2. 设计决定

### D1 · Bot 是定位，不是字段

**决定**：不引入 `BotId`。一个 Bot = (SOUL.md, memory scope, workspace, channel 集) 的 bundle；
今天只有一个，所有结构体都不为第二个预留字段。出现第二个 Bot 时再把 bundle 显式化。

Grok 的 roster 是多租户云产品形态（agent 上限 50，group ≤ 6，shared room 人机混合，
`org-chart/workspace/model.ts`）。komo 单机单人，这一层没有消费者。

### D2 · 等待是日志事实 + 一条唤醒登记；状态是投影

**决定**：一个 turn 的等待用两样东西表达——

- session 日志里一条 `turn/suspended` 事件（这个 turn 停在哪、等什么）；
- 调度器里一条 **唤醒登记**（什么条件、到什么时候、指向哪个 session/turn）。

turn 的状态（Running / WaitingApproval / WaitingUser / WaitingExternal / Scheduled …）**不存**，
从日志 fold：有 `approval/requested` 无 `resolved` = WaitingApproval；有 `turn/suspended` 无
`turn/started{resumed_from}` = 在等；有 `turn/started` 无终止事件 = Running 或 interrupted。

否决外部建议里的 `Task { status, session_id, next_wakeup_at }` 可变行：turn-durability 刚把
Run/RunStep 改成日志投影，再加一张 status 表就是回到两个权威源互相同步。Grok 的 UI 也是这么做的：
agent 上只有 `isRunning` 和 `awaitingUserResponse` 两个字段，`idle | typing | working | waiting`
由 `getAgentActivity` 推导（`features/org-chart/workspace/model.ts:63-75`）。

### D3 · 一个调度器：CronJob 泛化为 Routine，唤醒登记走同一个 sweep

**决定**：`CronJob.schedule: String` 变成 `trigger: Trigger`（tagged union，见 §3.3）；
唤醒登记与 routine 由同一个 `CronJobSweep` 驱动。统一原语是一条持久登记：

> 「X 发生时，在 session Z 上以授权 G **开始或继续** turn Y」

routine 是「开始新 turn」，唤醒登记是「继续挂起的 turn」或「以结果开始新 turn」。claim-before-run、
晚到裁决、`--skip-missed` 全部复用。不在 cron.db 旁边建第二套 Routine 模型。

Grok 的 Automation 形状是 `{name, prompt, trigger, isEnabled, runs[]}`，trigger 为
`cron | slack | github | microsoftTeams | linear | sentry | pagerduty` 之一，或
`{type: "group", listeners: [≤8]}`（任一触发）——`features/automations/routines/trigger-schema.ts`。
这与 CronJob 的差别只有 trigger 一个字段和 run 历史一个列表。

### D4 · 不建执行型 Task；kanban Task 的「等待」接到 Wakeup 上

**决定**：不引入第二个 Task 类型。kanban `Task` 仍是承诺清单，但它的 `Waiting` 状态不再只是一个
标签：一个进入 `Waiting` 的 Task 登记一条 `Wakeup::Event{ sender 匹配 waiting_on }`，被等的人一来消息，
就在 Task 的来源 session 上开一个 turn（§3.7）。agent「正在等什么」仍是 session 投影上的
`awaiting` 字段，来源是 D2 的 fold。

理由：Grok 的渲染层没有任何 Task / Goal 对象——顶层只有 agent、automation（及 runs）、async task
三种。「会话 + 等待 + routine run + 后台任务」把 Task 表要覆盖的东西全覆盖了，所以不建执行型 Task。
但 kanban 里已经有「等张三回复」这一类 Task，而 Wakeup 就是「等」的运行时形态——两个「等」不打通，
就是同一件事两套表达。roadmap §4 写「刻意不做 worker claim，因为 komo 是单 turn 助理」，
前提被新定位推翻，结论换成：执行不需要 Task，但 Task 的等待需要执行来兑现。

### D5 · 审批、提问、交接、后台任务是同一原语的四个变体

**决定**：四件事共用 `turn/suspended` + 唤醒登记，差别只在 `Wakeup` 变体和恢复时喂回模型的内容：

| 变体 | 挂起原因 | 唤醒条件 | 恢复喂回 | Grok 对应 |
|---|---|---|---|---|
| Approval | 工具需要审批 | `/approve` `/deny`、超时 | `approval/resolved` 后工具结果 | `auto-review-approval` 卡：`{requestId, status: pending|approved|always|denied|expired}`（`transcript-card/protocol.ts`） |
| UserReply | `ask_user` | 下一条用户消息、超时 | 答案文本 | widget 卡：`{prompt, options, allowCustom, dismissOnMoveOn}`，落地记 `respondedValue/widgetSkipped/widgetDismissed` |
| Handoff | 「你去登录一下 / 做件事然后告诉我」 | 用户消息 | 用户说的话 | `ComputerHandoff {requestId, instruction}` → `waiting | handed_back | replied | dismissed`（`computer/shell/model.ts`） |
| TaskDone | 后台 shell / delegate | 任务结算 | 任务结果引用 | `AsyncTask {kind: subagent|shell|cloud-agent, status: running}`（`agent-info/async-tasks/provider.ts`） |
| At / Event | 模型主动 `wait` | 时间到 / 事件命中 | 事件描述 | routine `event` 字段 |

Handoff 不是新工具：就是 `ask_user` 带一段 instruction，恢复条件相同。表里单列是因为它决定了
文案和通知方式，不决定模型。

---

## 3. 目标数据模型

### 3.1 新增 SessionEvent

```
turn/suspended   { turn_id, wakeup: Wakeup, summary, expires_at? }
wakeup/fired     { turn_id, wakeup_id, cause: "approve"|"deny"|"reply"|"time"|"event"|"task"|"expired", payload? }
approval/expired { turn_id, call_id, call_index }
task/spawned     { turn_id, task_id, kind: "shell"|"delegate", label }
task/settled     { task_id, outcome, result_ref, elapsed_ms }
```

- `turn/suspended` 是 durable barrier（同 `approval/requested`）：挂起前落盘，否则崩溃后不知道
  turn 是死在等待还是死在执行。
- 恢复走 turn-durability 的精确 resume：`turn/started{resumed_from}` + `request/header{reason: resume}`；
  `wakeup/fired` 是两者之间的因果链，没有它「为什么恢复」在日志里不可见。
- `approval/expired` 是 `approval/resolved` 之外的第三个结局，恢复后作为拒绝结果喂回模型。
- 后台任务的 `task/settled` 可以落在 turn 结束之后——这是它和工具调用的本质区别。

### 3.2 Wakeup 与唤醒登记

```rust
enum Wakeup {
    At { at: i64 },
    Approval { call_id: String },
    UserReply,
    TaskDone { task_id: String },
    Event { filter: EventFilter },
}

struct WakeupRegistration {
    id: String,                 // UUIDv7
    session_id: String,
    turn_id: Option<String>,    // Some = 继续挂起的 turn；None = 以 payload 开始新 turn
    wakeup: Wakeup,
    expires_at: Option<i64>,    // 过期 = 以 cause=expired 唤醒，不是静默丢弃
    grants: Vec<RuleSpec>,      // 从挂起时的 turn 继承（cron job 的 grants 一路带下去）
    created_at: i64,
}
```

**存放**：cron.db（durable），与 routine 同库，同一个 sweep 读。session 日志是「turn 在做什么」的权威，
登记是「何时叫它」的权威；fire 时先读日志核对该 turn 确实挂起且未恢复，对不上就丢弃登记并 warn。
启动时反向核对一次：有 `turn/suspended` 无登记的 session 补登记（只扫最近 N 个活跃 session）。

`expires_at` 默认值按变体：Approval 24h、UserReply 7d、At 无、TaskDone 无（任务自己有超时）、
Event 30d。过期一律以 `cause: expired` 唤醒并把「没等到」告诉模型，**不静默丢弃**——
一个从未被回答的问题不能让 turn 永远悬着。

### 3.3 Routine（CronJob 泛化）

```rust
enum Trigger {
    Cron { expr: String },                          // 现有 5 段 + `@every 30m` + `CRON_TZ=`
    At { at: i64 },                                 // 现有 `@at`
    Feishu { chat: String, match: FeishuMatch },    // mention | keyword(s) | reaction(emoji)
    Webhook { name: String },                       // POST /api/hooks/{name}，bearer key
    FileChanged { root: PathBuf, glob: String },
    Any(Vec<Trigger>),                              // ≤ 8，任一命中
}

struct RoutineRun {
    id: String,
    status: RoutineRunStatus,   // running | ok | error
    started_at: i64,
    event: String,              // 触发它的事件的一行描述：cron 槽位 / 消息摘要 / 文件路径
    session_id: Option<String>, // agent 模式：那次 turn 的 session
    output: String,             // 有界
}
```

`CronJob.schedule` → `trigger`；`last_output` / `last_run_session` / `last_run_at` / `last_status`
→ `runs: Vec<RoutineRun>`（保留最近 20 条）。不留兼容层：cron.db 按 AGENTS.md 规则删库重建
（`CronJobRecord → cron.db` 在「非加性变更删文件」清单里）。

事件触发的 routine 不记 `event` 就说不清「这次为什么跑」——Grok 每条 run 都带
`event`（`routines/controller.ts: RoutineRun {id, status, startedAt, detail, event}`）。

`Feishu` trigger 命中的消息**不是**普通聊天输入：它以 routine 的 prompt 开一个 `origin = cron`
的 turn，消息作为 event 注入，走 routine 的 grants。否则群里任何人一个 emoji 就能触发有授权的动作。

### 3.4 `wait` 工具

模型主动挂起的入口，`Risk::Safe`，sentinel 工具（同 `ask_user`）：

```
wait { until: "2h" | "2026-09-03 09:00" }          → Wakeup::At
wait { for_task: "<task_id>" }                     → Wakeup::TaskDone
wait { for_event: { webhook: "ci-done" } }         → Wakeup::Event
```

调用即 `turn/suspended` + 登记；恢复后工具返回值是唤醒事件的描述。**无人值守 turn 也可以调**——
这正是 routine 能做「检查完等两小时再检查」的方式；它的 grants 随登记带下去。
每 turn 最多 `WAIT_BUDGET_PER_TURN = 4` 次，防止模型用 wait 代替结束。

### 3.5 后台任务

`shell` 新增 `background: true`；`delegate` 新增 `detach: true`。两者写 `task/spawned`，立即返回
task_id，turn 可以结束。任务结算写 `task/settled`，若有登记则唤醒：

- turn 仍挂起在 `TaskDone` → 精确恢复；
- turn 已结束 → 以结果开一个新 turn（`turn_id: None` 的登记），prompt 是「你之前起的任务 X 完成了，结果如下」。

后台任务最多 `MAX_BACKGROUND_TASKS_PER_SESSION = 3`；进程重启时 `running` 的后台 shell 一律判
`uncertain` 结算（进程组已死，是否完成未知）——和工具调用的 `Uncertain` 同一语义。

### 3.6 Session 投影新增 `awaiting`

```rust
struct Awaiting { kind: WakeupKind, since: i64, summary: String, expires_at: Option<i64> }
```

fold 自 3.1 事件，进 state.db 的 session 投影（可重建）。`komo session list` / TUI / apps 用它显示
「等你审批 · 3h」「等 CI · 已 40min」。这是 Grok `awaitingUserResponse` 的对应物。

### 3.7 kanban Task ↔ Wakeup

```rust
struct Task {
    // ...现有字段
    /// 被等待方的 channel 身份；None = waiting_on 只是一段文字，无法唤醒。
    waiting_on_peer: Option<ChannelPeer>,
    /// 进入 Waiting 时登记的唤醒；离开 Waiting 时撤销。
    wakeup_id: Option<String>,
}
```

- **进入 `Waiting`**（`task` 工具 `update status=waiting`、CLI、reviewer 提取）：若能解析出
  `waiting_on_peer`（消息来自 channel 时，模型从对话上下文给出 peer；CLI 显式传），登记
  `WakeupRegistration { session_id: task.source, turn_id: None, wakeup: Event{ filter: FromPeer(peer) }, expires_at: due_at.or(30d) }`。
  只有文字的 `waiting_on` 不登记，`komo task list` 标出「不可唤醒」。
- **命中**：该 peer 的任一入站消息先照常走它自己的会话（它是对方在和 komo 说话，或是群里说了话），
  同时 fire 登记：在 `task.source` 上开一个 turn，prompt 是「你在等 <waiting_on> 关于「<title>」的回复，
  刚收到：<消息>」。Task 不自动改状态——是否算「回复了」由模型判断，`task update` 完成或继续等。
- **`wait { for_task: <kanban task id> }`**：当前 turn 挂起到该 Task 的等待方回复（`turn_id: Some`，
  精确恢复）。同一个登记，两种消费方式。
- **离开 `Waiting`**（done / cancelled / 改回 todo）：撤销登记。
- `due_at` 到期仍由 `TaskSweep` 投递提醒，不改。

匹配的是 **peer**，不是名字字串：`waiting_on` 是给人看的，`ChannelPeer` 才是能对上入站消息的东西。
`FromPeer` 是 `EventFilter` 的一个变体，与 §3.3 `Feishu` trigger 共用匹配器。

---

## 4. 关键流程

### 4.1 有人在场的审批（今天的路径改造）

```
tool 请求审批
→ approval/requested (durable)
→ turn/suspended{Approval} (durable) + 登记{expires 24h}   ← 新：释放 session slot
→ 提示送达 channel / TUI
   ├ /approve /deny 到达 → wakeup/fired → 精确恢复 → approval/resolved → 工具执行
   └ 24h 无人 → wakeup/fired{expired} → approval/expired → 恢复，工具返回「审批过期」
```

变化点：turn 不再持有 session slot 等 5 分钟。一个挂起的 turn 不占槽，用户可以在同一 session
继续说话。

**挂起期间同 session 的普通消息 = 用户放弃了这次审批**：消息照现有 interjection 规则追加为
`user/message`；审批以 `approval/resolved{deny, feedback: 指向该消息}` 结算；`wakeup/fired{cause: moved-on}`；
turn 精确恢复，模型同时看到「被拒绝」和用户刚说的话，接着回应。消息不再另排一个 turn——它已经在
这个 turn 里被看见了。`/approve` `/deny` 仍是显式路径。这与 `ask_user` 的「下一条消息即答案」和
Grok widget 的 `dismissOnMoveOn` 是同一条规则：**一个 pending 的等待被下一条用户消息取代，不并存**。

### 4.2 无人值守的审批（本 worktree 的命名来源）

cron/briefing turn 遇到 grants 之外的 `Risk::Normal` 动作，今天直接拒绝。改为：

```
→ approval/requested + turn/suspended{Approval} + 登记
→ HomeNotifier 推送：「routine X 想执行 <summary>，回复 /approve <id> 放行」
→ home chat 里 /approve <id> → 唤醒 → 精确恢复 → 执行
```

`/approve` 需要带 id，因为 home chat 自己的 session 与 routine 的 session 不同——今天的
`/approve` 只作用于当前 session。`Risk::Dangerous` 仍然只 `Once`，规则不变。

Grok 在 `automation_write` surface 上也走同一审批（agent 改 routine 本身要审），komo 的
`cron:add` 审批已经是这个。

### 4.3 提问 / 交接

`ask_user` 改为写 `turn/suspended{UserReply}` + 登记（7d）。下一条用户消息即答案（现有语义保留），
`/skip` 显式跳过；超时以 expired 恢复，工具返回「没等到答案」，模型按声明的假设继续或收尾——
现有 `ask_user` 的降级文案不变。Handoff 只是 question 文本是一段 instruction。

### 4.4 事件触发

- Webhook：`POST /api/hooks/{name}`，bearer key 校验，body 有界；命中 routine → 开 turn；
  命中登记 → 唤醒。
- Feishu：channel 已收全部消息；在 `GatewayDispatcher::handle` 入口前加一个 `TriggerMatcher`，
  按 `FeishuMatch` 匹配，命中即投递给 sweep（不进聊天路径）。
- FileChanged：`notify` crate 监听 `root`，防抖 2s，glob 过滤。

---

## 5. 改动清单

前置：turn-durability 第二批 2.2「精确 resume」完成。本 PRD 的一切恢复都是它。

### 第一批 · 挂起原语 + 审批改造

- **5.1 事件词汇**：`turn/suspended` / `wakeup/fired` / `approval/expired` 进 `SessionEventKind`；
  fold 规则：suspended 后无 resumed 的 turn 不算 interrupted、不进 `reconcile_interrupted`。
  验证：fold 单测；注入崩溃在 suspended 前后，恢复判定分别为 interrupted / suspended。
- **5.2 唤醒登记**：`WakeupRegistration` 表进 cron.db；`CronJobSweep` 增加登记的 claim-before-fire；
  fire 时核对日志。验证：登记指向已恢复的 turn 被丢弃并 warn；重复 fire 不重复恢复。
- **5.3 审批改造**：`ChatApprover` / TUI approver / `PolicyApprover` 的等待从 oneshot 改为挂起；
  `/approve <id>` 跨 session；过期路径。验证：gateway 重启后 `/approve` 仍能恢复并执行；
  24h 过期后模型收到拒绝；`Risk::Dangerous` 仍 `Once`。
- **5.4 无人值守审批**：cron turn 的 deny-all 改为挂起 + HomeNotifier。验证：一个没有 grants 的
  routine 在 home chat 收到提示，`/approve` 后动作执行，`/deny` 后 routine 以 error 结算。
- **5.5 `awaiting` 投影**：session 列表与 TUI 显示。验证：挂起/恢复/过期三态各一测。

### 第二批 · `wait` 与后台任务

- **5.6 `wait` 工具**：三种参数 → 三种 Wakeup；预算；无人值守可用。验证：routine 里
  `wait 2h` 后 gateway 重启，到点仍恢复；预算耗尽返回引导文案。
- **5.7 `ask_user` 持久化**：换到挂起原语，行为不变。验证：提问后重启，回答仍被接上。
- **5.8 后台 shell / delegate**：`task/spawned|settled`，结算唤醒或开新 turn；重启后 running 判 uncertain。
  验证：turn 结束后任务完成，session 收到带结果的新 turn；父 turn 挂起在 TaskDone 时精确恢复。
- **5.9 kanban Task ↔ Wakeup**：`waiting_on_peer` / `wakeup_id` 列（kanban.db 加列，`ensure_columns`）；
  进入/离开 `Waiting` 登记/撤销；`FromPeer` 过滤器；`wait { for_task }`。
  验证：Task 等某 feishu peer，该 peer 来消息后 `task.source` 上出现一个带消息内容的新 turn，
  Task 状态未被自动改动；Task 完成后同一 peer 再来消息不再触发；纯文字 `waiting_on` 在 list 里标「不可唤醒」。

### 第三批 · Trigger 泛化

- **5.10 `Trigger` 枚举 + `runs` 历史**：替换 `schedule` 与 `last_*`；cron.db 重建；`komo cron`
  CLI/`cron` 工具/api 三个入口走同一 `cron_actions`。验证：现有 cron 测试全绿；`Any` 任一命中只跑一次。
- **5.11 Webhook**：`/api/hooks/{name}`。验证：无 key 401；命中登记唤醒；命中 routine 开 turn 且 `event` 记录 body 摘要。
- **5.12 Feishu match**：`TriggerMatcher`（与 5.9 的 `FromPeer` 共用）；命中不进聊天路径。
  验证：非 `allow_from` 的群成员 reaction 能触发 routine 但 turn 的 grants 是 routine 的，不是发送者的。
- **5.13 FileChanged**：`notify` + 防抖。验证：批量写 50 个文件只触发一次。

### 第四批 · 收口

- **5.14 per-routine 通知策略**：`notify: always | on_error | never`（Grok 每 agent 有
  `notificationsEnabled` / `notifyOnUpdatesEnabled`）。「有异常才告诉我」在这里。
- **5.15 per-task artifacts**：`~/.komo/artifacts/<session>/`，进 workspace 的可写 roots。
- **5.16 文档**：AGENTS.md 模块地图更新（cron → routine + wakeup；approval 一节改写；task 一节加唤醒）。

---

## 6. 明确不做

- 不加 `BotId`、不做 agent roster / group / shared room / org chart。
- 不建执行型 Task 表；kanban Task 只多两列（`waiting_on_peer`、`wakeup_id`），不长出 status 机之外的东西。
- 不按名字字串匹配来信人；`waiting_on` 解析不出 peer 就是不可唤醒，不猜。
- 不做 VM / forever box / VNC handoff；Handoff 只是带 instruction 的提问。
- 不做 secret-request（agent 按 label 向用户要密钥，存入 broker，工具按名引用；Grok 的
  `secret-request` 卡 + `SecretsSnapshot {keys[]}`）。它是 ADR 0002 credential-broker 那一半的
  已验证形态，理由与 job-scoped grants 一样——人在场时不该让人离开对话去改 `.env`——
  但它是独立特性，另起 PRD。
- 不自动重放 `uncertain` 的后台任务。
- 不做 LLM 自然语言 allow/block 指令列表（Grok `DesktopAutoReviewInstructions
  {allowInstructions[], blockInstructions[]}`）；komo 的 `[policy] mode = "auto"` 只以 operator
  最新消息为授权依据，ADR 0003 的边界不放宽。
- 不让挂起的 turn 无限期存在：每个变体都有 `expires_at` 默认值。

---

## 7. 决定记录与待拍板

已决（2026-09-02）：

- **Q1 kanban Task 与 Wakeup 打通** → 打通。Task 的 `Waiting` 登记一条 `FromPeer` 唤醒，详见 §3.7、5.9。
- **Q2 审批挂起期间用户在同一 session 说话** → 视为放弃审批：`Deny{feedback: 那条消息}` 结算并恢复
  turn，见 §4.1。Grok widget 的 `dismissOnMoveOn` 与现有 `Answer::Deny(feedback)` 都是这个形状。

待拍板：

- **Q3 登记存 cron.db 还是 state.db**。本 PRD 选 cron.db（durable，与 routine 同 sweep）。
  反对意见：登记可由日志重建，属于 disposable。反驳：重建要扫所有 session 的尾部，
  启动时做不起；durable + 启动时只核对最近 N 个更便宜。
- **Q4 过期时长默认值**（§3.2）：Approval 24h、UserReply 7d、Task 等待 `due_at` 否则 30d、Event 30d、At 无。

---

## 8. 完成判据

1. gateway 在审批等待、提问等待、`wait 2h`、后台任务运行中四种状态下被 kill 并重启，
   对应的 `/approve`、用户回答、到点、任务结算都能恢复原 turn 且不重跑已完成的工具调用。
2. 一个无 grants 的 agent routine 在 03:00 遇到需审批动作，08:00 操作员在 home chat `/approve <id>`，
   动作在 08:00 执行，ledger 显示 `waited_ms ≈ 5h`。
3. 挂起的 turn 不占 session slot：挂起期间同 session 的新消息能得到回复（按 Q2 决定处理挂起项）。
4. 每个 Wakeup 变体的过期路径都让模型收到明确的「没等到」，日志里有 `wakeup/fired{expired}`。
5. `Trigger::Any` 中任一命中只产生一条 `RoutineRun`，且 `event` 能说出是哪一个命中。
6. Feishu 触发的 routine turn 的授权集合是 routine 的 grants，与触发者身份无关。
7. 所有状态（Running / Waiting* / Scheduled）都由 fold 得出，state.db 的 session 投影清空后可重建。
8. 审批挂起期间用户发一条普通消息：日志里依次是 `user/message`、`approval/resolved{deny}`、
   `wakeup/fired{moved-on}`、`turn/started{resumed_from}`；模型的下一条回复同时回应了拒绝和那条消息；
   该消息没有再开第二个 turn。
9. 一个 `Waiting` 的 kanban Task，被等的 peer 在 feishu 来消息后，Task 来源 session 出现新 turn 且带消息内容；
   Task 标记 done 后同一 peer 再来消息不再触发。
10. `cargo test --workspace` 通过；`komo run inspect` 能读出 suspended → fired → resumed 的链。

---

## 9. 来源索引（grok-bot 渲染层）

- Routine trigger union / group：`features/automations/routines/trigger-schema.ts`
- Schedule 语法（`@every`、`CRON_TZ=`、别名）：`features/automations/routines/schedule.ts`
- Run 历史含 `event`：`features/automations/routines/controller.ts`
- 审批卡（requestId / expired / proposedRule / surface 含 `automation_write`）：
  `features/conversation/cards/transcript-card/protocol.ts`、`auto-review-actions.ts`
- 提问卡（`dismissOnMoveOn`）：同 `protocol.ts` `WidgetPrompt`
- 后台任务：`features/agent-info/async-tasks/provider.ts`
- Handoff：`features/computer/shell/model.ts`（`ComputerHandoff`、`ComputerHandoffResolution`）
- agent 活动状态由字段推导：`features/org-chart/workspace/model.ts`
- 未连接平台时的 `listener-connect` 卡：`features/conversation/cards/transcript-card/views/listener-connect.tsx`
- 凭证请求（不做，记录）：`.../views/secret-request.tsx`、`contracts/desktop-bridge.ts` `SecretsSnapshot`
