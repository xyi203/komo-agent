# Bot 运行时：持久化等待、触发器与任务

范围：komo 从「个人聊天 Agent」转到「7×24 常驻的个人 AI Bot 运行时」需要补的运行时原语——
一个 turn 如何跨小时/跨天地等待并恢复、什么事件能唤醒它、routine 如何从 cron 泛化、Task 放在哪。
不含：provider 层、memory 治理、skills、apps/ 客户端、多 Bot 编排。

依据三份材料，按可信度排序：

1. 对现有代码的逐条核对（§1）。
2. [`docs/turn-durability.md`](turn-durability.md)——session 权威事件日志。本 PRD 的一切等待/恢复都建在
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
| 会话身份 | `session_for(peer)`：一个聊天 peer 一条 session；TUI 每次启动一个新 session id，`komo resume <id>` 才接上；`/new` 换 session | 操作者自己的各个入口互相断裂，Telegram 上午聊的下午 TUI 里接不上；连同一台电脑两次开 TUI 都接不上 |

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

### D6 · Home conversation 按 principal 归并

三个词各管一件事，边界压成一句：**Session 是 durability / event log 的内部执行单元；Conversation
是用户连续性的路由语义；Task 只描述工作，不拥有对话上下文。**

**不变量**：

```
same principal + private conversation
  => same logical home conversation
  => same ordered session timeline
```

而不是 `sender/peer => session`，也不是 `message => task => context`。

**约束**：

- 操作者自己的私有入口——TUI / desktop / web / Telegram DM / 飞书 DM / 微信——全部落到**同一个
  home session**。
- 有其他参与者的对话（飞书群、任何 correspondent ≠ me 的 chat）按 correspondent 各自一条 session，
  今天的 `find_by_peer` 只服务这一类。
- transport peer **不决定 session identity**，只负责消息来源和回复目的地。回复回到消息来的那个 peer，
  即使 turn 是在另一个入口的上下文里跑的。
- TUI 启动默认进入 home session；`komo resume <id>` 保留给 correspondent session 和历史排查。
- `/new` 保留，语义是**显式的 context boundary**：之后默认不携带 boundary 之前的 conversational
  working context。durable task / memory / grants / 挂起中的 turn 是否失效，由**各自的生命周期规则**
  决定，`/new` 不碰它们。它不再是"大清理按钮"——把 Conversation、Task、Policy 三种生命周期重新
  耦合起来是这条规则要防的事。实现上它是同一条日志里的一条边界事件，不是换 session id，否则不变量
  的第三行就破了。
- session / event log / checkpoint / recovery 机制**完全不变**。
- 同一 home session 保持 **single writer + turn serialization**：Telegram 和 CLI 同时说话就排队，
  这是"像一个人"的代价。长任务并行由 background task 与 suspend + wakeup 承担（D5），不通过拆
  Task context 解决。

**三个必须跟着改的东西**：

1. `Session.workspace` 今天是建 session 时锁定的身份字段。一条 home session 会从不同目录的 TUI
   进入，workspace 只能是 **turn 的属性**（`SessionContext::workspace_root` 已经是），不再是 session
   身份的一部分。这与 CONTEXT 里"Profile = 谁，Workspace = 哪里"两条正交轴一致：哪里干活是每个
   turn 自己说的。
2. **system prompt 的 context 层保持 per process，不随 turn 的 workspace 变。** 缓存前缀的顺序是
   tools → system → messages，system 一变，后面整段 history 的缓存全部失效。今天 context 层（项目
   `AGENTS.md`）读的是进程 cwd，`system_prompt.rs` 文档里"stable within a session"这句已经不准，
   要改成 per process。若某个 turn 需要带上它所在目录的项目指令，照 recall 记忆的做法作为
   `MessageSource::Injected` 块放到该 turn 用户消息的**尾部**——新字节本来就在那里，对缓存零成本。
3. **D6 排在 compaction（turn-durability 第三批）之后上线。** 今天 TUI 一开是空会话，history 近零；
   合到 home session 后每个 turn 都背历史窗口，上限 `max_history_bytes = 256 KB`（约 64k token）。
   命中率不变——锚定窗口让 history 前缀每 6 轮左右才移一次，和今天的 Telegram 长会话一样——
   变的是每轮输入量。compaction 把老历史换成 summary 之后窗口不再靠字节上限硬切，这个成本才回到
   合理范围。若要提前上线，必须同时把 `max_history_bytes` 默认值调小。

另一个不变的事实要写明：Anthropic 的 ephemeral 缓存 TTL 是 5 分钟，komo 未设 1h。"上午 Telegram、
下午 TUI"这类跨时段接续从来不可能命中缓存，D6 没有让它变差；它带来的命中收益只在同一时段内的
跨入口接续，以及 Responses 系少掉一堆一次性的 `prompt_cache_key`。

**范围严格限制在 identity / routing**：不引入 Task Router，不新增 Task 状态副本，不动 event-log
权威模型，不顺手重构 storage。上下文的连续性由已有的三层叠加提供——最近窗口（`find_windowed`）、
compaction summary（turn-durability 第三批）、检索（`session` 工具 + L3 记忆召回）——路由是排他的，
检索是叠加的，判错时前者静默替换正确上下文，后者只是多塞几段无关内容。

---

## 3. 目标数据模型

### 3.1 新增 SessionEvent

```
turn/suspended   { turn_id, wakeup: Wakeup, summary, expires_at? }
wakeup/fired     { turn_id, wakeup_id, cause: "approve"|"deny"|"reply"|"time"|"event"|"task"|"expired", payload? }
approval/expired { turn_id, call_id, call_index }
task/spawned     { turn_id, task_id, kind: "shell"|"delegate", label }
task/settled     { task_id, outcome, result_ref, elapsed_ms }
conversation/boundary { }                      ← /new；只影响 surface fold 与窗口起点（§3.8）
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

### 3.8 会话解析（D6）

```
InboundMessage { peer, sender, text }
      │
      ▼
 Principal Resolution        allow_from / pairing 已有的事：sender 是不是 me
      │
      ▼
 Conversation Resolution
      │
      ├── me + private peer ─────► Home Session（唯一，settings 里记 id）
      │
      └── correspondent ─────────► Conversation Session（find_by_peer，首次接触时开）
                                        │
                                        ▼
                                 serialized turns（claim_session 不变）
                                        │
                       ┌────────────────┼────────────────┐
                     recent          compaction        retrieval
                     window           summary           memory
```

- **Home session 的 id** 是 settings 表里一条记录，首次需要时创建；没有第二条。
- **回复目的地**来自 `InboundMessage.peer`，随 turn 走（`ReplySink` 已经是按 turn 给的），与 session
  无关。一个在 TUI 里挂起、由 Telegram `/approve` 唤醒的 turn，恢复后的回复回 Telegram，TUI 下次
  打开在窗口里看到同一段。
- **新事件** `conversation/boundary { turn_id? }`：`/new` 写它；surface fold 从最近一条 boundary 之后
  开始取模型历史；`find_windowed` 的窗口不越过它。它对 seq、恢复、审批、投影都不可见——它只影响
  "模型默认看到多长的历史"。
- `todo`（session 级工作焦点）是 conversational working context，随 boundary 失效；kanban Task、
  memory、grants、`WakeupRegistration` 不受 boundary 影响，各按自己的规则活或死。
- 记忆 `write_scope()` 规则不变：home session 没有 correspondent，写 `Global`；这正是它该有的语义。

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

- **5.1 事件词汇** —— **已完成**：三个事件加上 `Wakeup` / `EventFilter` / `WakeupCause`
  进 `SessionEventKind`（都是 required，老版本读不了新日志，按第一批的 fail-closed 规则）。
  「等待」不是新的一列，是 `RunStatus::Suspended`：turn 停下等东西时既不在跑（重启不该当成崩溃残留
  去 reconcile），也没有结束（没有结论，不是 episode）。`is_terminal()` 一个判据同时管住
  `unlearned`、`episode::assemble` 和 reconcile 三处，`recoverable` 在挂起时折成 false——
  它的回来是被安排好的，手动 resume 会把同一段活干两遍。`wakeup/fired` 把它带回 Running：
  醒了之后再崩，就又是普通的 interrupted。
  验证：`a_suspended_turn_is_waiting_rather_than_interrupted`（suspended 前后各切一刀，
  判定分别是 interrupted / waiting）、`a_fired_wakeup_takes_the_turn_out_of_waiting`、
  `reconciliation_leaves_a_suspended_turn_alone`。
- **5.2 唤醒登记** —— **已完成**：`wakeup_records` 进 `komo.db`（ADR 0004 合库后就是同一个库，
  Q3 那个「放哪」的问题自然消失了；表用 `ensure_table` 建，DDL 与 `push_schema` 逐字节对拍——
  索引名第一次就猜错了，那条测试当场抓住）。`Wakeup` 变体在行上摊平：`kind` 判别，
  载荷列各归各位，读不出来的载荷退化成 `UserReply`（会过期、会回来说没人答）而不是丢行。
  `take` 就是那个认领：行已经没了就答 `false`，所以两个 sweep、或者 sweep 和刚到的 `/approve`
  抢同一条登记，只有一个能 fire。`take_for_turn` 让一个 turn 手上所有等待一起退休——
  被审批唤醒的 turn 不该再被盯着同一件事的定时器唤醒第二次。
  `CronJobSweep` 同一个 tick 顺带扫登记（`WakeupWiring` 三件套：登记、日志、dispatch，
  少一件就不是「功能不全」而是「行为错误」）。顺序是**先认领、再核对日志**：
  日志说这个 turn 已经不在等了，就丢弃并 warn，因为 fire 它等于把续跑干过的活再干一遍；
  读不出日志一律判「不在等」——凭猜去唤醒正是这条检查要防的。
  反向核对做在 gateway 启动：`reregister_suspended_turns` 扫最近
  `SUSPEND_RECHECK_SESSIONS` 个 session，日志说挂起、却没人盯着的 turn 把等待补回来，
  等待本身从它自己的 `turn/suspended` 事件读（这就是那个事件带 `wakeup` 和 `expires_at` 的原因）。
  **grants 补不回来**——它只存在登记里，所以补登记醒来的无人值守 turn 能问、不能动，
  这是这个取舍安全的那一端。
  验证：`a_due_wakeup_fires_once_and_then_is_gone`（第二次 tick 什么都不唤醒）、
  `a_wakeup_for_a_turn_that_already_resumed_is_dropped`、`a_wait_that_ran_out_wakes_as_expired`、
  `a_wakeup_that_starts_a_fresh_turn_needs_no_suspended_turn`、
  `a_suspended_turn_nothing_is_watching_is_re_registered`（幂等）、
  `a_running_or_finished_turn_is_not_re_registered`，加 store 侧四条（五种变体往返、
  无 turn 的登记、认领两次成功一次、按 turn 一起退休）。
  **dispatch 还没有实现者**：`wakeups: None` 挂在 sweep 上，等 5.3 把挂起路径接上来——
  现在还没有任何东西会写登记，所以生产行为一字未变。
- **5.3 审批改造** —— **机制半已完成，答案半还没接**。
  已完成：`Decision::Suspend`（不是拒绝，是「答案还没到」，只存在于审批器↔gate 之间，
  tool 永远看不到它——顺手把三个 gated tool 的 `match Decision` 改成读 `is_allowed()` +
  `feedback()`）；gate 记下等待，executor **不结算**那次调用（无 step、无
  `tool/call-settled`——停下来等的调用没有发生），loop 以 `Suspended` 结束 turn，
  runtime 写 `turn/suspended` + 登记（带上 job grants）。
  留在日志里的是 `approval/requested` 没有 `resolved`，正是恢复已有的「问过、没跑」判据，
  所以答案到达后重新派发是这次调用的第一次也是唯一一次执行；`rebuild_from_events`
  因此对**卡在 gate 上的调用无条件重放**，不看幂等性——否则一个为审批停下的 `shell`
  回来会告诉模型「可能执行了也可能没有」，而这正是那道 barrier 要排除的。
  挂起的 turn **不写 assistant 消息**：它没有回答，而 surface 必须仍以用户消息结尾，
  续跑才是续跑。retention 的 floor 从 `recoverable` 放宽到 `!is_terminal()`。
  gate 问之前先读日志，按 `attempt_chain` 整条链找（答案记在**问的那个 turn** 上，
  现在问的是它的续跑），所以没人会被要求批准同一件事两次。
  `TurnWaker` 是另一侧：写 `wakeup/fired`、退休这个 turn 手上其他所有等待、抢 session slot、
  续跑；spawn 出去所以不占 sweep 的 tick。`attempt_chain` 从 llm.rs 搬进
  `domain::session_event`，rebuild 和 gate 共用一份。
  验证：`a_turn_waiting_on_an_approval_suspends_rather_than_failing`、
  `a_gated_call_honours_the_answer_already_in_the_log`、
  `an_approval_answered_after_a_restart_resumes_the_turn_and_runs_the_call`（**新进程**
  接手挂起的 turn，审批器一次都没被问，续跑跑了那个调用，挂起的那次尝试始终没有 step）、
  `waking_a_turn_records_the_cause_and_retires_its_other_waits`。
  **答复侧也已完成**：`/approve [wk-id] [session|always]` 和 `/deny [wk-id] [理由]`
  除了原来的内存路径，还会把答案**写进日志**（`approval/resolved`，durable 之后才继续）
  并唤醒那个 turn——挂起的 turn 不在这个进程里等，问它的那个进程可能已经重启了。
  id 用 `wk-` 前缀识别，所以 `/deny 太危险了` 还是理由、`/deny wk-0199 太危险了`
  是「答另一个 session 的那个等待」（routine 的审批在 home chat 里答，就是这条路）；
  `/approve the budget` 仍然是普通消息——只有认得的参数才当命令，否则会批准一件没人问过的事。
  重启后内存里的 prompt 没了、风险等级也就无从得知，这时 `session`/`always` 一律**收窄成
  只此一次**：放宽是唯一收不回来的方向。
  续跑逻辑收在 `GatewayDispatcher::continue_turn` 一处（`TurnWaker` 变成薄适配器），
  因为 sweep 的唤醒和到达的 `/approve` 要做的是同一件事，两份实现就是两次忘记写
  `wakeup/fired` 的机会。
  **过期路径**：`cause: expired` 的唤醒先写 `approval/expired`（call_index 从当初的
  `approval/requested` 读回来，而不是猜 0），gate 把它读成拒绝——再问一遍会把 turn
  永远停在同一个问题上。
  验证：`an_approval_that_expired_comes_back_as_a_refusal`（续跑收到「没等到答案」、
  调用没执行）、`an_expired_wait_records_the_expiry_before_continuing`、
  `an_approval_command_can_name_the_wait_it_answers`。
  **`ChatApprover` 已翻转**：提示发出去之后返回 `Suspend`，不再等 oneshot，
  `APPROVAL_TIMEOUT` 那 5 分钟随之删除——「多久没人答」现在是**等待自己的寿命**
  （`default_expiry_secs`，一天），不是某个进程坐在那里等的超时。
  `ApprovalState` 的 `pending` 退化成**提示缓存**（GUI 的审批弹窗轮询它），
  重启会丢——本来整个审批都会丢——而答案本身是 durable 的。
  `/approve session` 的 scope key 从当初的 `approval/requested` 读回来记住，
  因为内存里那份提示在 turn 挂起时就没了。
  **moved-on**：挂起期间同 session 的普通消息即放弃这次审批——消息以 `Injected`
  追加到**那个挂起的 turn**（surface fold 把它并进那条用户消息，交替不破、续跑原地重放），
  审批以「用户改说了这个」结算为拒绝，然后续跑。不是「拒绝 + 另起一个 turn」：
  那样模型会在不知道自己请求的动作已被放弃的情况下回答新消息，用户也会看到两个 turn。
  GUI 的审批弹窗和聊天的 `/approve` 现在走同一个入口
  （`GatewayDispatcher::answer_approval`），两半答案不会各走各的。
  api 的同步与流式两条路都认 `Suspended`：返回「等待批准」而不是 500——turn 没结束，
  答复到了会继续，回复落在 transcript 里，而调用方本来就是从那里读。
  验证：`saying_something_else_takes_the_place_of_a_pending_approval`、
  `a_noted_prompt_is_visible_until_it_is_answered`、
  `a_dangerous_prompt_narrows_a_widening_answer`（`Risk::Dangerous` 仍只批一次）。
  还没接：TUI approver 仍在进程内等（它本来就守着自己的 turn，不需要跨进程恢复）。
- **5.4 无人值守审批** —— **已完成**：cron runtime 的内层 approver 换成 `UnattendedSuspend`
  （`komo-agent` 的 `unattended`）：`Risk::Normal` 答 `Suspend`，`Risk::Dangerous` 仍拒绝——
  无人值守永不放行危险动作，事后 `/approve` 也不行。提示由 `CronJobSweep` 发而不是 approver 发，
  因为 `wk-<id>` 要等登记写完才存在：sweep 拿到 `Suspended` 后从日志读 `turn/suspended.summary`、
  从登记读 id，走已有 notifier 投递「回复 `/approve <id>` / `/deny <id>`」，只给 Once——
  `session`/`always` 是放宽，无人值守不给。job 的 `last_status` 多一个取值 `waiting`
  （不是 ok 也不是 failed），`last_run_session` 指向挂起的 turn。briefing 保持 deny：
  它一失败就降级成无工具 compose，简报已经投出去了，挂起只会留一条没人听的续跑。
  顺带补的两处：`continue_turn` 从 session 记录读回 `origin`、从登记读回 grants——
  原来续跑用 detached context，routine 醒来按普通对话评估权限（更宽）且丢掉自己的 grants；
  `run_projection` 沿 `resumed_from` 链继承 `approval/resolved`，否则答复记在问的那个 turn、
  动作跑在续跑里，§8 判据 2 的 `waited_ms ≈ 5h` 永远是空的。
  **遗留**：续跑跑在 dispatcher 的主 runtime 上，不是 cron runtime——工具集更大、经过 memory
  enricher；权限上 fail-closed（主 runtime 内层是 `ChatApprover`，detached 非交互即拒绝），
  但不对。要修需要 dispatcher 按 origin 路由 runtime，独立一件事。
  验证：`a_routine_stops_for_an_ungranted_action_and_acts_once_it_is_approved`（真 sweep →
  真 runtime → 挂起 → notifier 收到 `wk-` 提示 → 拨快 5h → `answer_approval` → 续跑执行、
  step `approved_by = human`、`waited_ms = 18_000_000`、登记退休、approver 没再被问）、
  `a_refused_routine_comes_back_and_does_not_act`、`a_routine_never_waits_for_a_dangerous_one`、
  `a_call_re_dispatched_after_a_wait_carries_the_answer_that_licensed_it`。
- **5.5 `awaiting` 投影** —— **已完成**：`Awaiting {turn_id, kind, since, summary, expires_at}`
  从日志 fold（`komo-core` 的 `domain::awaiting`），`turn/suspended` 置位，`wakeup/fired`、
  接手它的 `turn/started{resumed_from}`、以及那个 turn 的终止事件三者任一清除。fold 带
  **prior**（`project_awaiting(prior, events)`）：折前缀再折其余等于折全部，所以 turn 自己的
  那段 tail 就够——否则挂起期间另一个 turn 跑完，它的 tail 里没有挂起事件，会把别人的等待抹掉。
  落在 `session_records.awaiting`（`ensure_columns` 加的可空列，JSON，空串 = 不在等），
  写入点就是 run ledger 已经读过日志的那两处（`open_in_ledger` / `settle_turn`），不新开一次
  全量读；列是**缓存不是权威**，`Db::rebuild_projections`（原 `rebuild_run_projection`，
  现在一次 fold 同时喂两个投影）以 `prior = None` 重折全日志。
  显示：`komo session list` 一列、TUI 状态栏（resume 时读一次，发消息即清——说别的就是
  moved-on）、api 的 `SessionSummary` 带上该字段（apps 未改）。
  验证：`a_suspended_turn_is_a_session_that_is_waiting`、`an_answered_approval_ends_the_wait`、
  `a_wait_that_ran_out_ends_the_wait`、`another_turn_running_leaves_the_wait_alone`、
  `the_continuation_that_picks_the_turn_up_ends_the_wait`、
  `the_wait_a_session_is_stopped_in_rebuilds_from_the_log`（清空列后重建 = fold）、
  `a_suspended_turn_shows_up_as_the_session_waiting`（写入点确实接上了）。
- **5.6 Home conversation（D6）**：principal → conversation 两步解析进 `GatewayDispatcher`；home
  session id 记 settings；TUI 默认进入 home session；`/new` 改为写 `conversation/boundary`，不再
  `rotate`；`Session.workspace` 的 creation-locked 语义放弃，workspace 只从 `SessionContext` 读；
  `todo` 随 boundary 失效。**只改 identity / routing**，不碰事件日志、恢复、审批、storage；system
  的 context 层保持 per process。**前置：turn-durability 第三批 compaction 已落地**，否则同时调小
  `max_history_bytes`。
  验证：Telegram DM 与 TUI 各发一条，两条落在同一 session 且 seq 连续；飞书群消息落在另一条 session；
  TUI 关掉重开看到同一段对话；`/new` 之后模型历史从边界开始，但挂起中的审批仍可被 `/approve` 唤醒、
  kanban Task 与 memory 原样；在 TUI 挂起、从 Telegram `/approve` 的 turn，回复回到 Telegram。

### 第二批 · `wait` 与后台任务

- **5.7 `wait` 工具** —— **已完成**：三种参数 → 三种 Wakeup（`until` 走 reminder 的
  `parse_after` 与 cron 的 `@at` 解析，所以「已经过去的时间」和 DST 空洞在这里也被拒）；
  `WAIT_BUDGET_PER_TURN = 4`；`Scope::ALL`，无人值守 turn 也能调。
  **工具触发挂起的通道就是审批那条**：`ToolContext::wait_for` 填的是审批 gate 填的同一个
  `PendingSuspension`，所以 executor（不结算）、loop（`Suspended` 收尾）、runtime
  （写 `turn/suspended` + 登记）一行都不用改。不同的只有回来的路：
  `turn/suspended` 多带一个 **`call_id`**（停下来等的那次调用），
  `rebuild_from_events` 因此把它并进 `gated` 集合无条件重放；runtime 在续跑打开时把整条
  `attempt_chain` 的等待 fold 到 `RunContext` 上（`fold_turn_waits`），于是
  `ctx.resumed_wait()` 交给该调用它自己的那次唤醒、`ctx.waits_taken()` 是**从日志数**的
  每 turn 预算——内存里的计数会被它正在计的那次挂起清掉。
  `for_task` / `for_event` 只做登记形状：5.9 / 5.12 还不存在，今天没有东西 fire 它们，
  `Event` 靠 30 天过期回来说「没等到」，`TaskDone` 按 §3.2 不设第二个时钟。
  验证：`a_wait_stops_the_turn_and_says_when_to_come_back`（`turn/suspended{at, call_id}`、
  登记 `expires_at` 为 None、无 step）、
  `a_timer_that_came_due_after_a_restart_continues_the_turn`（**新 runtime 实例**接手，
  `fire_due_wakeups` 到点 fire，续跑里那次调用只有一个 step 且返回「时间到了」，
  登记已退休）、`a_spent_budget_reports_instead_of_stopping_the_turn`。
- **5.8 `ask_user` 持久化** —— **已完成**：`turn/suspended{UserReply}` + 登记（7d），
  内存里的 `ClarifyState`（oneshot、`CLARIFY_TIMEOUT`、`CLARIFY_BOUND`、per-turn 计数）
  整个删掉，不留兼容层。「下一条用户消息即答案」变成
  `GatewayDispatcher::answer_question`——聊天里的普通消息、GUI 的 inline reply、api 的
  cancel 走同一个入口，答案落在 `wakeup/fired{reply, payload}` 上，工具重放时读它；
  `/skip` 以 `moved-on` + 空 payload 显式跳过，过期以 `expired` 回来，两者都返回原来的降级文案。
  TUI 本地模式没有 dispatcher，但它本来就自己驱动 turn：日志那一半共用
  `interaction::record_wake`，续跑用 `resume_interrupted`，所以「问 → 答」在没有 gateway 的
  `komo chat` 里照常工作（没有 sweep，所以本地模式等不到 `wait 2h`——那要等 gateway 起来）。
  验证：`a_question_answered_after_a_restart_comes_back_as_the_answer`、
  `a_question_nobody_answered_comes_back_saying_so`（日志有 `wakeup/fired{expired}`）。
- **5.9 后台 shell / delegate**：`task/spawned|settled`，结算唤醒或开新 turn；重启后 running 判 uncertain。
  验证：turn 结束后任务完成，session 收到带结果的新 turn；父 turn 挂起在 TaskDone 时精确恢复。
- **5.10 kanban Task ↔ Wakeup**：`waiting_on_peer` / `wakeup_id` 列（kanban.db 加列，`ensure_columns`）；
  进入/离开 `Waiting` 登记/撤销；`FromPeer` 过滤器；`wait { for_task }`。
  验证：Task 等某 feishu peer，该 peer 来消息后 `task.source` 上出现一个带消息内容的新 turn，
  Task 状态未被自动改动；Task 完成后同一 peer 再来消息不再触发；纯文字 `waiting_on` 在 list 里标「不可唤醒」。

### 第三批 · Trigger 泛化

- **5.11 `Trigger` 枚举 + `runs` 历史**：替换 `schedule` 与 `last_*`；cron.db 重建；`komo cron`
  CLI/`cron` 工具/api 三个入口走同一 `cron_actions`。验证：现有 cron 测试全绿；`Any` 任一命中只跑一次。
- **5.12 Webhook**：`/api/hooks/{name}`。验证：无 key 401；命中登记唤醒；命中 routine 开 turn 且 `event` 记录 body 摘要。
- **5.13 Feishu match**：`TriggerMatcher`（与 5.10 的 `FromPeer` 共用）；命中不进聊天路径。
  验证：非 `allow_from` 的群成员 reaction 能触发 routine 但 turn 的 grants 是 routine 的，不是发送者的。
- **5.14 FileChanged**：`notify` + 防抖。验证：批量写 50 个文件只触发一次。

### 第四批 · 收口

- **5.15 per-routine 通知策略**：`notify: always | on_error | never`（Grok 每 agent 有
  `notificationsEnabled` / `notifyOnUpdatesEnabled`）。「有异常才告诉我」在这里。
- **5.16 per-task artifacts**：`~/.komo/artifacts/<session>/`，进 workspace 的可写 roots。
- **5.17 文档**：AGENTS.md 模块地图更新（cron → routine + wakeup；approval 一节改写；task 一节加唤醒）。

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
- 不做 Task Router：不让一条消息被"挂到某个 Task"从而决定它的上下文。指代（"刚才那个""昨天那个
  方案"）由窗口 + compaction + 检索叠加解决，不由路由排他解决。
- 不让 Task 持有 goal / plan / findings 这类模型维护的状态文档；那是日志之外的第二份权威。
- 不把 `/new` 做成清理按钮：它不删 todo 之外的任何东西，不结束挂起的 turn，不撤销 grants。
- 不为 home session 引入多写者或并行 turn；并行是 background task 和 routine 的事。

---

## 7. 决定记录与待拍板

已决（2026-09-02）：

- **Q1 kanban Task 与 Wakeup 打通** → 打通。Task 的 `Waiting` 登记一条 `FromPeer` 唤醒，详见 §3.7、5.10。
- **Q2 审批挂起期间用户在同一 session 说话** → 视为放弃审批：`Deny{feedback: 那条消息}` 结算并恢复
  turn，见 §4.1。Grok widget 的 `dismissOnMoveOn` 与现有 `Answer::Deny(feedback)` 都是这个形状。

待拍板：

- **Q3 登记存 cron.db 还是 state.db**。本 PRD 选 cron.db（durable，与 routine 同 sweep）。
  反对意见：登记可由日志重建，属于 disposable。反驳：重建要扫所有 session 的尾部，
  启动时做不起；durable + 启动时只核对最近 N 个更便宜。
- **Q4 过期时长默认值**（§3.2）：Approval 24h、UserReply 7d、Task 等待 `due_at` 否则 30d、Event 30d、At 无。

---

## 8. 完成判据

0. 操作者从 Telegram DM、飞书 DM、TUI 三个入口各说一句，三句在同一条 session 日志里 seq 连续；
   一条飞书群消息落在另一条 session。TUI 关掉重开，看到的是同一段对话。`/new` 之后模型看不到
   边界前的对话，但 `komo run inspect` 仍能读出边界前的全部 turn，挂起中的审批仍能被唤醒。
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
