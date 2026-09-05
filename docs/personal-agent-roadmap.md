# 个人 Agent 能力缺口与路线图

本文档基于当前 komo 的实现状态，整理它从"能聊天和调用工具"继续走向"能长期辅助个人事务"还差什么。重点不是复述已落地能力，而是把下一阶段的产品/工程缺口排清楚。

## 现状

被动应答、常驻入口、任务/记忆/审计这几条主链路已经成型：

- `AgentRuntime` 负责会话生命周期、消息持久化、run ledger 开关和回复写回。
- LLM 走 komo 自己的 provider 层（`crates/komo-provider`，Responses / Messages 两种 wire，无第三方 LLM crate），一轮一次 completion；工具调用循环在 `run_agent_loop`，自带瞬时错误重试（幂等区分）、`Uncertain` 结算、取消、挂起和 `max_turns` 轮次预算。
- gateway 常驻一个 loopback api channel（OpenAI 兼容 + `/api/*` 只读视图），CLI 在 gateway 持有 db 锁时经它路由，不再直接开 db。
- 已注册工具包括 `time`、`shell`、`read`/`write`/`edit`/`apply_patch`、`grep`/`glob`、`web_fetch`/`web_search`、`session`、`reminder`、`cron`、`memory`、`delegate`、`skill`、`task`、`todo`、`ask_user`/`wait`、`run_code`、`logs`、`wiki_*`、`homeassistant`，以及 `[mcp.servers.*]` 挂载的 MCP 工具。
- `reminder` 支持一次性提醒和 5 字段 cron 周期提醒；`cron` 是 routine：command / agent 两种 job，由 cron slot、`@at`、webhook、飞书消息或 reaction、文件变更触发（`docs/bot-runtime.md`）。
- `gateway` 已具备常驻进程、launchd 安装、维护任务调度、chat 交互式审批和 proactive home channel。
- ingress channel 已落地：Feishu、Telegram、WeChat；聊天通道带 pairing / allowlist。Home Assistant 曾做过事件通道，已移除——每条 state_changed 换一次 LLM turn 不划算，改为只保留 `homeassistant` 工具按需拉取。
- 所有表在一个 `~/.komo/komo.db` 里（ADR 0004），disposable / durable 是表的属性；transcript 是 `~/.komo/sessions/` 下追加式 JSONL 文件，run ledger 是它的投影（`docs/turn-durability.md`）。
- reflective reviewer 已能提取 candidate memories 和 commitments；承诺会进入 task inbox。
- memory 已有 L1 pinned profile、L2 governance tool、L3 active recall。
- run ledger 已记录每个 turn 和每次工具调用，CLI 可 `komo run list/inspect/resume/rollback/prune`。
- 已有 operator CLI：`cron`、`session`、`task`、`run`、`memory`、`wiki`、`skills`、`journey`、`pair`、`policy`、`model`、`channel`、`workday`、`logs`、`doctor`、`health`、`dream`、`upgrade`。

总体判断：**komo 是一个简洁的 agent core，能力扩展走 skill / MCP / Python 插件，不往 core 里堆连接器**。入口、任务、记忆和审计已经有骨架；core 的四件收官事（§3 权限策略、§5 memory 质量、§6 run resume、§9 skill governance）都已落地。日历/邮件/笔记这类真实数据源由 skill 接入（用现有通用工具调 API），core 提供的是让 skill 安全好用的地基。

## 1. 真实个人数据：走 skill，不进 core

个人 agent 的价值来自真实上下文，但连接器**不编译进 komo**。日历、邮件、外部任务、Notion/Obsidian、本地文件夹这类数据源，用 skill（指导 agent 拿现有通用工具 `web_fetch`/`shell`/`read` 调 API 的知识文件）、`[mcp.servers.*]` 挂载的 MCP 服务，或 `~/.komo/plugins/*.py` 里的 Python 插件接入。core 不新增 per-service 工具，保持简洁。

这个取舍对 core 的真实要求，恰好就是本路线图的四个重点：

- **权限策略（§3）**：skill 驱动的 API 调用要靠可配置的域名/命令授权自动放行，而不是每次人工 `/approve`。
- **skill governance（§9）**：连接器逻辑活在 skill 里，skill 的审查、保护、来源追溯就是连接器的质量线。
- 凭证仍集中在 `~/.komo/.env`，skill 文本里不放 secrets。
- 原则不变：先只读后写入，写入必须过权限策略并进 run ledger。

## 2. 主动摘要升级

每日 briefing 已落地：`BriefingSweep` 读取 open tasks + 近期记忆，用 aux LLM 组织成摘要，经 `HomeNotifier` 投递；它是 opt-in 的，只有设置 `briefing_schedule` 才运行，也可通过 `briefing_workdays_only` 限制为中国工作日。

当前缺口：

- briefing 跑在只读工具集 + deny-all 无人值守审批器上，接不了需要副作用的外部数据。要包含日历/邮件，走的路是 `unattended = true` 的 policy 规则或一个带 `grants` 的 agent routine 去拉数据，而不是往 core 加连接器。
- 每周摘要尚未实现，但框架已经够用：换 prompt、换 cron、读取完成任务和近期 run/memory 即可。

每日摘要最终应包含：

- 今天日程
- 到期任务
- 未关闭承诺
- 最近新增重要记忆
- 待确认的 memory / task candidate
- 邮件或消息里的待回复事项

每周摘要建议包含：

- 本周完成事项
- 卡住的项目
- 被多次推迟的任务
- 新形成的偏好或工作流规则
- 建议清理的过期/低置信记忆

## 3. 权限策略产品化

**已落地**（设计与缺口分析见 `.scratch/permission-policy/PRD.md`）：

- 独立 policy 层：`domain/policy.rs` 纯规则引擎 + `agent/policy_approver.rs` 装饰器（包在 `CliApprover`/`ChatApprover` 外层），config.toml `[policy]` + `[[policy.rule]]` 配置。
- 规则维度：category（shell 命令前缀 / file 目录+读写 / network 域名 / homeassistant 服务）、matcher、channel scope、`include_dangerous`、`default_normal` 兜底。deny 永远压过 allow；无 session（sweep/aux）时 Allow 不无人值守生效。
- 读操作 deny-only：`web_fetch` 和 `read`/`grep`/`glob` 以 `Risk::Safe` 过策略，deny 规则可封域名/路径（exfiltration 防线），未命中不打扰、不升级。
- 操作面：`komo policy list` / `komo policy check`（dry-run 并指出命中规则）、doctor `policy:` 段。
- 分层不变式：policy 在各工具 hardline 地板之上，只能收紧不能放松；策略判定进 tracing 日志，deny 结果以工具错误进 run ledger。

后续也已落地：`unattended = true` 规则和 agent routine 自带的 `grants`（创建 job 的同一条审批里批准，随 job 撤销）是无人值守的两条窄通道；`[policy] mode = "auto"` 让 aux 审查器先判"操作者最新消息是否明显授权"，只能放行或交人（ADR 0003）；每条放行都记下是哪一级放的（`komo run inspect` 的 `allowed by …`）。

剩余缺口：

- 审批交互里提示"可以把这个动作加进 policy"（从临时审批到规则的引导路径）。

## 4. 任务与承诺模型

这块已经基本落地，但仍有少量产品缺口。

已实现：

- durable `Task` 单表：`inbox` / `todo` / `waiting` / `done` / `cancelled`。
- `waiting_on` 覆盖个人语境里的承诺/等待关系；`waiting_on_peer` 写了地址的 Task 会登记一条 wakeup，对方来消息就在来源 session 上开一个 turn（`docs/bot-runtime.md` D4 / §3.7）。
- `board` 字符串覆盖轻量项目分组，不建 Project 实体。
- `task` tool 支持 capture/list/update/complete。
- `TaskSweep` 每分钟投递到期任务，at-most-once。
- reflective reviewer 会从会话中提取 commitments，自动进入 inbox，用户再 triage。
- session-scoped `todo` tool 已落地，用于当前会话的工作焦点列表。

刻意不做：

- task dependency graph
- owner / worker claim
- multi-agent swarm 调度字段

原因仍然成立：没有 worker 群，这些结构没有消费者。"单 turn 助理"这个前提已经变了——turn 可以挂起、被唤醒、开后台任务——但结论不变：执行不需要 Task，Task 的等待由执行来兑现（`docs/bot-runtime.md` §6 明确不建执行型 Task、不做 Task Router）。

剩余缺口：

- 更好的 task triage 操作体验，例如批量 promote/reject/board 分类。
- task 来源投递更细化：从哪个 channel 来，最好回哪个 channel。
- 与外部任务系统同步。

## 5. 长期记忆召回与治理

memory 三层已经落地：

- L1 pinned profile：手动 pin、active、confirmed/user_written、scope 过滤后注入。
- L2 memory tool/governance：save/search/list/update/promote/reject/archive/pin。
- L3 active recall：基于用户当前输入做 lexical ∪ semantic 混合召回（`RecallQuery`；embedding 来自 `[memory] embedding_model`，无模型则退化为 lexical），注入 top memories，并记录 `last_used_at`。

治理也已落地：dreaming（`DreamSweep`，默认每晚 3 点，`komo dream [--apply]` 可预览/手动跑）按**证据**决定 candidate 的去向——晋升只看 `support_count` / `contradiction_count` / 显式确认，归档只看召回冷热；两条轴刻意分开，因为按召回晋升会让错的记忆靠被取出来自证。只动 candidate，pin 仍是手动专属。

质量升级（`.scratch/memory-quality/PRD.md`）也已落地：

- dreaming 的 query-diversity（OpenClaw 的 `minUniqueQueries`）：`mark_used` 记录 query 词面指纹到 `recall_query_hashes`（原地加列迁移），promote 判据加 `unique_queries >= 2`——同一问题连问 N 次不再能把 candidate 泵成 active。
- candidate 批量 triage：memory 写命令经 gateway 的 loopback POST 路由（不再被 db 锁挡住），promote/reject 变参，`komo memory triage` 交互式清堆（最老优先）。
- aux recall agent（宽取窄注）：recall 取 15 个候选，超过 5 个注入名额时由 aux 模型筛选相关性并可压缩为单行；严格 JSON + id 校验 + 4s 超时回落，`mark_used` 只记真正注入的条目——dreaming 消费的是"相关性过滤后"的信号。
- reviewer 防自噬规则继续保持：不能从注入块再提取记忆。

后续也已落地：`MemoryConsolidator`（新观察先找相关记忆，分类为 same / supports / contradicts / supersedes，再记证据、标 contested 或 supersede）、`BeliefState` 与 `MemoryProvenance`（工具读到的"事实"只能自成 candidate，不能给用户说过的话作证）、跨语言 embedding 召回、episodic search（`session` 工具对 komo 自己全部 transcript 的混合检索）。

剩余缺口：

- 联想式召回：现有打分只有词面和语义两个臂，entity overlap / procedural match 尚未进入 `RecallQuery`。
- episodic → procedural 的中间态：skill candidate 目前直接由 reviewer 提出，没有"多次重复的做法先成 procedural memory 再升 skill"这一步。

治理原则不变：自动提取只能进 candidate，不能自动 pin；影响长期行为的内容必须可追溯、可拒绝、可归档。

## 6. 工作流执行记录与恢复

run ledger 的记录层已经落地：

- 一次 turn = 一条 `Run`。
- 每次工具调用 = 一条 `RunStep`。
- 埋点在 `ToolExecutor::execute_round`，覆盖 LLM function-calling 路径；Run / RunStep 全部是 session 事件日志的投影（`docs/turn-durability.md`）。
- step args 可由工具自行 redaction；`shell` 会擦掉疑似密钥，`write` 会丢掉写入正文。
- `komo run list [--limit N]` / `komo run inspect <id>` 可查看。

剪枝已有 operator 动作：`komo run prune --before <date>|--keep <N>`。

resume 也已落地（`.scratch/run-resume/PRD.md`）：ledger 是审计账不是 checkpoint，所以 `komo run resume [<id>]` 不做断点回放，而是把原始输入 + 已完成步骤摘要组装成 priming 输入，在原 session 重派一轮新 turn，由模型判断哪些副作用已生效；新副作用照常走审批。`recoverable` 从日志 fold 出来（无终结事件且未被认领），认领是续跑自己的 `turn/started{resumed_from}`，`run list` 以 `⟲` 标记。不做自动 resume——无人在场重放半完成副作用不可接受。

文件改动可撤销：`write`/`edit`/`apply_patch` 在同一把路径锁内保存 run 首次触碰前的 pre-image（`~/.komo/checkpoints/`，7 天），`komo run rollback <id>` 放回；内容已被后续改动覆盖的文件跳过并点名。只给 operator，不给模型。

尚未做：

- 基于 run ledger 的权限审计视图（`approved_by` 已按 step 记录，缺聚合视图）。
- 中断的主动可见性（startup 发现 n>0 时经 HomeNotifier 提示）——后置，等核心链路用出真实需求。

## 7. 更强的 planner / orchestrator

架构前提已经完成：工具循环在 `run_agent_loop`，provider 层只承担单次 completion（`LlmClient::begin_turn` / `TurnDriver` 是扩展的 seam）。控制点加在循环的轮与轮之间。

已落地的控制点：

- `todo`：模型可维护当前会话的步骤列表，最多一个 `in_progress`。
- 工具失败重试（`ToolExecutor`）：连接级错误任何工具都重试，歧义错误（超时/5xx/429）只重试声明 `idempotent` 的工具，分类器放弃重试的歧义失败结算为 `Uncertain` 交给模型判断，终结错误和 panic 不重试。
- 预算：`max_turns` 轮次预算（超限强制收尾）+ 软工具调用预算 + 结果字节上限（超限全文落盘、head+tail 预览）。
- 澄清问题：`ask_user` 哨兵工具挂起 turn，答案经 wakeup 回到同一次调用（`/skip` 让它按假设继续）。
- 取消 / 暂停 / 恢复：TUI Esc 与 `POST /api/interactions/{session}/cancel` 取消；`wait` 挂起等事件、时间或后台任务；`shell {background}` / `delegate {detach}` 把工作放到进程自己的任务里。
- 中途产物：`~/.komo/artifacts/<session>/` 是 turn 的可写产物目录，从不清扫。
- 长对话：`compaction` 在窗口裁掉最老消息前把它们换成一条 summary（追加，不改写）。

仍缺：

- 时间 / token / 风险等级维度的预算。
- 跨 session 的 Goal 分组：给 run、task、memory 打同一个标签用于过滤和召回加权——只做标签，不做路由，不持有状态文档（`docs/bot-runtime.md` §6）。

## 8. 本地快捷入口

api channel 的 loopback HTTP 已经覆盖这个需求的大半：脚本、Raycast、Automator、快捷指令都可以直接 `POST /v1/chat/completions`（`~/.komo/gateway.json` 里有地址和 key）。不再计划新的本地 channel；剪贴板 / share sheet / 浏览器页入口如果要做，是调 api channel 的外围小工具，不进 core。

## 9. 可观察性与 inspect

已有 operator 视图：

- `komo cron list`
- `komo session list/clean`
- `komo task list`
- `komo run list/inspect`
- `komo memory list/search/promote/reject/pin`
- `komo pair list/approve/revoke`
- `komo logs`
- `komo workday`
- `komo doctor`：config health + gateway 存活 + channel/凭证状态 + home channel + 最近失败 run（gateway 状态聚合与 config health 都归它）
- `komo memory report`：memory quality report（状态/置信分布、待 triage 的堆）
- `komo run prune`
- `komo dream`：dreaming 预览/手动执行

**skill governance 已落地**（`.scratch/skill-governance/PRD.md`）。前提是先收敛了存储：过去 reviewer 写 db、运行时读文件目录，两套互不可见；现在**文件系统是唯一事实源**——`~/.komo/skills/` 是 komo 自有的 durable skill 主目录。治理四件套：

- triage：reviewer 提取只落 `.candidates/`（覆盖前滚 `.history/`），`komo skills promote|reject` 由 operator 决定，从不直接生效——与 memory candidate 同一梯子。
- 保护：`protected` = 只有 operator 能改；reviewer 连 candidate 提案都不生成（保护挡在提案生成处，防"一键 promote 覆盖"）；agent 无 skill 写路径。
- 启停：`disabled` 的 skill 留在盘上可 inspect，但不进模型 catalog，`skill view` 返回明确的"已停用"。
- inspect 与审计：`komo skills inspect` 显示全文/来源/路径/历史；`komo skills audit` 从 run ledger 的 `skill view` steps **派生**调用记录（不存任何 usage 计数字段），按 turn 的 `Run.outcome` 归因。
- 退役与恢复：`komo skills archive|restore`，活跃 skill 从不被删除；reviewer 改已有 skill 前必须先读到真实正文（`grounded_rewrite`），读不到就丢弃提案。

所有治理命令都是纯文件操作，gateway 持锁时照常可用。**live registry 热加载已落地**：`SkillRegistry` 每次查询都重扫 skill 目录，install/promote/enable/disable 在 `skill` 工具的下一次 `list`/`view` 就可见，无需重启 gateway。仍冻结在启动快照的只有系统提示里那份 capped skills catalog teaser（活在 cache-stable 提示层，为保 prompt 缓存刻意不热加载；它只是个有界提示，指引模型调 `skill` list 拿完整实时集）。

candidate 过期也已落地：没人裁决的提案在 `SKILL_CANDIDATE_EXPIRY_DAYS` 后由 dream sweep 移入 `.expired/`，`restore` 只能把它放回 candidate，不能直接上线。

目标是让用户随时看懂 komo 当前知道什么、正在做什么、为什么这样做。

## 10. 文档与代码同步

文档落后会直接误导后续 agent。仓库里的长文档各有一个职责：

- README：面向用户的当前能力、安装、配置和运行方式。
- AGENTS.md：面向 coding agent 的真实架构、命令和工程约束；每个模块的"为什么"就写在这里。
- `CONTEXT.md` + `docs/adr/`：术语表与架构决定记录。
- `docs/personal-agent-roadmap.md`：产品能力缺口和推荐实现顺序。
- `docs/bot-runtime.md`、`docs/turn-durability.md`、`docs/episode-learning-framework.md`：三份现行设计稿，各自带"现状核对 / 决定 / 明确不做 / 完成判据"，实现落地后在文中标记而不是另起一份。

从未落地的设计稿和已实施完的执行计划一律删除（2026-09 删掉了 riko 方案、Cordis 方案、架构深化计划和通用 memory 论述），避免继续引用过期事实。以后新增长文档前先判断是否应该并入 README、AGENTS、ADR 或本 roadmap；贴进来的外部讨论稿不算设计稿，要先按上面的格式重写。

## 推荐实现顺序

core 只做四件事，按依赖顺序：

1. ~~**权限策略配置化（§3）**~~ ✅ 已落地（`.scratch/permission-policy/PRD.md`）：独立 policy 层 + deny-only 读操作 + `komo policy` 操作面；剩 `unattended` 窄通道，挂起到 briefing-via-skill 出现。
2. ~~**memory 质量（§5）**~~ ✅ 已落地（`.scratch/memory-quality/PRD.md`）：aux recall agent、candidate 批量 triage、dreaming query-diversity；embedding/hybrid search 仍然后置。
3. ~~**run resume（§6）**~~ ✅ 已落地（`.scratch/run-resume/PRD.md`）：`komo run resume` 重派中断 turn，`recoverable` 由 reconcile 置位、resume 清零。
4. ~~**skill governance（§9）**~~ ✅ 已落地（`.scratch/skill-governance/PRD.md`）：文件系统为唯一事实源，triage/保护/启停/审计齐备。

明确不做进 core 的：日历/邮件/笔记等数据连接器（走 skill / MCP / Python 插件）、本地快捷入口的新 channel（api channel 的 loopback HTTP 已可承接脚本/Raycast）。

已经完成的里程碑：ingress、durable task、commitment extraction、ChatApprover、daily briefing、memory L1/L2/L3 + dreaming、memory 质量与 consolidation、embedding 召回与 episodic search、run ledger + prune + resume + rollback、单库合并（ADR 0004）、自有 provider 层、MCP、Python 插件宿主、wiki 索引、挂起 / 唤醒 / routine 触发器（bot-runtime）、后台任务、compaction、skill governance。

下一阶段的候选主题：

1. 联想式召回与 episodic → procedural 的中间态（§5）。
2. 跨 session 的 Goal 标签（§7）。
3. 权限审计聚合视图与"审批转规则"的引导（§3、§6）。
4. 每周摘要（§2）。
