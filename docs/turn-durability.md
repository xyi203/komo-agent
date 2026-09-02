# Session 权威事件日志：一次 turn 的持久化

范围：session 内发生什么、一次 turn 如何持久化、崩溃后怎么恢复、读模型如何重建、什么时候删除。
不含：provider wire format、policy 梯子、channel 协议、长期 memory 本身的存储。

依据是对 OpenClaw v2 / pi harness / deepseek-harness / codex / grok 五个实现，以及 komo
现有 transcript、turn journal、Run/RunStep 读写路径的逐条核对。§1 是决定，§2 定义数据模型，
§3 说明读写与恢复，§4 是改动清单。

---

## 1. 三条设计决定

### D1 · Session 是一条权威事件日志

**决定**：session 的权威聚合是 `SessionHeader + SessionEvent log`。Header materialize 之后，
任何 session 内已经发生、并会影响后续行为的事实，只能写入这一条按 `seq` 排序的 append-only
日志；transcript、turn journal、Run/RunStep 不再各自保存一套部分重叠的事实。

`SessionEvent` 是“这个 session 中已经发生的一件事”的不可变记录，不是当前状态，也不是
进程内 event bus 的通知。例如，工具开始和工具结算是两条事件，后者不会回头修改前者：

```json
{"v":1,"seq":42,"at":"2026-09-01T10:30:00Z","type":"tool/call-started","data":{"turn_id":"turn-7","call_id":"call-3","call_index":2,"tool":"shell","arguments":{"command":"cargo test"}}}
{"v":1,"seq":43,"at":"2026-09-01T10:30:04Z","type":"tool/call-settled","data":{"turn_id":"turn-7","call_id":"call-3","call_index":2,"outcome":"succeeded","result":"test result: ok"}}
```

三条硬约束：

1. **单一顺序**：同一 session 只有一个递增且连续的 `seq`，所有 ingress 共用同一个写入槽；
   event 写入后不可修改，只能追加表达后续事实的 event。
2. **模型可见即可重建**：任何进入下一次 provider request 的消息、工具结果、context replacement
   都必须能从日志重建；不能存在第二份只给模型看的隐藏历史。
3. **投影不是权威**：state.db 可以保留跨会话索引和读模型；派生行必须能从
   Header + RetentionBase + retained events 重建。`run prune` tombstone 这类 operator control state
   是额外输入，不伪装成可重建派生行；projection 写失败不能反过来改变已经提交的 session 事实。

**边界**：统一的是 session-local facts，不是所有持久化数据。

- `SessionHeader` 在逻辑上单独保存 session id、origin、workspace、created_at 等身份元数据；
  它描述这条日志是谁，不描述日志里发生了什么。物理上它是 `manifest.json` 的不可变字段，
  state.db 中的 session metadata 只是用于列表查询的副本。
- `memory.db`、`kanban.db`、`cron.db`、`permissions.json`、skills 文件仍是各自领域的权威存储。
- 大体积 tool output 和文件 checkpoint 仍使用有界保留期的外部文件；event 只保存模型实际看到的
  有界内容、内容摘要和引用。

原方案用剪枝、跨会话查询和可变字段否决单一日志，这三个问题不要求保留多套 session 权威数据：

- `run prune` 只改变 operator 的 run 查询视图，不影响模型行为，属于 projection control state；
  它继续在 state.db 内用一个事务写 tombstone 并删除投影行，不向 N 个 session 日志追加事件。
- `run list`、`unlearned`、`skills audit`、`memory used` 走 state.db 的跨 session 投影索引。
- `learned`、`recoverable`、`outcome`、`memories` 由后续状态事件表达，再 fold 成当前值。

**物理删除规则**：保留中的 event 不删、不原地改写；event artifact 从第一版起就按序列化字节数
分段。只有在 completed-turn 边界先提交权威 `RetentionBase` 并原子推进 `truncated_before` 后，
才允许整段删除被覆盖的老 segment。tool-output/checkpoint 继续按现有 7 天策略删除；整个 session
被明确删除时再删除剩余 segments、retention base 和 projections。`run prune` 不承担日志字节回收。

---

### D2 · 副作用之前先持久化意图，逐 call 结算

**决定**：事件先进入 session 的内存序列，再由 write-behind 批量落盘；但所有具有语义的边界
必须先 durable flush 成功，后续行为才能发生。本文的 `flush` 明确定义为：把待提交 record 写入
文件并对该文件执行 `fsync` / `sync_data`；首次 materialize、segment roll 和 retention truncate
还必须原子更新 manifest，并 fsync 父目录。只刷用户态缓冲不叫 flush。

1. 发起 provider request 前，flush 本轮已经组装进 request 的前缀事件。
2. `execute_round` 已知整轮全部调用且用 `join_all` 同时派发：按 provider 顺序一次 append 这一轮
   所有 `tool/call-started`，每条携带从 0 开始的 `call_index`，只 durable flush 一次，然后才启动
   整轮工具；不是每 call 一次 fsync。
3. **settle 逐 call**：每个 tool 独立 settle 后立即 append 一条 `tool/call-settled`，不能等整轮
   完成才写一条 `Results`；它沿用 started 的 `call_index`。settled event 的 `seq` 是完成顺序，
   continuation fold 必须按 `call_index` 重排成 provider 顺序，绝不能按 settled seq 组装结果。
   settle 不要求每 call 单独 fsync；下一次 provider request 或 turn end barrier 将已完成结果一起
   durable flush。barrier 前崩溃而尚未落盘的结果保守判为 uncertain。
4. **审批是单独的 durability gate**：`ToolContext::decide()` 在等待前 append + durable flush
   `approval/requested`；审批返回后 append `approval/resolved`。只有 `resolved = allow` durable flush
   成功后才把 Allow 返回给工具代码，使后续副作用有资格开始；deny 不执行副作用，随 settled/
   turn-end barrier 落盘即可。第一版每个 call 最多一次 approval，且工具必须在任何副作用之前
   调用它；第二次 approval 或“先修改、后审批”都是工具契约错误，不能用 not-started 规则恢复。
   requested 或 allow 的 durable write 失败必须 fail closed，不提示后继续、也不把 Allow 返回工具。
5. `turn/completed|failed|cancelled` 提交前，flush 该 turn 的所有前置事件。

这让崩溃恢复有可判定的语义：

- 没有该轮 durable 的 `tool/call-started`：工具确定没有派发，可按原 provider 顺序安全重新派发。
- 有 `tool/call-started` 和 `approval/requested`，但没有 durable 的 `approval/resolved`：确定没有
  执行工具副作用，resume 可以安全重新进入审批，不受 `Tool::idempotent()` 限制。
- 有 durable 的 `approval/resolved(deny)`：确定没有执行工具副作用，直接用 event 中的拒绝原因
  重建 settled denial，不重新提示、不执行工具。
- 有 durable 的 `approval/resolved(allow)`，没有 `tool/call-settled`：结果未知，不能假设失败并重做。
- 有 `tool/call-started`、没有 `approval/requested`、也没有 `tool/call-settled`：无法证明该工具
  需要审批，仍按结果未知处理。
- 有 `tool/call-settled`：使用真实结果，不因同轮其他调用中断而丢失。
- 只有“结果未知”且 `Tool::idempotent()` 为真的调用允许自动重放；结果未知的非幂等工具给模型
  明确的 uncertain outcome，并要求核对外部状态。确定 not-started 的调用不受这条幂等限制。

komo 保留比 deepseek-harness 更强的恢复语义：不把所有崩溃 turn 一律用 synthetic closers
关闭。`assistant/round`、provider continuation state、已结算 tool result 都进入事件日志，
`resume` 从最后一个稳定 step 继续；只有无法验证或无法续跑的尾部才关闭为 interrupted。

独立的 turn journal 因此没有继续存在的理由：它原本记录的 envelope、assistant round 和 results
都成为 `SessionEvent`。日志的生命周期跟 session 走，不再有“journal 按 status 或 7 天删除”的
第二套保留策略。

---

### D3 · 区分 turn continuation 与 conversation surface

**决定**：同一条事件日志派生两个不同读模型，不能都叫“模型历史”：

- **turn continuation**：只在当前 turn 执行和崩溃恢复时使用，由 `assistant/round`、
  `tool/call-settled` 等事件重建 provider 下一轮请求。
- **conversation surface**：后续 turn 使用的长期对话历史，保持 komo 现有语义，只接收
  `user/message`、最终 `assistant/message` 和 compaction summary；原始 tool rounds 不重复注入。

每个会产生长期 conversation message 的 event 必须声明它如何进入 surface：

```text
surfaceOp: "append"                    普通 user / 最终 assistant message
surfaceOp: { replace, start, end }     compaction：用当前 event 替换 surface 上的连续区间
无 surfaceOp                           round / tool / turn / approval / cancel 等不进长期上下文
```

`start` / `end` 引用的是当前 surface 上真实存在的 event seq；replacement event 同时记录所有
被遮住的 `sourceEventSeqs`。fold 时验证引用存在、顺序合法且覆盖完整，然后原子地替换 surface
区间。旧事件仍留在权威日志里。

读取不能“在第一条 compaction 处停止”。后续还可能 append 新消息，也可能再次替换之前生成的
summary；正确规则是按 seq fold 完整日志，或者从有版本和 watermark 的 projection checkpoint
恢复后继续 fold 尾部。Checkpoint 是可删除重建的加速缓存，不是第二权威来源。

由此得到三种视图：

- 人看的 transcript 读取 append-origin 消息和完整事件 provenance，能看见摘要遮掉了什么。
- 当前 turn 的下一轮模型请求读取 turn continuation。
- 后续 turn 的模型请求读取 conversation surface。

Compaction 只减少模型上下文，不直接回收日志磁盘空间；物理回收由 D1 的 retention base +
整段截断协议完成。

---

## 2. 目标数据模型

### 2.1 Session

```text
Session = SessionHeader + [SessionEvent...]
```

`SessionHeader` 保存不可重放的身份元数据；`SessionEvent` 保存可重放的 session 事实。

```rust
struct SessionEvent {
    version: u32,
    seq: u64,
    occurred_at: DateTime<Utc>,
    ignorable: bool,
    kind: SessionEventKind,
}
```

`seq` 是持久化顺序，也是 fold 顺序；时间戳只用于展示和诊断，不能用于恢复排序。
事件 payload 必须是可版本化的 owned data，不能引用进程内对象。`ignorable` 默认 `false`，
第一版所有事件都为 `false`；字段先保留给未来不影响模型历史、恢复和副作用判定的观测事件。

第一版物理布局是每 session 一个目录，而不是一个无限增长的文件：

```text
sessions/<session-id>/
  manifest.json
  base.<through-seq>.json
  000000.jsonl
  000001.jsonl
  ...
```

`manifest.json` 保存不可变 `SessionHeader`、`truncated_before`、当前 retention base 和 segment 的
`{ordinal, first_seq, sealed_last_seq?}`；它是存储元数据，不是 SessionEvent。active segment 的完整
尾 record 决定 `next_seq`，manifest 不随每批 append 重写。segment 按序列化字节数达到代码常量
`SEGMENT_TARGET_BYTES` 后，在 completed-turn 边界滚动；单个超大 turn 可以让当前段暂时超过目标，
不能在可续跑的 turn 中间切断恢复单元。sealed segments 总量超过代码常量
`SESSION_RETAINED_BYTES` 时，retention sweep 从最老的 completed-turn segment 开始截断，直到回到
目标内。安全条件优先于空间目标：若最老 segment 含 recoverable/unlearned run，允许暂时超限，
记录 blocked bytes 并在状态变化后重试，绝不能为了满足上限强行截断。两个阈值都从现有 session
分布实测后固化为代码常量，不增加 runtime 配置。

首次 materialize 必须原子提交 manifest 和首段，之后 segment 只 append。manifest 通过临时文件、
rename、文件 fsync 和父目录 fsync 原子替换。存储接口暴露 `append_batch`、`durable_flush`、
`read_from(seq)`、`load` 和 `truncate`，不把 JSONL/segment 细节泄漏到 runtime。

### 2.2 第一版事件词汇

```text
session/title-changed
session/model-changed

turn/started
user/message
request/header
request/context
assistant/round
tool/call-started
tool/call-settled
assistant/message
turn/completed | turn/failed | turn/cancelled

compaction/started | compaction/completed
learning/completed | learning/skipped
approval/requested | approval/resolved
```

这里的 `assistant/round` 是 provider 一轮 completion 的可恢复记录；`assistant/message` 是 turn
结束后进入 conversation surface 的最终消息。两者不能互相替代。
Compaction 的 summary 使用 `source = compaction` 的 `user/message`，并携带
`surfaceOp.replace`；bracket event 只记录 compaction 生命周期，不产生模型消息。

`request/header` 是下一次 provider request 在 derived messages 之外的完整稳定输入：实际生效的
provider/model/effort 和 sampling config、渲染后的 system prompt、按顺序组装的 tool schemas。
它必须 canonicalize 后做全字段相等比较，只在以下时点写完整快照：

- `reason = initial`：新 session 的第一次 provider request。
- `reason = resume`：恢复出来的 loop 第一次 request，即使内容与上一快照相同也写，用于标记续跑边界。
- `reason = change`：后续 request 的 canonical header 与最新快照不同时。

其余轮次继承最近的 `request/header`，不能沿用旧 journal 的习惯每轮复制 system prompt 和 tool
schemas。`foldRequestHeader` 只取截至目标 seq 的最新完整快照，不使用 delta。

`request/context` 只保存 route/capacity 元数据：`provider`、`model`、可选 `context_window`；仅在
route 或 capacity 变化时写。它不包含 system prompt、tool schemas 或 messages，也不参与
request/header 相等比较。

`tool/call-started`、`tool/call-settled`、`approval/requested`、`approval/resolved` 都携带
`call_id + call_index`。`call_id` 负责身份关联，`call_index` 是对应 `assistant/round` 中的 provider
顺序；同一 round 内必须唯一、连续且两者对应一致。append 时仍按真实发生顺序分配 event seq，
所以 settled seq 只表示完成先后，不能作为下一轮 tool results 的排列依据。
Continuation fold 先按 `call_index` 填结果槽，再把 mid-turn interjection 放到全部 tool results 之后，
与当前 live `TurnLoop::step` 的顺序一致；重复、越界或与 `call_id` 对不上的 index 必须拒绝恢复。

审批在当前实现中由工具按实际参数动态调用 `ToolContext::decide()`，executor 不能在派发前可靠判断
某个 call 是否需要审批。因此 `approval/requested` 必须存在：只看 `approval/resolved` 的缺失，
无法区分“正在等审批”和“不需要审批、已经执行”。目标 `ToolContext` 需要携带当前 call 的
`call_id/call_index`，在同一个入口记录 requested/resolved，避免各工具分别实现留痕。
`approval/resolved` 的核心 payload 必须包含 allow/deny 和 denial feedback；放行层级、scope key、
wait time 可以作为后续审计字段，但不能缺少恢复拒绝结果所需的决定与反馈。

未知 event type 默认拒绝恢复；只有显式标为 ignorable、且保证不影响模型历史和副作用判定的
事件才允许跳过。当前项目不保留旧格式兼容：开发阶段可以删除 disposable 的 state/session
数据，但不能加入双写、fallback 或长期 migration 层。

### 2.3 权威日志与读模型

| 读模型 | 输入事件 | 存放位置 | 是否权威 |
|---|---|---|---|
| turn continuation | assistant round + tool settle + interruption | 当前 turn 内存 | 否 |
| conversation surface | user/final assistant + `surfaceOp` | 内存，可 checkpoint | 否 |
| human transcript | 消息事件 + provenance | 按需 fold | 否 |
| current session state | session/turn/model/title 事件 | state.db | 否 |
| run ledger | turn/assistant/tool/approval 事件 | state.db | 否 |
| learning queue | turn end + learning 状态事件 | state.db | 否 |
| skill/memory usage | tool settle + learning 事件 | state.db | 否 |

Projection 的更新顺序固定为“日志先提交，投影后更新”。投影失败时标记 lag，后台按 watermark
补 fold；不能回滚或覆盖已经提交的日志。冷启动可以用 `{projection_version, last_seq, state}`
checkpoint 加速，版本不匹配时丢弃并从日志重建。

`ProjectionCheckpoint` 和 `RetentionBase` 不是同一种东西：

- Projection checkpoint 是可删除缓存，丢失只影响性能。
- Retention base 是删段后的新权威起点，至少包含 `through_seq`、下一条 `truncated_before`、
  当前 conversation surface、最新 request header/context 和继续 fold 所需的 session state。
  一旦 manifest 指向它并删除旧 segment，它就不能单独删除；否则权威历史出现缺口。

Retention base 只在没有 open turn 的 segment 边界生成。截断后，人类 transcript 在
`truncated_before` 之前只剩 base 中的 summary/surface，不再保证能查看已删除的原始事件；这是
session 数据可丢弃和日志空间有上限的直接代价，不伪装成无损 compaction。
在 `surfaceOp` compaction 落地前，base 保存当前 `find_windowed` 会送给模型的完整 retained window；
落地后保存 compacted surface。两种情况都不扩大模型可见历史，并保证 base 自身也有界。

---

## 3. 一次 turn 的目标写入与恢复

### 3.1 正常路径

```text
claim session slot
  append turn/started + user/message
  append request/header only on initial / resume / change
  append request/context only when route / capacity changed
  flush request prefix

每轮：
  provider stream
  append assistant/round
  一次 append 本轮全部 tool/call-started
  durable flush 一次
  并发执行每个 tool call：
    若调用 ToolContext::decide：
      append + durable flush approval/requested
      await approval
      append approval/resolved
      allow 时 durable flush 后才继续
    execute / settle
    append tool/call-settled

  下一轮 provider request 前 flush 已结算结果
  continuation 按 call_index 组装 provider-order results

append assistant/message
append turn/completed
flush
release session slot

同步推进内存 projections，异步 checkpoint 到 state.db
finish 后分派 learning
```

事件 append 必须经过每 session 单写者；不同 tool 可以并发执行，但 seq 分配和 event commit
仍串行。HTTP 与 chat ingress 继续共用 `claim_session`，不能各自建立写入序列。

日志 durable 后立即同步推进当前 session 的内存 projection；state.db 中的持久化 projection
可以异步追赶。当前 turn 的 learning 直接消费这份已推进的 session state，不能依赖可能落后的
state.db；后台 learning sweep 再通过 projection watermark 补处理漏项。

### 3.2 崩溃恢复

恢复步骤固定为：

1. 只接收 record boundary 完整且 seq 连续的物理记录；丢弃 torn tail。
2. 有 `truncated_before` 时从权威 RetentionBase 开始 fold，否则从日志开头开始；可用匹配版本和
   watermark 的 projection checkpoint 跳过一段纯计算，但不能用它填补权威缺口。
3. 找到未结束 turn、未结束 assistant round 和未结算 call；用 `call_id` 关联同一调用，并校验
   `call_index` 与 assistant round 的 provider 顺序一致。
4. 按 `call_index` 建立与 assistant round 等长的结果槽：已结算 call 填真实结果；durable deny
   填拒绝结果；not-started call 可以不受幂等限制地重新派发或重新进入审批；只有 uncertain 且
   `Tool::idempotent()` 为真的调用可以自动重放。其余 synthesized result 也填回原 call_index，
   不能追加到完成结果末尾。
5. 能精确续跑时创建 continuation run 并从最后稳定 step 继续；不能时追加 interrupted closers。
6. 修复事件 durable 后才向调用方暴露恢复后的 Session。

恢复不能原地删掉用户消息或修改旧结果；cancel、said-more、interruption 和 repair 都追加新事件，
由 fold 决定当前含义。

### 3.3 删除与空间控制

- tool-output 和 checkpoints 继续按 7 天清理，event 中的引用过期后仍保留摘要和当时给模型的
  有界内容。
- `run prune` 仍是 state.db 内的单事务：向 projection-local tombstone 表写筛选条件或 run id，
  再删除对应 Run/RunStep 投影行。projector rebuild 必须同时读取 tombstone，不能让记录复活。
  state.db 被整体删除时 tombstone 也一起丢失，符合它作为 disposable operator state 的现有边界。
- retention sweep 只选择完整、sealed、结束于 completed turn 的老 segments；其中每个 run 必须
  已终结、不可 resume，且 learning 已 completed/skipped，不能为了空间丢掉仍待处理的 episode。
  然后 fold 并 durable 写唯一命名的 `base.<through-seq>.json`，再原子更新 manifest 的 base 引用
  和 `truncated_before`，最后删除已覆盖 segments 与旧 base：
  manifest 提交前崩溃最多留下无引用 base；提交后、删除前崩溃最多暂时多占空间，两者都不能
  留下不可读缺口。
- manifest 提交后，以新的 `truncated_before` 对账 state.db：删除 cutoff 前的 Run/RunStep 派生行。
  这一步失败只会短暂留下 stale projection；启动补扫和 projector watermark 必须再次收敛，查询
  不得把 cutoff 前的行当成可恢复 run。
- retention 条件在每个 completed turn 后检查，并在 gateway 启动时补扫；因此长驻飞书 session
  不需要 `/new` 才能释放空间。
- 删除 session 时，先停止该 session 的 ingress，flush 日志，再删除事件 artifact、projection、
  session-index 和关联的 disposable 文件。
- 不提供单 event 删除，也不为了回收 RunStep 改写 retained segment。

---

## 4. 改动清单

### 第一批 · 建立一条可跑通的权威日志

#### 1.1 定义 `SessionEvent` 与存储契约

- 在 `komo-core` 定义 `SessionHeader`、`SessionEvent`、第一版 event payload 和 fold 错误。
- 第一版 event 的 `ignorable` 全为 `false`，未知 required event、seq 缺口、非法 surface replacement
  必须 fail closed。
- **验证**：event round-trip；并发 append 仍连续；未知 required event 拒绝恢复；第一版 catalog
  不含 ignorable event。

#### 1.2 分段存储与 retention truncate

**已完成**，但接线是后补的：`seal_if_full`/`retention_candidates`/`truncate`
写完之后长期零调用者，段从不滚、retention 从无候选，整条链路从最上游断着。
现在 `SessionEventRepository::turn_boundary` 在 `runs.finish` 之后滚段，
滚成功才考虑切；`keep_from` 由 runtime 用 `project_runs` 的 recoverable
加 ledger 的 unlearned 算出。切只丢最旧的一个段，稳定在预算线而不是过线即清空。

接线时发现并修掉一个会毁数据的 bug：base 的文档一直写着 seq 故意不连续，
但 fold 要求全程连续，于是**任何一份真实的 base 都会让 session 读不出来**。
现在 `fold_surface` 的 `dense_from` 就是日志的 `truncated_before`：低于它只查
顺序，从它开始缺口就是缺口。


- 在 `komo-infra` 实现 manifest + segments + RetentionBase，以及 `append_batch`、`durable_flush`、
  `read_from`、`load`、`truncate`。
- segment 按实测后固化的字节阈值、只在 completed-turn 边界滚动；retention sweep 必须使用
  “先 base、再 manifest、最后删段”的提交顺序，并拒绝覆盖 recoverable 或尚未 learning 的 run。
- `durable_flush` 必须落到文件系统 durability boundary，不得只 flush 用户态缓冲。
- **验证**：torn tail 只丢最后半条；在 base write、manifest replace、segment delete 前后分别
  注入崩溃，恢复都无 seq 缺口；recoverable/unlearned segment 不被截断；持续写入长生命周期
  session 后 retained bytes 有上限。

#### 1.3 request header/context 去重

- `request/header` 保存 canonical full snapshot，只在 `initial`、`resume`、`change` 写；相同 header
  的普通轮次不写。
- `request/context` 只在 route/capacity 改变时写，不能承载 system prompt 或 tool schemas。
- **验证**：连续十轮相同 system/tools 只有一条 initial header；变化时一条 change；resume 必有
  一条 resume；任意 seq 的 header fold 能还原当时实际 request state。

#### 1.4 消息投影与旧写路径切换

- `SessionRepository::find_windowed` 改从事件日志的 conversation surface 读取。
- cancel / said-more 不再修改或删除历史行，只追加事件并由 fold 解释。
- user/assistant 写入改为 message events；journal envelope、assistant、results 改为
  request/assistant/tool events。
- 保留窗口读取性能：从 retention base 或 projection checkpoint + 文件尾恢复，不允许长会话
  每轮全量解析。
- 不做双写或旧格式 fallback；该数据均属 disposable，切换时明确删除旧 state/session 数据。
- **验证**：一次含工具调用的 turn 只从 SessionEvent 即可重建 provider continuation 和最终消息；
  全量读取和 windowed 读取结果一致。

#### 1.5 落实 turn durability barriers

- 每轮按 provider 顺序 batch append 全部 `tool/call-started`，durable flush 一次后再进入
  `join_all`；不能在每个 call 前各做一次 fsync。started/settled 都携带相同的 `call_index`。
- 每个调用 settle 时立即 append `tool/call-settled`；不再等待整轮 Results。下一 request/turn-end
  barrier 统一 durable flush 已结算结果；continuation projector 按 `call_index` 重排，不按完成 seq。
- 给 `ToolContext` 增加当前 `call_id/call_index` 和 approval event recorder：进入 approver 前 durable
  写 `approval/requested`；Allow 返回工具前 durable 写 `approval/resolved(allow)`；Deny 不执行工具。
- `RunStep` projection 从这两类 event 生成，tool output 的完整正文仍走有界外部文件。
- **验证**：一轮 10 个调用的 started barrier 只发生一次 durable flush；工具按 2→0→1 完成时，
  live 与 rebuild 都按 0→1→2 喂给 provider，interjection 都在结果之后；重复/越界 index 拒绝恢复。
  3 个调用完成 2 个时崩溃，durable 的 settled 使用真实结果，其余进入 not-started、重放或
  uncertain。审批等待期崩溃判 not-started 并重新审批，durable deny 重建拒绝结果，durable allow
  后崩溃才判 uncertain；requested/allow event 的 durable write 失败时工具未执行。

#### 1.6 删除 tool-output 零读者索引

- 停掉 `tool_output_store::index_append`，删除每 session 的 `tool-output/index.jsonl`；完整 output
  文件和 event 中的有界 preview/path 保留。
- 不增加替代索引：当前仓库只有这份 index 的写入点，没有生产读者，RunStep projection 已包含
  tool、bytes 和 output path。
- **验证**：超限结果仍可按 event 给出的路径读取，新 session 不再生成 `index.jsonl`。

### 第二批 · 把执行账本改成投影

#### 2.1 Run/RunStep 跨会话投影

**已完成**：`komo-core::domain::run_projection::project_runs` 是那个 fold，
`assert_ledger_matches_log` 在六种 turn 形态上逐字段对拍 fold 与现有写入端。
投影额外带 `ProjectedStep::settled`——账本表达不了「发出去了但没结算」，
因为 step 行是结算时才写的。

**切换前还要决的四件事**（都不是 fold 的问题，是写入端的）：

1. `prune` 删掉的 run 会在 rebuild 时复活，需要 `run/pruned` 之类的墓碑。
2. `outcome` 是可修订的，投影必须 merge 而不是覆盖。
3. `learned` 目前没有任何人写 `learning/completed`/`skipped`；
   coordinator 拿到事件依赖之前，它得继续留在行上——fold 恒返回 false
   会让 sweep 永远重读每一个 run。
4. `reconcile_interrupted` / `mark_resumed` 变成「无终止事件」+ 一个认领事件；
   认领靠日志 seq 分配天然串行化，比现在的行更新更可靠。


- state.db 的 Run/RunStep 表保留为查询索引，不再由 runtime 和 executor 直接作为事实源写入。
- projector 保存 `last_seq`，提交必须幂等；提供按单 session 和全量 rebuild。
- `run list`、`run inspect`、`unlearned(None, 200)`、`skills audit`、`memory used` 全部继续走投影表。
- `run prune` 的 tombstone 是 projection control state，不是 SessionEvent；prune 和投影删除保持
  一个 state.db 事务，不打开 N 个 session artifact。
- **验证**：保留 tombstone、清空 Run/RunStep 等派生表后重建，以上查询与清空前一致且被 prune
  的记录不会复活；跨 session prune 的返回计数仍精确。

#### 2.2 精确 resume

- 从 `assistant/round` 和逐 call settle 事件重建 `RebuiltTurn`。
- 未结算且幂等的工具自动重放；非幂等工具产生 uncertain result。
- 普通 Failed、Cancel 和 Interrupted 的 recoverable 状态由 fold 得出，不再分别更新多个字段。
- **验证**：覆盖 provider 前崩溃、部分 tool settle、全部 settle 未发下一轮、最终消息未提交四个断点。

### 第三批 · Projection 与 compaction

#### 3.1 projection checkpoint

- 为 conversation surface、session summary、run ledger 定义版本化 fold。
- checkpoint 至少在 SessionHeader materialize、`turn/*` 结束和 session dispose 时写入。
- 日志领先 cache；cache 写失败可观测但不让 turn 失败。
- **验证**：任意 projection checkpoint 置旧或删除后，冷读结果不变；此测试不得删除权威
  RetentionBase。

#### 3.2 `surfaceOp` + compaction

- 给 user/final-assistant event 加 surface 声明并实现严格 replacement 验证；tool settle 只进入
  turn continuation 和 run ledger，不直接进入长期 conversation surface。
- 摘要生成后追加 replacement event，不改写旧事件。
- 后续消息和第二次 compaction 必须能继续在当前 surface 上工作。
- **验证**：完整 fold 与 checkpoint + tail fold 一致；未被 retention truncate 的 human transcript
  保留被遮住内容；模型只见摘要。

### 第四批 · 相邻正确性问题

这些问题由本次核对发现，但不是权威事件日志首批实现的前置条件：

1. `delegate` session 不参与 learning，避免父子 session 被算成两次独立 evidence。
2. 等待 `claim_session` 的 ingress 也必须先登记 cancel ownership，Stop 同时覆盖运行中和排队中。
3. consolidator 的关联检索能看到 `Rejected` memory，但 Rejected 仍不得进入自动注入。
4. 第一批已记录 approval 生命周期；后续再给 `approval/resolved` 补最终放行层级、scope key 和
   等待时间，作为 run ledger projection 的审计字段。
5. memory observation 增加 provenance class，防止 untrusted tool content 被当成用户事实晋升。
6. **二次 resume 会丢掉第一次续跑之前的轮次**。续跑是一个新的 `Run`，事件按新 run_id 落盘，
   而 `rebuild_from_events` 只认单个 turn_id，于是 A→B→C 的链条里 C 看不到 A 的 assistant
   round，退化成从用户消息重跑整轮。降级是安全的（只是白花钱），但一次崩溃可恢复、两次就不行
   并不是想要的语义。修法二选一：`resume_interrupted` 沿 `resumed_from` 收集整条链按集合过滤，
   或让续跑的事件沿用原始 turn_id、把 ledger 的 run_id 和日志的 turn_id 分开——后者更正确，
   一个逻辑 turn 在日志里就该是一个 id。

---

## 5. 明确不做

- 不把 `SessionEvent` 理解成“所有数据放进一个文件”；global durable domains 保持独立。
- 不保留 transcript、journal、RunStep 三套权威写入，也不做长期双写兼容层。
- 不把 projection cache 当成恢复兜底；cache 丢失必须只影响性能。RetentionBase 是截断后的权威
  起点，不属于这条规则。
- 不用时间戳排序，不允许同一 session 多写者自行分配 seq。
- 不在 compaction 时删除被遮住的事件，也不把模型上下文压缩描述成磁盘回收。
- 不为一轮里的每个 `tool/call-started` 各做一次 fsync。
- 不把 `run prune` 广播成 N 个 session event；它只写 projection control state。
- 不自动重放 outcome unknown 的非幂等工具。

---

## 6. 完成判据

1. 保留 projection-control tombstones、清空其余 session/run projection 表后，能从
   SessionHeader + RetentionBase + retained SessionEvents 重建所有未过期查询结果。
2. 删除任何 projection checkpoint 后，模型历史不变；删除 RetentionBase 必须被加载器拒绝为
   权威缺口，不能静默退回残缺日志。
3. 长生命周期 session 持续写入并触发多次 segment roll/truncate 后，retained bytes 回到上限内；
   在 truncate 每个提交点注入崩溃都能恢复。
4. 连续十轮相同 system prompt/tool schemas 只写一次 initial `request/header`；change/resume 的
   full snapshot 能在任意 seq 重建当时 request state。
5. 一轮 10 个 tool calls 的 started barrier 只做一次 durable flush；tool 按任意完成顺序 settle，
   live 与 rebuild 仍按 `call_index` 产生 byte-identical 的 provider-order results。
6. 在 approval requested 前、等待中、deny/allow durable 前后、tool settle 前后分别注入崩溃：
   requested 后无 resolved 的调用判 not-started 并重新审批，durable deny 重建拒绝结果，durable
   allow 后未 settle 才判 uncertain。
7. 一个 session 的 retained event seq 与 `truncated_before` 连续，两个 ingress 不会并发提交同一序号。
8. 新 turn 不再写旧 transcript、turn journal 和直接 RunStep 权威路径；新 session 不再生成
   `tool-output/index.jsonl`。
9. `cargo test --workspace` 通过，并有长会话 windowed read 的回归测试，避免每轮全量 fold。
