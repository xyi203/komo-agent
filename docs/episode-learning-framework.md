# Episode 学习闭环设计

> 存储说明：本文写作时 komo 还有 `state.db` / `kanban.db` / `memory.db` / `cron.db` 四个库文件。ADR 0004 之后它们合并为一个 `~/.komo/komo.db`，文中的库名指的是其中对应的表，disposable / durable 是表的属性，不是文件的属性；除此之外结论不变。

> 状态：Phase 1、2、3 已实现（2026-08-25）。
>
> 范围：在现有 run ledger、reflective reviewer、Memory consolidation 和 Skill governance 之上，建立基于任务结果证据的学习闭环。
>
> Phase 1 落地位置与相对本文的偏差：
>
> - `komo-core` `domain/episode.rs` — `EpisodeView` / `OutcomeVerdict` /
>   `OutcomeEvidence` / `OutcomeAssessment` / `AssessedEpisode`。
> - `komo-services` `episode.rs` — `assemble`（§5.2 的 EpisodeAssembler，落成一个自由函数
>   而非结构体：第一版只有一个纯读取操作，没有需要持有的状态）。
> - `OutcomeEvaluator` 同样没有独立模块，实现为 `OutcomeAssessment::deterministic`
>   —— 它是 `EpisodeView` 上的纯函数，放在类型旁边比新开一个只含单个函数的模块更直接。
> - `komo-bot` `learning_coordinator.rs` — `LearningCoordinator`（替换
>   `ReviewCoordinator`，旧的 session cadence 路径连同 `reviewed_through` /
>   `review_candidates` / `mark_reviewed` / `count_user_turns` 一并删除，不做并存兼容层）。
> - watermark 落在 `Run.learned`（state.db 追加列，`DEFAULT true` 只回填历史行，
>   避免升级当天把整个既有 ledger 一次性喂给抽取）。
> - 触发保留了「攒够 interval 再学」的节流，但计数单位从 session 的 user turn 数
>   换成该 session 未学习的 run 数；sweep 不受 interval 约束。
> - 第 8 节的 `OutcomeAssessment` 持久化属于 Phase 2（需要被后续反馈修订时才需要），
>   Phase 1 只在内存中传给抽取器。

## 1. 结论

Komo 不需要新增一套 Episode 存储。现有 `Run` 已经表示一次用户请求驱动的 agent turn，`RunStep` 已经记录该轮的工具调用及结果。学习系统应把它们组装成只读的 `EpisodeView`，而不是复制事实。

学习闭环也不应实现成一个统一 Validator 驱动的线性流水线。Fact 和 Procedure 的验证问题不同：

- Fact 验证“这个主张是否为真”，进入现有 `MemoryConsolidator`；
- Procedure 验证“这套做法在什么条件下是否稳定有效”，进入受治理的 Skill candidate；
- Commitment 不是知识，继续进入 task inbox。

Outcome 是一组可追加、可修订的结果证据。它影响候选的可信度，但不决定一次 episode 是否允许产生学习。

```text
User Request
    │
    ▼
Run / Episode
    ├── Input / Final Output
    ├── RunSteps
    └── Execution Status
    │
    ▼
Episode Assembler
    │
    ▼
Outcome Assessment ◀──────────── 后续用户反馈
    ├── success
    ├── failure
    └── unknown
    │
    ▼
Learning Extractor
    ├── Fact Observation ──▶ Memory Consolidator ──▶ Memory candidate / evidence
    ├── Skill Patch ───────▶ Skill candidate ──────▶ operator / execution validation
    └── Commitment ───────▶ Task inbox
```

## 2. 目标

这个设计要解决四个问题：

1. 学习判断能看到一轮执行中真正发生的事情，而不只看到 user/assistant 文本；
2. 区分“agent 正常返回了回复”和“用户目标确实完成”；
3. 让成功、失败和后续用户反馈都能成为可审计的证据；
4. 复用 Memory 与 Skill 已有的治理路径，不增加第二套晋升规则。

非目标：

- 不把完整工具输出复制进长期 Memory 或 Skill；
- 不让 agent 根据一次自评直接激活 Skill；
- 不把 transient task result、commit SHA、单次报错等写成长久知识；
- 不改变 Wiki 的定位。Wiki 仍是用户维护、按需检索的外部知识源。

## 3. 当前基础与缺口

### 3.1 已有能力

当前实现已经具备学习闭环的大部分基础模块：

- `crates/komo-core/src/domain/run.rs`
  - 一次 user turn 对应一个 `Run`；
  - 每次工具调用对应一个 `RunStep`；
  - step 保存经过脱敏和截断的 args、result、error、structured output、耗时及 uncertain 状态。
- `crates/komo-bot/src/reviewer.rs`
  - 从 transcript 中提取 Memory observation、Skill proposal 和 Commitment；
  - 修改已有 Skill 前读取真实 Skill body，避免盲写覆盖。
- `crates/komo-services/src/memory_consolidation.rs`
  - 将 observation 分类为 `supports | contradicts | supersedes | unrelated`；
  - 对同一 session 的重复表达只计一次证据；
  - 冲突和替代会阻止旧 Memory 继续自动注入。
- `crates/komo-infra/src/skills.rs`
  - reviewer 只能写 Skill candidate；
  - candidate 在 operator promote 前不会进入运行时；
  - protected Skill 不接受自动提案。

### 3.2 关键缺口

当前 reviewer 的输入是完整 session transcript，但 `review_prompt` 只渲染 `Message.content`：

- `RunStep` 没有进入 reviewer；
- assistant message 上的 `tool_note` 没有进入 reviewer；
- reviewer 不知道 run 是成功返回、执行失败还是被取消；
- reviewer 按 session cadence 运行，而不是消费一个已经 settled 的 episode；
- `RunStatus::Done` 只表示完成了回复，不表示用户目标成功；
- Skill candidate 没有与使用它的 episode outcome 建立证据关系。

当前 post-turn review 还在 `runs.finish(&run)` 之前异步启动。未来一旦学习依赖最终 Run 和 RunStep，这个顺序会产生竞态，因此学习触发必须移动到 run finalization 之后。

## 4. 核心语义

### 4.1 Episode 等于一个 Run

Episode 的标识直接使用 `run_id`。它的事实来源保持唯一：

```text
RunRepository       → execution status、input、final output、timestamps
RunStepRepository   → tool calls、results、errors、uncertain
```

`Run.input` 和 `Run.final_output` 已经是本轮的 user/assistant 文本。当前 transcript
entry 没有 `run_id`，不能靠时间戳或相邻位置猜测归属；第一版因此只使用 ledger 中与
run 明确绑定的内容。`EpisodeView` 是临时 read model，不单独持久化：

```rust
struct EpisodeView {
    run: Run,
    steps: Vec<RunStep>,
}
```

组装过程必须复用 ledger 已做的脱敏和截断，不允许为学习流程绕过现有安全限制读取未经治理的原始输出。若未来确实需要超过 `RUN_FIELD_CAP` 的完整消息，应先在 transcript envelope 上增加明确的 `run_id`，不能增加基于时间或内容匹配的隐式关联规则。

### 4.2 Execution Status 不等于 Goal Outcome

需要明确区分两条轴：

```text
Execution Status：Running | Done | Failed
Goal Outcome：    Success | Failure | Unknown
```

典型情况：

| 场景 | Execution Status | Goal Outcome |
|---|---|---|
| 测试通过并交付结果 | Done | Success |
| 正常回复“无法确定原因” | Done | Unknown |
| 工具调用成功，但没有验证用户目标 | Done | Unknown |
| 测试明确失败 | Done 或 Failed | Failure |
| provider 中断 | Failed | Unknown |
| 非幂等工具超时，结果 uncertain | Failed | Unknown |
| 用户主动取消 | Failed | Unknown，默认不学习 |

`Done` 不能直接映射为 `Success`，`Failed` 也不能在缺少目标证据时直接映射为 `Failure`。

### 4.3 Outcome 是可修订的证据集合

很多结果无法在当前 episode 结束时确认：

```text
Episode A：agent 修改代码并回复完成
Episode B：用户说“可以了”或“还是不行”
```

因此 Episode End 只能产生 provisional assessment。后续 user turn 可以向前一个相关 run 追加结果证据，并重新计算 verdict。

概念模型：

```rust
enum OutcomeVerdict {
    Success,
    Failure,
    Unknown,
}

enum OutcomeEvidenceKind {
    UserConfirmed,
    UserRejected,
    DeterministicCheck,
    StructuredToolResult,
    ExecutionStatus,
    AgentClaim,
}

struct OutcomeAssessment {
    run_id: String,
    verdict: OutcomeVerdict,
    evidence: Vec<OutcomeEvidence>,
    evaluated_at: i64,
}
```

证据强度固定为：

```text
用户明确确认或否定
    > 测试、检查命令、结构化断言等确定性验证
    > 工具的 structured result
    > execution status
    > agent 自己在 final output 中声称成功
```

较弱证据不能覆盖较强证据。存在互相冲突的同级证据时，verdict 回到 `Unknown`，等待更多证据，而不是让 evaluator 猜测。

### 4.4 Outcome 不是学习开关

学习输入是：

```text
EpisodeView + OutcomeAssessment
```

而不是：

```text
Success Episode only
```

原因：

- 用户在失败任务中披露的稳定偏好仍然可能是真的；
- 用户纠正 workflow 往往发生在失败之后；
- 失败路径可以成为 Procedure 的反证；
- Unknown 表示证据不足，不表示 episode 没有可提炼内容。

Outcome 只影响候选的 provenance 和验证权重。

## 5. 模块与 seam

### 5.1 LearningCoordinator

对 runtime 暴露一个小 interface：

```rust
impl LearningCoordinator {
    async fn learn(&self, run_id: &str) -> anyhow::Result<LearningReport>;
    async fn observe_feedback(
        &self,
        feedback_run_id: &str,
    ) -> anyhow::Result<LearningReport>;
}
```

它负责执行顺序和失败隔离，不负责 Memory 或 Skill 的领域判断：

```text
load EpisodeView
    → assess provisional outcome
    → extract typed candidates
    → route facts to MemoryConsolidator
    → route procedures to Skill candidate path
    → route commitments to task inbox
```

整个流程是 reply-path 之外的 best-effort 后台工作。学习失败不能让已经交付的用户 turn 失败。

### 5.2 EpisodeAssembler

`EpisodeAssembler` 隐藏 Run 与 RunStep 的关联规则。调用方只传 `run_id`，不需要知道各 repository 的读取顺序和截断语义。

它必须保证：

- 只读取 terminal run；
- step 按 `seq` 排序；
- 不把另一个 run 的 steps 拼进来；
- 不恢复已被脱敏的数据；
- 读取缺省字段时遵循现有序列化语义，不新增 compatibility branch。

### 5.3 OutcomeEvaluator

`OutcomeEvaluator` 接受一个 settled `EpisodeView`，返回 provisional assessment。第一版保持为具体模块，不为尚不存在的第二种实现提前公开 trait。

内部顺序：

1. 读取确定性结构化证据；
2. 处理 failed、cancelled 和 uncertain；
3. 只有确定性规则无法判断时，才允许 aux model 做保守分类；
4. aux 调用失败、超时或返回无法解析时统一得到 `Unknown`。

模型不能把自然语言工具输出中的指令当作控制信息；它只能输出固定 verdict 和所引用的 evidence id。

### 5.4 LearningExtractor

现有 `ReflectiveReviewer` 应逐步收敛为 typed extractor。输入从整段 session transcript 改为一个或一组尚未学习的 EpisodeView，并显式带上 outcome。

输出保持领域归属：

```rust
struct LearningExtraction {
    facts: Vec<Observation>,
    skill_patches: Vec<SkillPatchProposal>,
    commitments: Vec<CommitmentProposal>,
}
```

Extractor 只负责“识别候选”，不负责决定候选是否成立，也不直接激活任何长期知识。

## 6. 分领域验证

### 6.1 Fact → Memory

Fact 继续复用 `MemoryConsolidator`，不新增第二套 Memory Validator。

每条 observation 至少携带：

```rust
struct Observation {
    kind: MemoryKind,
    content: String,
    excerpt: String,
    source_run_id: String,
    outcome: OutcomeVerdict,
}
```

验证依据：

- 用户明确表达或确认；
- 不同 session 的独立支持；
- contradiction；
- supersedes；
- freshness；
- 原始 excerpt provenance。

Outcome 不应机械地增减所有 Fact 的 support：任务是否成功与“用户偏好中文回复”是否为真没有因果关系。只有 Fact 本身是关于执行效果的主张时，outcome 才是它的直接证据。

### 6.2 Procedure → Skill

Procedure 不进入 Memory，也不增加 `MemoryKind::Procedural`。它表示“怎么做”，应生成已有 Skill 的 patch proposal，或在没有归属时生成新的 Skill candidate。

Procedure 的证据不是“相同文本出现了几次”，而是：

- 哪些 run 实际加载或采用了这条 Skill；
- 当时的任务条件是什么；
- outcome 是 Success、Failure 还是 Unknown；
- 是否存在确定性验证；
- 是否出现回归或反例。

第一阶段保持当前治理规则：所有自动提取只写 candidate，由 operator 通过 `komo skills promote|reject` 决定是否生效。不要在没有 replay、反例治理和回滚语义前增加自动 Skill 晋升。

### 6.3 Commitment → Task

Commitment 不是 Fact 或 Procedure，也不进入 Outcome Validator。继续沿用现有路径：

- 自动提取只进入 inbox；
- 使用 origin session 与 content-derived key 去重；
- 已在本轮完成的事项不提取；
- 用户后续确认、取消或完成时走 Task 自己的状态机。

## 7. 生命周期与触发顺序

正确顺序：

```text
persist user message
    → run agent loop
    → persist assistant reply or failure placeholder
    → finalize Run
    → runs.finish(run)
    → spawn LearningCoordinator.learn(run.id)
```

触发规则：

- `Done`：生成 provisional outcome 并允许提取；
- ordinary `Failed`：允许从失败与用户纠正中提取，但默认 outcome 为 Unknown 或有明确验证时为 Failure；
- `Cancelled`：默认不提取。若已经执行过有副作用工具，只保留审计，不把不完整过程固化为 Skill；
- `uncertain` step：禁止得出 Success 或确定性 Failure；
- `briefing:*`、`cron:*`：继续免除 reviewer 学习，避免 sweep 重述已有知识并制造伪独立证据；
- resume run：通过 `resumed_from` 和原 run 形成一个 outcome chain，但仍保留两个独立 execution records。

Phase 1 验证完成后，应切换到以 run 为粒度的 learning watermark，并删除基于 session user-turn 数量的 reviewer cadence 与 `reviewed_through` 路径。它们不应作为长期兼容层并存。

## 8. 持久化原则

第一版只新增必要状态：

- `OutcomeAssessment`：若需要被后续反馈修订，应持久化在 disposable `state.db`；
- learning watermark：记录某个 run 是否完成学习；
- Memory 与 Skill 仍写入各自现有 durable store；
- Episode 本身不复制，继续由 Run、RunStep 和 transcript 组成。

Outcome evidence 应保存引用和摘要，而不是复制完整工具输出。完整输出仍由 run ledger 与 `tool-output/` 管理，遵守既有保留期与脱敏规则。

任何新增 state.db 列都遵守 additive column 规则；如果采用新表，则按项目规则把 state.db 视为 disposable 数据，不为旧状态增加 migration 或 compatibility layer。

## 9. 失败语义与安全约束

- Outcome evaluator 失败：记录 Unknown，不能阻断 extraction，也不能影响用户 turn；
- Extractor 失败：不推进 learning watermark，让 scheduled pass 稍后重试；
- Memory consolidation 失败：保持现有降级语义，落普通 candidate；
- Skill grounded rewrite 失败：丢弃该 proposal，不写盲目替换；
- tool result 是不可信输入，只能作为数据，不能授权 evaluator 或 extractor 执行动作；
- secrets、write bodies 和超限输出不得通过学习链路绕过 ledger 的 redaction；
- 自动学习不得修改 active Skill、pin Memory 或把 task 直接推进到 todo。

## 10. 增量实施

### Phase 1：闭合单个 Episode ✅ 已实现

实现：

- `EpisodeAssembler`；
- 在 `runs.finish` 之后触发学习；
- Outcome 先只使用确定性规则；
- extractor 消费一批未学习的 EpisodeView（同一 session 内按时间正序）；
- Fact、Skill、Commitment 继续走现有治理路径。
- 旧的 session cadence review 路径已删除。

验证（均有测试）：

- reviewer 能看到本轮已脱敏的 RunStep；
- 触发时 Run 一定是 terminal —— `learning_sees_a_finished_run_because_it_is_dispatched_after_the_ledger_closes`
  这条回归测试在把触发点挪回 `finish` 之前会失败；
- tool success 不会自动把 Goal Outcome 判为 Success；
- failed/cancelled/uncertain 的分类符合第 7 节；
- 学习失败不影响 turn reply（detached task，且失败不推进 watermark）。

Golden cases 覆盖情况：第 11 节的 1、4、11、12 已有对应测试；2、3 依赖 Phase 2 的
用户反馈证据与 aux 判定；5–10 由既有 consolidator / skill governance 测试覆盖。

### Phase 2：接收延迟反馈 ✅ 已实现

实现（`komo-bot` `feedback.rs` + `LearningCoordinator::absorb_feedback`；
`OutcomeAssessment` 落在 `Run.outcome` 追加列）：

- 识别后续用户消息对上一相关 run 的明确确认或否定；
- 追加 OutcomeEvidence 并重新计算 verdict；
- 将更新后的结果关联到已有 candidates。

验证（均有测试）：

- “可以了”能确认上一 run，而不是当前空操作；
- “还是不行”能推翻 agent 的自报成功；
- 模糊反馈保持 Unknown —— 分类器 fail-closed：模型报错、超时、无法解析、
  一行里同时出现两个判词、判词不在行首，一律无判决；
- 新证据只关联同 session 的紧邻上一个 run，不向更早回溯；
- 用户确认压过 uncertain step，但 uncertain 证据仍然保留在列表里；
- 抽取器读的是存储的 outcome，不是重新计算的 —— 否则改判等于没改。

与本文的一处偏差：`absorb_feedback` 在上一个 run 没有存储 assessment 时会
重新计算确定性部分再追加，而不是从空列表开始。否则一个自身触发未曾跑过的 run
（崩溃、重启）会在收到反馈的那一刻丢掉它的 uncertain step 和失败记录。

### Phase 3：Procedure 效果证据 ✅ 已实现

实现（`operator_control/actions.rs` 的 `run_verdicts` + `skill_invocations` /
`skill_usage`）：

- Skill invocation 与 run outcome 关联；
- `komo skills audit` 聚合视图显示 `N✓ N✗ N?`，单 skill 视图每行显示该 turn 的
  结局，并单独列出失败反例的 run id；
- 保留 operator promote，没有自动晋升。

验证（均有测试）：

- 同一 run 内多次 view 只计一次（视图数仍照实显示，但结局按 run 计）；
- Unknown 不被当作 Success；
- 窗口外的 run 读作 Unknown，不读作成功；
- 失败反例在 `komo skills audit <name>` 末尾单独点名，不埋在计数里。

关于「Skill 被加载但没有采用时不算正向证据」：ledger 看不到「是否真的照做」，
所以这类情况落在 Unknown 桶里 —— 这是诚实的归类而不是检测。CLI 的说明文字里
写明了这一点。

尚未实现（来自 hermes 借鉴笔记，不属于本文 Phase 3）：`used >= N && 有失败`
时自动触发 reviewer 生成 skill 修订候选。本阶段先把证据摆出来给 operator 看；
自动提案建立在这份证据之上。

## 11. Golden cases

实现前先固定以下端到端样例：

1. agent 回复“完成”但没有验证：`Done + Unknown`；
2. `cargo test` structured result 为成功且目标就是通过该测试：`Done + Success`；
3. 工具成功但用户随后说“结果不对”：outcome 从 provisional 更新为 Failure；
4. non-idempotent tool uncertain：保持 Unknown，不重复调用，不提炼成功 Procedure；
5. 失败 episode 中用户明确说“以后都用中文”：仍生成 Preference observation；
6. 同一 session 重复表达相同 Fact：support 只计一次；
7. 两个独立 session 支持同一 Fact：满足 Memory 的独立证据规则；
8. 相同 Procedure 文本被提取两次但从未验证成功：仍只是 Skill candidate；
9. protected Skill 被提议修改：不生成 candidate；
10. scheduled sweep 重读同一 run：不重复产生 evidence 或 proposal；
11. cancelled pristine run：不学习；
12. cancelled run 已产生副作用：只保留审计，不推断结果。

## 12. 最终边界

```text
Transcript / Run ledger = 发生过什么
Outcome Assessment      = 结果证据目前支持什么判断
Memory                  = agent 受治理地相信什么
Skill                   = agent 在适用条件下怎么做
Task                    = 仍需推进什么
Wiki                    = 用户维护、按需读取的外部知识
```

这六个概念不能互相替代。学习闭环的价值不在于把更多内容自动写入长期存储，而在于让每次写入都能回到具体 episode、具体结果证据和明确的治理路径。
