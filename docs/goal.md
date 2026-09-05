Goal runtime

如果这还是你之前的 Komo，我会做一个比较大的核心调整：

> **把 `SessionManager` 从核心抽象里拿掉，改成 `GoalRuntime + TaskRuntime`。**
>
> Session 仍然存在，但只作为 LLM provider / prompt cache / replay 的内部实现细节，永远不暴露给用户。

整体我建议长这样：

```text
TG / Lark / WeChat / TUI / CLI
               │
               ▼
┌──────────────────────────────┐
│        Message Gateway       │
│ identity / normalize / route │
└──────────────┬───────────────┘
               ▼
┌──────────────────────────────┐
│         Goal Router          │
│ 这条消息属于哪个 Goal？      │
│ 新 Goal / 继续 / 打断 / 恢复 │
└──────────────┬───────────────┘
               ▼
┌────────────────────────────────────────┐
│              Goal Runtime              │
│                                        │
│ Goal                                   │
│ ├── Task A ── Run ── Tool Calls       │
│ ├── Task B ── Run ── SubAgent         │
│ ├── Task C ── waiting                  │
│ └── Working State                     │
└──────┬─────────┬──────────┬────────────┘
       │         │          │
       ▼         ▼          ▼
   Context    Memory      Skills
   Builder    System      Registry
       │         │          │
       └─────────┴──────────┘
                 ▼
        ┌─────────────────┐
        │   Agent Kernel  │
        │ think / act /   │
        │ observe / steer │
        └───────┬─────────┘
                ▼
        Tool Runtime / MCP
                │
          Policy / Sandbox
                │
                ▼
         Environment
```

## 1. 最重要的抽象：Goal → Task → Run，而不是 Session

这是我认为你这个 Bot 和 Hermes / OpenCode / Codex 最大的差异。

```text
Goal
“把我的个人 Agent 做出来”

  Task
  “设计 memory”
      Run #31
      Run #42

  Task
  “实现 TG gateway”
      Run #55

  Task
  “优化 memory retrieval”
      Run #93
```

其中：

| 概念                      | 生命周期     | 用户是否感知         |
| ------------------------- | ------------ | -------------------- |
| Goal                      | 天 / 周 / 月 | 是                   |
| Task                      | 分钟 / 天    | 是，但不一定明确展示 |
| Run                       | 秒 / 小时    | 否                   |
| Provider Thread / Session | 任意         | 完全否               |
| Prompt Cache Lane         | 任意         | 完全否               |

比如你今天在 TUI 说：

> memory 我想改成 episodic + semantic

明天在 Telegram 说：

> 昨天那个 memory 继续一下

Bot 不应该找“Telegram session”。

而应该做：

```text
message
   ↓
GoalResolver
   ↓
goal = personal-agent
task = memory-design
   ↓
restore working state
retrieve relevant memory
continue task
```

所以 Channel 和 Goal 是 **N:N**：

```text
              ┌── Telegram
Goal A ───────┼── TUI
              └── Lark

Telegram ─────┬── Goal A
              ├── Goal B
              └── Goal C
```

Provider 自己的 conversation/thread ID 仅记录在某个 `RunContext` / `CacheLane` 下。

---

# 2. Agent Kernel 要非常薄

这里我更赞同 Pi，而不是把 Hermes 整套东西搬进核心。

Pi 的设计很有价值：默认就是一个非常小的 coding harness，只有少数核心工具，同时通过 Extensions、Skills、RPC、SDK 扩展；extension 甚至可以拦截 lifecycle/tool call/context compaction。([GitHub][1])

你的 kernel 最好基本只有：

```rust
loop {
    context = context_builder.build(goal, task);

    response = model.generate(context, tools);

    match response {
        Final(answer) => break,
        ToolCall(call) => {
            result = tool_runtime.execute(call);
            append_event(result);
        }
    }
}
```

不要一开始往 loop 里硬编码：

```text
memory
skill learning
planning
reflection
scheduler
subagent
browser
message platform
prompt compression
```

这些都应该围绕 Kernel 工作。

也就是说：

> **Kernel 稳定，Harness 可进化。**

Prime Agent 的 Continual Harness 正好值得借鉴：它把 supplemental prompts、memory、skill descriptions、subagent specs 当成可以持续、小步、有证据地改进的 durable state，而不是把整个 agent loop 不断重写。([iTHub][2])

---

# 3. 自进化：不要让 Agent “修改自己”，而是让 Harness 学习

这是整个设计中我最建议你控制住的地方。

Hermes 已经在做比较激进的版本：复杂任务完成、踩坑后找到正确路径、用户纠正等情况下，可以自己 create/patch/edit skill。([GitHub][3])

你可以借鉴这个触发机制，但再往前做一步：

```text
Experience
    ↓
Reflection
    ↓
Learning Candidate
    ├── MemoryCandidate
    ├── SkillCandidate
    ├── RoutingPolicyCandidate
    ├── PromptPatchCandidate
    └── ToolWorkflowCandidate
             ↓
           Eval
             ↓
      shadow / experiment
             ↓
           Promote
```

例如连续发生：

```text
Goal: CR Shoplazza MR

Run 1:
grep → read 20 files → 看错方向 → 用户纠正

Run 2:
先 git diff → 找 affected files → callers → tests
成功
```

Agent 不应该只写一条：

> 用户喜欢先看 diff。

而应该产生：

```yaml
candidate:
  type: procedural
  trigger:
    task: code-review
    source: git-diff
  workflow:
    - inspect diff
    - inspect callers
    - inspect tests
    - identify behavior changes
  evidence:
    successful_runs: [...]
    failed_runs: [...]
```

反复有效后：

```text
Procedural Memory
        ↓
Skill
```

这才是真正的：

> experience → memory → procedure → skill

而不是每完成一次任务就疯狂生成 `SKILL.md`。

### 哪些东西允许自动进化

```text
                   自动修改
Memory             ✓
Skill               ✓
Skill trigger       ✓
Prompt fragment     ✓
Tool selection hint ✓
Subagent spec       ✓
Retrieval weights   ✓

Tool implementation  △ 需要 tests
Policy               △ 只能更严格，放宽需确认
Agent Kernel         ✗
Security boundary    ✗
Credential logic     ✗
```

核心原则是：

> **自进化发生在 data plane，不发生在 trusted kernel。**

Codex 的 sandbox + approval 分离就是值得保留的安全边界；尤其 Tool/MCP 还可能跨越本地 sandbox 的信任边界，因此 Policy 应该控制“能力”，而不能只控制某一个 shell tool。

---

# 4. Memory 不要等同于 Vector DB

我建议直接借认知心理学建立五层。

```text
┌─────────────────────────────────────┐
│ Working Memory                      │
│ 当前 Goal / Task 正在想什么         │
├─────────────────────────────────────┤
│ Episodic Memory                     │
│ 我们经历过什么                      │
├─────────────────────────────────────┤
│ Semantic Memory                     │
│ 我知道什么                          │
├─────────────────────────────────────┤
│ Procedural Memory                   │
│ 我应该怎么做                        │
├─────────────────────────────────────┤
│ Prospective Memory                  │
│ 以后需要做什么                      │
└─────────────────────────────────────┘
```

对应你的 Agent：

| 人类认知    | Agent                                        |
| ----------- | -------------------------------------------- |
| Working     | 当前 Goal/Task state、plan、open questions   |
| Episodic    | 某次任务发生了什么、失败/成功过程            |
| Semantic    | 用户偏好、项目知识、事实、概念               |
| Procedural  | Skill、workflow、tool-use pattern            |
| Prospective | reminder、pending dependency、scheduled task |

Working Memory 最近还有持续的模型争论，但作为有限、任务相关的在线认知资源来处理是合理的。([Springer][4])

真正关键的是 Memory **生命周期**：

```text
Experience
   ↓ encode
Episodic
   ↓ repeated retrieval / reflection
Consolidation
   ↓
Semantic / Procedural
   ↓
retrieval
Working Memory
   ↓
new experience
```

认知心理学里 retrieval 本身会加强长期记忆，而 spacing + retrieval practice 对长期保持也有大量证据。([ScienceDirect][5])

这意味着你的 Agent 可以设计一个很有意思的机制：

```text
memory 被成功 retrieve
        ↓
本次任务帮助程度高？
        ↓ yes
strength +1
association(goal, entity, skill) +1
        ↓
达到阈值
        ↓
consolidate
```

近期的 agent-memory 工作也开始明确采用 episodic graph → semantic distillation 这类结构，而不是单纯“所有历史 embedding 后 top-k”。([ACL Anthology][6])

每条 memory 至少有：

```rust
Memory {
    id,
    kind,

    content,

    entities,
    associations,

    provenance,
    confidence,

    created_at,
    last_retrieved_at,
    retrieval_count,

    salience,
    strength,

    valid_from,
    valid_until,
}
```

**provenance 一定不要省。**

否则自进化几个月之后，你会得到一个“坚信自己幻觉”的 agent。

---

# 5. Memory retrieval 应该是 associative，而不仅是 semantic similarity

例如你说：

> CR 一下 baymax

如果只有 embedding：

```text
baymax 相似内容 top 10
```

不够。

应该同时扩散：

```text
baymax
  ↓ entity
Shoplazza POD
  ↓
code review
  ↓ procedure
CR skill
  ↓ episodic
以前 baymax CR 的经验
  ↓ semantic
你的 code review 偏好
```

可以做：

```text
score =
    semantic_similarity
  + entity_overlap
  + goal_relevance
  + temporal_relevance
  + retrieval_strength
  + provenance_quality
  + procedural_match
```

第一版其实不用 Graph DB：

```text
Turso / SQLite
+
FTS/BM25
+
embedding
+
memory_links
```

已经足够。

---

# 6. Goal 自己有 Working Memory

不要每次把历史 conversation summary 当工作状态。

应该：

```yaml
goal:
  id: personal-agent

  objective: 做个人长期运行 agent

  current_state: architecture decided
    implementing memory

  decisions:
    - Rust
    - goal-oriented instead of session-oriented

  open_questions:
    - retrieval algorithm
    - evolution evaluator

  artifacts:
    - repo xxx

  tasks: ...

  next_actions: ...
```

这是 **Goal State**。

原始聊天只是 evidence/event。

这样即使：

```text
换模型
换 provider
context compact
Telegram → TUI
程序重启
两个月之后继续
```

都没关系。

---

# 7. Prompt Cache 从架构第一天就考虑

这个我建议你不要后补。

当前 xAI 的缓存也是典型的 prefix cache：前缀越稳定命中率越高，并明确建议 static content 前置、旧 message 不修改、持续 append；Responses API 可以用 `prompt_cache_key`。([Grok API Documentation][7])

OpenAI Responses 现在同样提供 `prompt_cache_key`，以及 provider 侧 conversation/previous response 能力。([OpenAI Developers][8])

Anthropic 则有显式 prompt caching，支持不同 TTL 的 cache。([Claude Platform Docs][9])

所以 Context Builder 建议固定顺序：

```text
┌────────────────────────────┐
│ ① Stable Kernel Prompt     │ ┐
│ agent identity             │ │
│ invariants                 │ │
│ safety policy              │ │ Cache-friendly
│ stable tool definitions    │ │
│ stable protocol            │ ┘
├────────────────────────────┤
│ ② Semi-stable              │
│ user profile               │
│ project context            │
│ skill catalog              │
├────────────────────────────┤
│ ③ Goal Context             │
│ goal state                 │
│ task state                 │
│ retrieved memory           │
├────────────────────────────┤
│ ④ Current Run              │
│ recent events              │
│ tool results               │
│ current user message       │
└────────────────────────────┘
```

尤其注意：

**不要这样：**

```text
System:
Current time: 2026-09-05 10:53:42
...
5000 tokens static prompt
```

因为最前面的 timestamp 每次变，直接污染 prefix。

而应该：

```text
[static cacheable prefix]

--- dynamic context ---
time = ...
goal = ...
memory = ...
```

Tool schema 也要：

```text
canonical serialization
固定顺序
稳定 descriptions
```

不要每 turn 根据几十个条件生成略有不同的 tool JSON。

Skill 采用 **progressive disclosure**：

```text
prompt 只放：
skill name + description

真正命中后：
load SKILL.md
```

Hermes 目前也是这种思路，以减少 context token。([GitHub][10])

---

# 8. 我建议引入一个内部概念：CacheLane

这可以彻底解决你“不要 Session，但又需要缓存”的矛盾。

```rust
CacheLane {
    id,
    goal_id,
    task_id,

    provider,
    model,

    provider_thread_id,
    prompt_cache_key,

    prefix_hash,
    toolset_hash,
    created_at,
}
```

用户完全不知道它存在。

例如：

```text
Goal personal-agent
    │
    ├── Task memory
    │      └── CacheLane Claude-1
    │
    └── Task gateway
           └── CacheLane GPT-1
```

模型切换：

```text
Claude CacheLane
      ↓
materialize GoalState
      ↓
new OpenAI CacheLane
```

没有所谓：

> “因为切换模型所以开启了一个新 session。”

这就是 provider implementation detail。

Prime Agent 最近甚至加入了针对 Codex Responses 的 cached WebSocket transport，第一轮后尽量只发送新增 conversation items；这进一步说明这种优化应该封装在 Provider/Transport 层，而不是侵入你的 Goal 模型。([GitHub][11])

---

# 9. 从这些 Agent 各拿什么

| 项目            | 我会拿                                                                                |
| --------------- | ------------------------------------------------------------------------------------- |
| **Hermes**      | 常驻 gateway、memory/skills 学习闭环、cron、progressive skill loading                 |
| **Pi**          | 极薄 kernel、extension lifecycle、RPC/SDK embedding                                   |
| **OpenCode**    | plan/build 分离、subagent、tool permission、parallel exploration                      |
| **Codex**       | sandbox、approval、workspace trust boundary                                           |
| **Grok Build**  | headless + TUI、plan/review、skills/plugins/hooks/MCP/subagent 统一 extension surface |
| **Prime Agent** | Continual Harness、evidence-backed self improvement、RLM 思路                         |

Hermes 当前本身已经覆盖 Telegram/Discord/Slack/WhatsApp/Signal 等统一 gateway、持久 memory、skill 自改进和 scheduler，是你常驻 Bot 部分最直接的参考。([GitHub][12])

Grok Build 开源后的 harness 也明确包含 agent loop、tools、TUI，以及 skills/plugins/hooks/MCP/subagents extension system。([SpaceXAI][13])

Prime Agent 的 RLM 很有意思，但我**不会把 persistent IPython kernel 作为你的核心抽象**。它特别适合 coding/research，却不是 TG/Lark/家庭自动化/长期个人助手的共同最小模型；把它作为一种 `CodeExecutionEnvironment` 或 subagent backend 更合理。Prime Agent 本身目前确实把 persistent IPython kernel + recursive subagent 作为核心。([GitHub][14])

---

# 10. 最终我会定成 8 个核心模块

```text
komo/
├── gateway
│   ├── telegram
│   ├── lark
│   ├── wechat
│   └── tui
│
├── goal
│   ├── goal_router
│   ├── goal_runtime
│   └── task_runtime
│
├── agent
│   ├── kernel
│   ├── context_builder
│   └── model_router
│
├── tool
│   ├── registry
│   ├── runner
│   ├── policy
│   └── sandbox
│
├── memory
│   ├── working
│   ├── episodic
│   ├── semantic
│   ├── procedural
│   ├── prospective
│   └── consolidation
│
├── skill
│   ├── registry
│   ├── loader
│   └── evaluator
│
├── evolution
│   ├── reflector
│   ├── candidate
│   ├── evaluator
│   └── promotion
│
└── storage
    ├── event_store
    ├── state_store
    ├── blob_store
    └── search
```

其中最核心的依赖关系是：

```text
Message
   ↓
Goal
   ↓
Task
   ↓
Context
 ┌─┴──────────────┐
Memory          Skills
 └─┬──────────────┘
   ↓
Agent Kernel
   ↓
Tools
   ↓
Events
   ↓
Experience
   ↓
Evolution
 ┌─┴──────────────┐
Memory          Skills
```

这其实就是整个产品的飞轮。

---

## 第一版我反而建议砍得很狠

先不要做复杂多 Agent、Graph Memory、自动改代码、RL、自训练。

第一版只验证：

1. **同一个 Goal 能不能跨 TG/TUI/Lark 无缝继续。**
2. **程序重启后 Task 能不能正确恢复。**
3. **Agent 能不能把一次重要经验变成 episodic memory。**
4. **重复几次之后能不能 consolidation 成 semantic/procedural memory。**
5. **成功 workflow 能不能产生 skill candidate，并经过 eval 后升级。**
6. **Prompt cache hit rate 能不能稳定测出来。**

如果这六件事情做好，你做出来的就已经不是 Hermes clone 了。

最值得优先投入的两个创新点其实是：

> **Goal-oriented Runtime，而不是 Session-oriented Runtime。**

以及：

> **Experience → Episodic → Semantic / Procedural → Skill 的认知式学习闭环。**

这两个东西我认为可以成为你这个个人 Agent 真正长期积累价值的核心。

[1]: https://github.com/up0to1/pi-mono/blob/main/packages/coding-agent/README.md?utm_source=chatgpt.com "pi-mono/packages/coding-agent/README.md at main · up0to1/pi-mono · GitHub"
[2]: https://ithub.global.ssl.fastly.net/PrimeIntellect-ai/prime-agent?utm_source=chatgpt.com "GitHub - PrimeIntellect-ai/prime-agent: A self-improving RLM agent for coding workflows and long-running autonomous tasks. · GitHub"
[3]: https://github.com/mikesmarcos/hermes-agent-NousResearch/blob/main/website/docs/user-guide/features/skills.md?utm_source=chatgpt.com "hermes-agent-NousResearch/website/docs/user-guide/features/skills.md at main · mikesmarcos/hermes-agent-NousResearch · GitHub"
[4]: https://link.springer.com/article/10.1007/s41465-026-00357-5?utm_source=chatgpt.com "What is Working Memory? Can It be Improved? How Would This Go? | Journal of Cognitive Enhancement | Springer Nature Link"
[5]: https://www.sciencedirect.com/science/article/pii/S1364661310002081?utm_source=chatgpt.com "The critical role of retrieval practice in long-term retention - ScienceDirect"
[6]: https://aclanthology.org/2026.acl-long.625/?utm_source=chatgpt.com "HeLa-Mem: Hebbian Learning and Associative Memory for LLM Agents - ACL Anthology"
[7]: https://docs.x.ai/developers/advanced-api-usage/prompt-caching?utm_source=chatgpt.com "Prompt Caching | SpaceXAI Docs"
[8]: https://developers.openai.com/api/reference/cli/resources/responses/methods/retrieve?utm_source=chatgpt.com "Get a model response | OpenAI API Reference"
[9]: https://docs.anthropic.com/en/docs/about-claude/pricing?4810b549_page=3&73cdfb14_page=2&939688b5_page=1&e768fcd2_page=2&utm_source=chatgpt.com "Pricing - Anthropic"
[10]: https://github.com/openax-reference/nousresearch-hermes-agent/blob/main/website/docs/user-guide/features/overview.md?utm_source=chatgpt.com "nousresearch-hermes-agent/website/docs/user-guide/features/overview.md at main · openax-reference/nousresearch-hermes-agent · GitHub"
[11]: https://github.com/PrimeIntellect-ai/prime-agent/blob/main/packages/ai/CHANGELOG.md?utm_source=chatgpt.com "prime-agent/packages/ai/CHANGELOG.md at main · PrimeIntellect-ai/prime-agent · GitHub"
[12]: https://github.com/nousresearch/hermes-agent?utm_source=chatgpt.com "GitHub - NousResearch/hermes-agent: The agent that grows with you · GitHub"
[13]: https://x.ai/news/grok-build-open-source?utm_source=chatgpt.com "Grok Build is Now Open Source | SpaceXAI"
[14]: https://github.com/PrimeIntellect-ai/prime-agent/blob/main/packages/coding-agent/docs/index.md?utm_source=chatgpt.com "prime-agent/packages/coding-agent/docs/index.md at main · PrimeIntellect-ai/prime-agent · GitHub"
