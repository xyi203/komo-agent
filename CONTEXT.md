# komo

个人 agent 框架：以聊天助理为主形态（渠道 + reminder / cron / briefing / sweeps），同时承接单 turn 内自主完成的编码类任务。本文件只是术语表，不含实现细节。

## Language

### 产品定位

**可治理个人 Agent**:
以个人为服务对象、能长期承接事务的 Agent，其数据处理与副作用始终受本地控制、权限策略、审计和人工批准约束。
_Avoid_: 聊天机器人, 自动化脚本

### 会话与历史

**Turn**:
一次用户输入到一次最终回复之间的完整执行，内部可含多轮工具调用。turn 内部模型看到真实的工具调用与结果；turn 结束后这些细节不进入会话历史。
_Avoid_: round（round 指 turn 内部的一轮模型往返）

**Tool Note**:
折叠进 assistant 消息的本 turn 工具活动摘要——每步一行（工具名、参数片段、结果片段），并携带可回捞文件的路径。它是索引，不是记录。
_Avoid_: tool history, tool transcript

**Turn Trace**:
turn 结束时写入 tool-output 存储的本 turn 完整工具轨迹文件（每步 args + 模型侧结果原文），供后续 turn 用 read/grep 按需回捞。disposable，随 tool-output 的保留期过期；持久审计记录是 Run Ledger。
_Avoid_: checkpoint, snapshot

**Run Ledger**:
每个 turn 一条 Run、每次工具调用一条 RunStep 的持久审计记录。它记录发生过什么，不是可恢复的执行状态。
_Avoid_: checkpoint, session state

**History Window**:
每 turn 送入模型的会话历史窗口（条数 + 字节双界）。窗口外的消息仍在库里，只是不进 prompt；窗口被裁剪时向模型插入标记告知。
_Avoid_: context window（那是模型的物理上限，不是 komo 的裁剪策略）

### 执行控制

**Cancel**:
对进行中 turn 的终止：loop 的每个 await 都与取消信号竞速，被取消的 turn 记为 Failed 且不可恢复。入口是 TUI 的 Esc 和 GUI / HTTP 的 `POST /api/interactions/{session}/cancel`；聊天渠道没有取消命令。
_Avoid_: interrupt, abort（不要暗示可恢复或可续跑）
