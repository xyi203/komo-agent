# 四个 db 合成一个 `komo.db`：耐久性是表的属性，不是文件的属性

`~/.komo/state.db`、`kanban.db`、`cron.db`、`memory.db` 合并为一个 `~/.komo/komo.db`。
"可随手删"与"持久"的区别保留，但落在**表级规则**上（哪些表只能加性变更、哪些表允许行级清理），
不再靠把数据放进不同文件来表达。session 转录仍是 `sessions/` 下的文件，`permissions.json` 与
`skills/` 不变——本 ADR 只关于 Turso 库。

## 拆分当初的两个依据

拆成四个文件的理由写在 `docs/agents/architecture-notes.md`：

1. **删文件重置**。toasty 的 `push_schema` 只在新建文件时跑且不幂等，所以 schema 一变就要删文件；
   持久数据放在自己的文件里，删 `state.db` 才不会伤到它。
2. **一个文件一把跨进程锁**。Turso 对每个 db 文件持有排他锁，拆开似乎能让 CLI 在 gateway
   运行时至少碰到一部分数据。

两条今天都不成立。

**依据一已被原地迁移取代。** `persistence/mod.rs` 里的 `ensure_columns`（加列）、`ensure_table`
（建表，`db.rs` 已用它在既有 state.db 上补了 `inbox_records` 和 `run_memory_records`）、
`drop_retired_columns`（删列）覆盖了实际发生过的每一种 schema 变更。"删文件重置"这条路已经没人走，
而且走不得：state.db 里的 reminder、pairing、settings（`/sethome` 覆盖、briefing 水位）都是用户
手写的，删了就丢。`persistence/mod.rs` 那句"state.db 还装着全部转录"也已过期——转录是文件。
所谓 disposable 早已名不副实。

**依据二从未兑现。** gateway 启动时四个库全部打开（`cli/gateway.rs` 开 state / kanban / cron，
`cli/wiring.rs` 开 memory），CLI 一样全被锁在外面走 loopback（`services/operator_control`）。
拆开没有换来任何并存能力，coexistence 是靠 api channel 解决的，与文件数无关。

## 拆开的真实代价

- 三个"持久库"各只有**一张表**，却各自付一份 connect 前奏、一份 rusqlite→turso 迁移标记
  （`.turso` / `.sqlite-backup`）、一个 config url、`direct.rs` 里一个 `OnceCell`、TUI 与 gateway
  wiring 里各一行。
- 文档里维护一张"改哪个模型删哪个文件"的对照表，而它描述的操作已经不做了。
- **跨库没有事务。** cron job 与它的 grants 同库所以没事；但 bot-runtime PRD 里的 kanban Task ↔
  唤醒登记 ↔ 来源 session 三边分属 kanban.db / cron.db / state.db，只能顺序写加补偿。合库之后是一个
  `with_write_retry` 里的一个事务。

## 决定

- 一个文件 `~/.komo/komo.db`，一个 `Db` 类型持有全部模型。`KanbanDb` / `CronDb` / `MemoryDb`
  作为独立连接类型消失；各仓储 trait 的实现落在同一个 `Db` 上，`komo-infra` 之外看不到变化。
- config 只剩一个 `db_url`；`KOMO_HOME` 仍然决定它在哪。
- 耐久性规则改写为表级：
  - `memory_records` **只能加性变更**（原 memory.db 规则原样保留）。
  - `task_records`、`cron_job_records`、唤醒登记表、`pairing_records`、`reminder_records`、
    `setting_records` 是持久数据：schema 变更走 `ensure_*`，没有"删了重建"。
  - session 元数据、run 账本、inbox、todo 允许**行级**清理，操作面就是已有的 `komo run prune`、
    `komo sessions clean`——重置是删行，不是删文件。
- 不给 memory 留独立文件。唯一像样的理由是"跨机器备份时它是一个实体"，但它一旦独立就仍要保留
  整套 connect / 迁移 / url，而导出应由 `komo memory` 承担，不该依赖拷文件。

## 迁移

一次性，在 `Db::connect` 里：`komo.db` 不存在而旧文件存在时，把 kanban / cron / memory 三个文件的
行原样导入，再把旧文件改名为 `<name>.merged-backup`。这是仓库里已有的两种"首次连接导入"模式
（markdown 记忆 → memory.db、rusqlite → turso）的第三次使用，不是长期兼容层：导入代码在下一个
大版本删除。

state.db 也导——它虽被文档标为 disposable，但 pairing 与 reminder 丢了就要重新配对、重建提醒，
导入的成本远低于解释为什么它们没了。

## 要留意的

- **MVCC 冲突域变大。** Turso 是行级 MVCC，`with_write_retry` 已在每个写点，理论影响很小；
  合并后跑一遍 `cargo test --workspace`，重点看并发写的测试。
- **`push_schema` 只在新文件跑**这条约束没变，只是现在整个仓库只有一个新文件的时刻。任何新表都必须
  同时提供 `ensure_table` 的 DDL，并有一个测试锁住它与 `push_schema` 输出的一致性——`InboxRecord`
  已经是这样做的，照抄。
- 测试里那几十个 `komo_*_test.db` 文件名不受影响，它们本来就是每测一个临时库。

## 时机

放在 bot-runtime 第一批（持久化等待）之前。那一批要加唤醒登记表、重建 cron job 的 trigger 列、
并让 Task 与登记互相引用——三件事都碰库结构，先合库能让它们只碰一次，并且从第一天就在一个事务里。

## 回滚与重开条件

回滚就是不做：这是一个纯基础设施变更，不改任何 trait。重开拆分的条件只有一个——出现**必须**
独立于 gateway 进程被打开的数据（例如另一个长期进程要直接读 memory 表而不经 api channel）。
那时拆出去的也应该是那一张表，不是回到四个文件。
