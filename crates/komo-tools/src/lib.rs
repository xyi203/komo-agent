//! Every tool the agent can call, over `komo-core`'s `Tool` trait.
//!
//! Its own crate for two reasons: nothing in the agent references a concrete
//! tool (the loop dispatches through `ToolExecutor`), and this is the half of the
//! old bin crate that changes most often — so it compiles in parallel with the
//! agent instead of ahead of it.
//!
//! `delegate` is the exception and stayed in the binary: a delegation *is* a
//! real agent turn, so that tool holds an `AgentRuntime`.

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub mod apply_patch;
pub mod ask_user;
pub mod cron;
pub mod edit;
pub mod fs_common;
pub mod glob;
pub mod grep;
pub mod homeassistant;
pub mod http;
pub mod logs;
pub mod mcp;
pub mod memory;
pub mod plugin;
pub mod read;
pub mod reminder;
pub mod run_code;
pub mod session;
pub mod shell;
pub mod skill;
pub mod task;
pub mod time;
pub mod todo;
pub mod wait;
pub mod web_fetch;
pub mod web_search;
pub mod wiki_index;
pub mod wiki_read;
pub mod wiki_search;
pub mod write;
