//! The agent itself: the tool loop, the gateway that hosts turns, the scheduled
//! sweeps, and the provider client the loop drives.
//!
//! This is the top of the dependency graph below the binary. It depends on the
//! `Tool` trait rather than on any concrete tool, which is why `komo-tools`
//! compiles beside it rather than ahead of it — the two only meet in
//! `cli/wiring.rs`, where the catalog is registered.
//!
//! `llm` and `delegate` live here rather than with their old neighbours because
//! both assemble a *turn*: `llm::assemble` builds the tiered system prompt
//! around `system_prompt`, and a delegation runs a real agent turn on an
//! `AgentRuntime`.

pub mod auto_reviewer;
pub mod compaction;
pub mod daemon;
pub mod delegate;
pub mod feedback;
pub mod gateway;
pub mod interaction;
pub mod learning_coordinator;
pub mod llm;
pub mod pairing;
pub mod policy_approver;
pub mod reviewer;
pub mod runtime;
pub mod system_prompt;
