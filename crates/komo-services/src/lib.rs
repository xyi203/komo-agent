//! The services the tool loop is built out of, none of which knows about the
//! agent above it.
//!
//! `tool_execution` is the centre: `ToolExecutor::execute_round` runs a round's
//! calls against `komo-core`'s `Tool` trait, so it needs no concrete tool and no
//! runtime. Around it sit the pieces tools and the loop share — output bounding,
//! memory enrichment, the clarify sentinel, the skill registry, and the
//! file/search primitives the fs tools are assembled from.
//!
//! `operator_control` stayed in the binary: it reaches up into `agent::daemon`
//! and out to the gateway client, so it is wiring rather than a service.

pub mod checkpoint_store;
pub mod clarify;
pub mod cron_actions;
pub mod diff;
pub mod episode;
pub mod file_mutation;
pub mod memory_consolidation;
pub mod memory_enrichment;
pub mod memory_query;
pub mod patch;
pub mod search;
pub mod session_indexing;
pub mod skill_registry;
pub mod tool_execution;
pub mod tool_output_store;
pub mod wiki_chunking;
pub mod wiki_indexing;
