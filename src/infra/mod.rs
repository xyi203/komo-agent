//! What is left of `infra/` after the dependency-light half moved to
//! `komo-infra`: the pieces that reach *upward* into the agent and the services,
//! so they cannot live below them.
//!
//! - `messaging` hosts the chat channels, which dispatch real turns.
//! - `gateway_client` speaks the operator-control vocabulary.
//! - `file_watcher` is the third routine-event ingress (§5.14) — a `Channel`
//!   like the chat ones, but beside `messaging` rather than in it: it carries
//!   no messages and opens no conversation.
pub mod file_watcher;
pub mod gateway_client;
pub mod rendezvous;

pub mod messaging;
