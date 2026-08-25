mod app;

/// The version string every surface reports — see [`app::VERSION`].
pub use app::VERSION;
mod approver;
mod channel;
mod doctor;
mod dream;
mod gateway;
mod health;
mod init;
mod inspect;
mod journey;
mod logs;
mod memory;
mod model;
mod pair;
mod policy;
mod resume;
pub mod rollback;
mod service;
mod skill;
mod upgrade;
mod wechat;
mod wiki;
pub(crate) mod wiring;
mod workday;

pub use app::run;
