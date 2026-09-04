//! A kanban Task's standing wake (docs/bot-runtime.md §3.7).
//!
//! `Waiting` used to be a label. It is now a label *and* a registration: a task
//! that names who it waits on, as an address rather than a name, registers one
//! `Event{FromPeer}` wake, and the person's next message brings the commitment
//! back on the session it came from.
//!
//! Everything that can move a task into or out of `Waiting` comes through
//! [`TaskWaiting::sync`] — one function, because "register when it enters,
//! retire when it leaves" written once per write site is three chances to leak
//! a registration nobody will ever retire.
//!
//! The registration is deliberately **not** a mirror of the row: it says
//! nothing about the task beyond who to listen for, and the task holds only its
//! id. Which of the two is authoritative never comes up — the task decides
//! whether a wake *should* stand, and `sync` makes the store agree.

use std::sync::Arc;

use komo_core::domain::{
    home::HomeRepository,
    session_event::{EventFilter, Wakeup},
    task::Task,
    wakeup::{WakeupRegistration, WakeupRepository},
};

/// How long a task's wake stands when the task names no deadline of its own.
/// The generic `Event` lifetime: long enough that a commitment outlives the
/// conversation that made it, short enough that a forgotten one does not stand
/// forever.
pub const TASK_WAIT_DEFAULT_EXPIRY_SECS: i64 = 30 * 86_400;

pub struct TaskWaiting {
    wakeups: Arc<dyn WakeupRepository>,
    /// Where a task with no source session is woken. A commitment captured
    /// outside any conversation still belongs to the operator, and the home
    /// conversation is where the operator is.
    home: Arc<dyn HomeRepository>,
}

impl TaskWaiting {
    pub fn new(wakeups: Arc<dyn WakeupRepository>, home: Arc<dyn HomeRepository>) -> Self {
        Self { wakeups, home }
    }

    /// Bring the store's standing wake in line with what `task` now says.
    ///
    /// Mutates `task.wakeup_id`; the caller writes the row afterwards, so one
    /// task change is still one write. Best-effort by design at the *edges* —
    /// an unreachable registration store must not stop the user from marking a
    /// task done — but the id is only written back when the row actually
    /// landed.
    pub async fn sync(&self, task: &mut Task, now: i64) -> anyhow::Result<()> {
        let wanted = task.is_wakeable().then(|| filter_for(task)).flatten();
        match (wanted, task.wakeup_id.clone()) {
            (None, None) => Ok(()),
            // It left `Waiting`, or lost the address that made it wakeable.
            (None, Some(id)) => {
                self.wakeups.take(&id).await?;
                task.wakeup_id = None;
                Ok(())
            }
            (Some(filter), None) => self.register(task, filter, now).await,
            (Some(filter), Some(id)) => {
                // Still waiting, but possibly on somebody else now — and a
                // registration the store no longer holds (fired, expired) has
                // to be replaced or the task is silently unwakeable.
                if self.standing(&id, &filter).await {
                    return Ok(());
                }
                self.wakeups.take(&id).await?;
                task.wakeup_id = None;
                self.register(task, filter, now).await
            }
        }
    }

    async fn register(&self, task: &mut Task, filter: EventFilter, now: i64) -> anyhow::Result<()> {
        let session_id = match task.source.is_empty() {
            false => task.source.clone(),
            true => self.home.home_session().await?,
        };
        // No turn to continue: a reply arriving days later opens a turn of its
        // own, carrying what was said. `due_at` is the task's own deadline, so
        // it is the wake's — past it the task is late, not waiting.
        let registration = WakeupRegistration::new(session_id, Wakeup::Event { filter }, now)
            .expiring_at(Some(
                task.due_at.unwrap_or(now + TASK_WAIT_DEFAULT_EXPIRY_SECS),
            ));
        self.wakeups.save(&registration).await?;
        task.wakeup_id = Some(registration.id);
        Ok(())
    }

    /// Whether `id` is still registered and still listening for `filter`.
    async fn standing(&self, id: &str, filter: &EventFilter) -> bool {
        let Ok(rows) = self.wakeups.list().await else {
            return true;
        };
        rows.iter()
            .any(|r| r.id == id && matches!(&r.wakeup, Wakeup::Event { filter: f } if f == filter))
    }
}

/// The address to listen for, or `None` when the task carries only a name.
fn filter_for(task: &Task) -> Option<EventFilter> {
    task.waiting_on_peer
        .as_ref()
        .map(|peer| EventFilter::FromPeer {
            platform: peer.platform.clone(),
            peer_id: peer.peer_id.clone(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::session::ChannelPeer;
    use komo_core::domain::task::TaskStatus;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryWakeups {
        rows: Mutex<Vec<WakeupRegistration>>,
    }

    #[async_trait]
    impl WakeupRepository for MemoryWakeups {
        async fn save(&self, registration: &WakeupRegistration) -> anyhow::Result<()> {
            self.rows.lock().unwrap().push(registration.clone());
            Ok(())
        }
        async fn list(&self) -> anyhow::Result<Vec<WakeupRegistration>> {
            Ok(self.rows.lock().unwrap().clone())
        }
        async fn take(&self, id: &str) -> anyhow::Result<bool> {
            let mut rows = self.rows.lock().unwrap();
            let before = rows.len();
            rows.retain(|r| r.id != id);
            Ok(rows.len() != before)
        }
        async fn take_for_turn(&self, _session: &str, _turn: &str) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    struct MemoryHome;

    #[async_trait]
    impl HomeRepository for MemoryHome {
        async fn get(&self) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        async fn set(&self, _address: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn home_session(&self) -> anyhow::Result<String> {
            Ok("home-session".to_string())
        }
    }

    fn waiting_task() -> Task {
        let mut task = Task::new("等张三的方案".into());
        task.status = TaskStatus::Waiting;
        task.waiting_on = "张三".into();
        task.waiting_on_peer = Some(ChannelPeer::new("feishu", "ou_x"));
        task.source = "s1".into();
        task
    }

    fn service(wakeups: &Arc<MemoryWakeups>) -> TaskWaiting {
        TaskWaiting::new(wakeups.clone(), Arc::new(MemoryHome))
    }

    #[tokio::test]
    async fn waiting_on_a_peer_registers_a_wake_and_leaving_retires_it() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();

        let id = task.wakeup_id.clone().expect("the task holds its wake");
        let rows = wakeups.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, id);
        assert_eq!(rows[0].session_id, "s1");
        assert_eq!(rows[0].turn_id, None, "a reply opens a turn of its own");
        assert_eq!(
            rows[0].wakeup,
            Wakeup::Event {
                filter: EventFilter::FromPeer {
                    platform: "feishu".into(),
                    peer_id: "ou_x".into()
                }
            }
        );
        assert_eq!(
            rows[0].expires_at,
            Some(1_000 + TASK_WAIT_DEFAULT_EXPIRY_SECS)
        );

        task.status = TaskStatus::Done;
        service(&wakeups).sync(&mut task, 2_000).await.unwrap();
        assert_eq!(task.wakeup_id, None);
        assert!(wakeups.list().await.unwrap().is_empty());
    }

    /// A name is not an address. The task still says who it waits on, and
    /// nothing listens — which is the honest answer, not a guess.
    #[tokio::test]
    async fn a_task_naming_only_a_person_registers_nothing() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        task.waiting_on_peer = None;
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();
        assert_eq!(task.wakeup_id, None);
        assert!(!task.is_wakeable());
        assert!(wakeups.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_task_with_no_source_session_waits_on_the_home_conversation() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        task.source = String::new();
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();
        assert_eq!(wakeups.list().await.unwrap()[0].session_id, "home-session");
    }

    /// Twice in a row is once: syncing a task that already holds a matching
    /// registration must not leave a second one behind for the same reply.
    #[tokio::test]
    async fn syncing_an_unchanged_wait_leaves_one_registration() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();
        let id = task.wakeup_id.clone().unwrap();
        service(&wakeups).sync(&mut task, 1_100).await.unwrap();
        assert_eq!(task.wakeup_id, Some(id));
        assert_eq!(wakeups.list().await.unwrap().len(), 1);
    }

    /// Waiting on somebody else is a different wait: the old one has to go, or
    /// the first person's next message wakes a commitment that is not theirs.
    #[tokio::test]
    async fn changing_who_it_waits_on_replaces_the_wake() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();
        let first = task.wakeup_id.clone().unwrap();

        task.waiting_on_peer = Some(ChannelPeer::new("telegram", "42"));
        service(&wakeups).sync(&mut task, 1_100).await.unwrap();

        let rows = wakeups.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(task.wakeup_id, Some(first));
        assert_eq!(rows[0].id, task.wakeup_id.clone().unwrap());
    }

    /// A registration that already fired leaves the task holding a spent id.
    /// Re-entering `Waiting` has to register again, or the commitment is
    /// silently unwakeable from then on.
    #[tokio::test]
    async fn a_spent_registration_is_replaced_rather_than_trusted() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();
        let fired = task.wakeup_id.clone().unwrap();
        wakeups.take(&fired).await.unwrap();

        service(&wakeups).sync(&mut task, 1_200).await.unwrap();
        assert_ne!(task.wakeup_id, Some(fired));
        assert_eq!(wakeups.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_due_date_is_the_wakes_deadline() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let mut task = waiting_task();
        task.due_at = Some(5_000);
        service(&wakeups).sync(&mut task, 1_000).await.unwrap();
        assert_eq!(wakeups.list().await.unwrap()[0].expires_at, Some(5_000));
    }
}
