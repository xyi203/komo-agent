//! Inbound message → standing wakes it fires (docs/bot-runtime.md §3.7).
//!
//! The thin shell around [`komo_core::domain::trigger`]: that decides whether a
//! filter is about the message, this reads the registrations, claims each hit
//! and wakes what it points at. Split that way because the deciding is pure and
//! the rest is three stores — and because §5.13's chat-triggered routine is the
//! same shell over the same matcher, differing only in what a hit turns into.
//!
//! **The message keeps its own route.** A trigger never redirects it: whoever
//! wrote is talking to komo on their own conversation, and that turn happens as
//! it always did. What a hit adds is a *second* turn, on the session the
//! commitment came from, saying what just arrived. Two conversations about one
//! message, which is what it is — no Task router, no message belonging to a
//! task (§6).
//!
//! **Claim before fire.** `take` answers `false` when the row is already gone,
//! so a message racing the expiry sweep wakes the turn once.
//!
//! The task's own status is **not** touched. Whether an arriving message counts
//! as the reply that discharges a commitment is a judgement, and the model
//! makes it in the turn this opens.

use std::sync::{Arc, RwLock};

use komo_core::domain::{
    session::ChannelPeer,
    session_event::WakeupCause,
    task::{Task, TaskRepository},
    trigger::{InboundEvent, matching},
    wakeup::{WakeupDispatch, WakeupRegistration, WakeupRepository},
};
use tracing::{info, warn};

pub struct TriggerMatcher {
    wakeups: Arc<dyn WakeupRepository>,
    tasks: Arc<dyn TaskRepository>,
    /// Whoever knows how to wake a turn. Attached after construction: it needs
    /// the dispatcher, and the dispatcher holds this. Absent ⇒ nothing fires.
    dispatch: RwLock<Option<Arc<dyn WakeupDispatch>>>,
}

impl TriggerMatcher {
    pub fn new(wakeups: Arc<dyn WakeupRepository>, tasks: Arc<dyn TaskRepository>) -> Self {
        Self {
            wakeups,
            tasks,
            dispatch: RwLock::new(None),
        }
    }

    /// Install who wakes a turn. Called once, during gateway wiring.
    pub fn attach_dispatch(&self, dispatch: Arc<dyn WakeupDispatch>) {
        *self.dispatch.write().unwrap() = Some(dispatch);
    }

    /// One inbound message. Fires every standing wake it matches, and answers
    /// how many.
    ///
    /// Best-effort throughout: a trigger store that cannot be read must never
    /// keep the message itself from being answered.
    pub async fn on_inbound(&self, peer: &ChannelPeer, text: &str) -> usize {
        let Some(dispatch) = self.dispatch.read().unwrap().clone() else {
            return 0;
        };
        let rows = match self.wakeups.list().await {
            Ok(rows) => rows,
            Err(error) => {
                warn!(%error, "could not read standing wakes for an inbound message");
                return 0;
            }
        };
        let event = InboundEvent { peer, text };
        let hits: Vec<WakeupRegistration> = matching(&rows, &event).into_iter().cloned().collect();

        let mut fired = 0;
        for registration in hits {
            match self.wakeups.take(&registration.id).await {
                Ok(true) => {}
                // Somebody else claimed it — a sweep expiring it at this
                // instant. Theirs to report.
                Ok(false) => continue,
                Err(error) => {
                    warn!(%error, wake = %registration.id, "could not claim a triggered wake");
                    continue;
                }
            }
            let payload = self.payload_for(&registration, text).await;
            match dispatch
                .fire(&registration, WakeupCause::Event, &payload)
                .await
            {
                Ok(()) => {
                    fired += 1;
                    info!(
                        wake = %registration.id,
                        session = %registration.session_id,
                        "a message woke a standing wait"
                    );
                }
                Err(error) => warn!(%error, wake = %registration.id, "failed to fire a wake"),
            }
        }
        fired
    }

    /// What the woken turn is handed.
    ///
    /// A wake a task registered opens a fresh turn, so it has to carry the
    /// whole situation — which commitment, who was being waited on, what they
    /// said. A wake a *turn* is parked on already has all of that in its own
    /// history, so it gets the message and nothing else.
    async fn payload_for(&self, registration: &WakeupRegistration, text: &str) -> String {
        if registration.turn_id.is_some() {
            return text.to_string();
        }
        match self.claim_task(&registration.id).await {
            Some(task) => waiting_task_prompt(&task, text),
            None => text.to_string(),
        }
    }

    /// The task this registration belonged to, with its now-spent id cleared.
    ///
    /// Clearing is what makes `komo task list` honest immediately: the wake is
    /// gone, so the task is no longer wakeable until something puts it back
    /// into `Waiting`.
    async fn claim_task(&self, wakeup_id: &str) -> Option<Task> {
        let mut task = match self.tasks.find_by_wakeup_id(wakeup_id).await {
            Ok(task) => task?,
            Err(error) => {
                warn!(%error, "could not read the task a wake belonged to");
                return None;
            }
        };
        task.wakeup_id = None;
        if let Err(error) = self.tasks.update(&task).await {
            warn!(%error, task = %task.id, "could not clear a task's spent wake");
        }
        Some(task)
    }
}

/// The message a commitment's wake opens its turn with.
fn waiting_task_prompt(task: &Task, text: &str) -> String {
    let who = match task.waiting_on.trim().is_empty() {
        false => task.waiting_on.trim().to_string(),
        true => task
            .waiting_on_peer
            .as_ref()
            .map(ChannelPeer::address)
            .unwrap_or_else(|| "对方".to_string()),
    };
    format!(
        "你在等 {who} 关于「{}」的回复，刚收到：{text}\n\n\
         这条是不是你等的回复由你判断：是就用 `task` 把它更新或完成，不是就继续等。",
        task.title
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::session_event::{EventFilter, Wakeup};
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

    #[derive(Default)]
    struct MemoryTasks {
        rows: Mutex<Vec<Task>>,
    }

    #[async_trait]
    impl TaskRepository for MemoryTasks {
        async fn save(&self, task: &Task) -> anyhow::Result<()> {
            self.rows.lock().unwrap().push(task.clone());
            Ok(())
        }
        async fn find(&self, id: &str) -> anyhow::Result<Option<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|t| t.id == id)
                .cloned())
        }
        async fn list_open(&self) -> anyhow::Result<Vec<Task>> {
            Ok(self.rows.lock().unwrap().clone())
        }
        async fn update(&self, task: &Task) -> anyhow::Result<()> {
            let mut rows = self.rows.lock().unwrap();
            if let Some(slot) = rows.iter_mut().find(|t| t.id == task.id) {
                *slot = task.clone();
            }
            Ok(())
        }
        async fn find_by_source_message_id(
            &self,
            _source: &str,
            _key: &str,
        ) -> anyhow::Result<Option<Task>> {
            Ok(None)
        }
        async fn find_by_wakeup_id(&self, wakeup_id: &str) -> anyhow::Result<Option<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|t| t.wakeup_id.as_deref() == Some(wakeup_id))
                .cloned())
        }
    }

    #[derive(Default)]
    struct RecordingDispatch {
        fired: Mutex<Vec<(String, WakeupCause, String)>>,
    }

    #[async_trait]
    impl WakeupDispatch for RecordingDispatch {
        async fn fire(
            &self,
            registration: &WakeupRegistration,
            cause: WakeupCause,
            payload: &str,
        ) -> anyhow::Result<()> {
            self.fired.lock().unwrap().push((
                registration.session_id.clone(),
                cause,
                payload.to_string(),
            ));
            Ok(())
        }
    }

    fn from_peer(platform: &str, peer_id: &str) -> Wakeup {
        Wakeup::Event {
            filter: EventFilter::FromPeer {
                platform: platform.into(),
                peer_id: peer_id.into(),
            },
        }
    }

    fn matcher(
        wakeups: &Arc<MemoryWakeups>,
        tasks: &Arc<MemoryTasks>,
    ) -> (TriggerMatcher, Arc<RecordingDispatch>) {
        let dispatch = Arc::new(RecordingDispatch::default());
        let matcher = TriggerMatcher::new(wakeups.clone(), tasks.clone());
        matcher.attach_dispatch(dispatch.clone());
        (matcher, dispatch)
    }

    #[tokio::test]
    async fn a_waiting_tasks_peer_opens_a_turn_that_names_the_commitment() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let tasks = Arc::new(MemoryTasks::default());
        let registration = WakeupRegistration::new("s1", from_peer("feishu", "ou_x"), 1_000);
        wakeups.save(&registration).await.unwrap();

        let mut task = Task::new("等张三的方案".into());
        task.status = TaskStatus::Waiting;
        task.waiting_on = "张三".into();
        task.waiting_on_peer = Some(ChannelPeer::new("feishu", "ou_x"));
        task.wakeup_id = Some(registration.id.clone());
        tasks.save(&task).await.unwrap();

        let (matcher, dispatch) = matcher(&wakeups, &tasks);
        let fired = matcher
            .on_inbound(&ChannelPeer::new("feishu", "ou_x"), "方案发你了")
            .await;

        assert_eq!(fired, 1);
        let calls = dispatch.fired.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "s1");
        assert_eq!(calls[0].1, WakeupCause::Event);
        assert!(calls[0].2.contains("方案发你了"), "{}", calls[0].2);
        assert!(calls[0].2.contains("等张三的方案"), "{}", calls[0].2);

        // Claimed, and the task no longer claims to be watched.
        assert!(wakeups.list().await.unwrap().is_empty());
        let stored = tasks.find(&task.id).await.unwrap().unwrap();
        assert_eq!(stored.wakeup_id, None);
        assert_eq!(
            stored.status,
            TaskStatus::Waiting,
            "whether that was the reply is the model's call, not the matcher's"
        );
    }

    /// The registration is the whole gate: once the task is done and its wake
    /// retired, the same person writing again is just a message.
    #[tokio::test]
    async fn a_peer_nobody_is_waiting_on_fires_nothing() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let tasks = Arc::new(MemoryTasks::default());
        let (matcher, dispatch) = matcher(&wakeups, &tasks);
        assert_eq!(
            matcher
                .on_inbound(&ChannelPeer::new("feishu", "ou_x"), "在么")
                .await,
            0
        );
        assert!(dispatch.fired.lock().unwrap().is_empty());
    }

    /// A turn parked on `wait { for_task }` has the commitment in its own
    /// history already; it gets the message and nothing more.
    #[tokio::test]
    async fn a_suspended_turn_is_handed_the_message_itself() {
        let wakeups = Arc::new(MemoryWakeups::default());
        let tasks = Arc::new(MemoryTasks::default());
        wakeups
            .save(
                &WakeupRegistration::new("s1", from_peer("feishu", "ou_x"), 1_000).continuing("t1"),
            )
            .await
            .unwrap();

        let (matcher, dispatch) = matcher(&wakeups, &tasks);
        matcher
            .on_inbound(&ChannelPeer::new("feishu", "ou_x"), "好了")
            .await;
        assert_eq!(dispatch.fired.lock().unwrap()[0].2, "好了");
    }
}
