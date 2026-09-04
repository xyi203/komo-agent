//! The file ingress for event-triggered routines (docs/bot-runtime.md §5.14).
//!
//! A `Channel` rather than a sweep, because what it does is *wait*: `notify`
//! gives it a stream from the OS (FSEvents / inotify), and the gateway already
//! has one shape for "a long-running thing that turns arrivals into turns and
//! stops when the process does". It happens not to carry messages, which is why
//! it sits beside `messaging/` rather than in it.
//!
//! Three properties it exists for:
//!
//! - **Debounced.** Saving fifty files is one thing happening. Paths arriving
//!   inside [`DEBOUNCE`] of each other are collected into a single
//!   [`ExternalEvent::FileChanged`], so a batch write fires a routine once —
//!   with the batch's size and first path on the run record, not fifty runs
//!   nobody can read.
//! - **Deduplicated by root.** Two routines watching one directory need one
//!   watch. Globs are not part of the watch at all: filtering is
//!   `Trigger::matched_by`'s, so the routine that fires is decided by exactly
//!   the same code whichever ingress the event came from.
//! - **Re-read on a timer.** A routine added, removed or paused mid-run changes
//!   what is watched within [`RESCAN`], with no restart. Cheap because it is a
//!   list of jobs already in memory-speed storage, and re-registering a watch
//!   is idempotent.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use notify::{EventKind, RecursiveMode, Watcher};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use komo_agent::daemon::RoutineEventSource;
use komo_agent::gateway::Channel;
use komo_agent::interaction::GatewayDispatcher;
use komo_core::domain::cron::{CronJobStatus, Trigger};

/// How long the watcher waits for quiet before it calls a burst one event.
/// Two seconds is the PRD's figure: long enough to cover a `git checkout` or a
/// formatter rewriting a tree, short enough that a single save still feels
/// immediate.
const DEBOUNCE: Duration = Duration::from_secs(2);

/// How often the watched set is reconciled against the routines. A minute is
/// the cron sweep's own cadence — the same promise, that a job edited now takes
/// effect without a restart.
const RESCAN: Duration = Duration::from_secs(60);

/// How many paths one batched event carries. A `cargo build` touches thousands;
/// the routine needs to know that something changed and roughly what, not to be
/// handed the whole tree.
const MAX_BATCH_PATHS: usize = 500;

pub struct FileWatcher {
    routines: Arc<RoutineEventSource>,
}

impl FileWatcher {
    pub fn new(routines: Arc<RoutineEventSource>) -> Self {
        Self { routines }
    }
}

#[async_trait]
impl Channel for FileWatcher {
    fn name(&self) -> &str {
        "files"
    }

    /// Ignores the dispatcher: this ingress holds the routine source directly
    /// (it is built alongside it in the gateway's own wiring), and unlike a chat
    /// channel it never opens a conversation.
    async fn serve(
        &self,
        _dispatcher: Arc<GatewayDispatcher>,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.watch(shutdown).await
    }
}

impl FileWatcher {
    /// The loop itself, separate from the `Channel` shape so it can be run
    /// against a real directory without a gateway around it.
    async fn watch(&self, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel::<PathBuf>();
        // The OS callback runs on notify's own thread and must not block: it
        // does nothing but hand the path over.
        let mut watcher = match notify::recommended_watcher(move |result| match result {
            Ok(notify::Event { kind, paths, .. }) if is_content_change(&kind) => {
                for path in paths {
                    let _ = tx.send(path);
                }
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "file watch error"),
        }) {
            Ok(watcher) => watcher,
            // A gateway that cannot watch files is still a gateway. Every other
            // routine keeps working; this one says why it does not.
            Err(error) => {
                warn!(%error, "file-changed routines are disabled: no filesystem watcher");
                return Ok(());
            }
        };

        let mut watched: BTreeSet<PathBuf> = BTreeSet::new();
        let mut pending: Vec<PathBuf> = Vec::new();
        let mut rescan = tokio::time::interval(RESCAN);
        loop {
            // A pending batch is what turns the select into a debounce: while
            // paths are held, the timer arm is armed, and every further path
            // pushes the deadline back by resetting it.
            let quiet = tokio::time::sleep(DEBOUNCE);
            tokio::pin!(quiet);
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = rescan.tick() => {
                    self.reconcile(&mut watcher, &mut watched).await;
                }
                path = rx.recv() => match path {
                    Some(path) => {
                        if pending.len() < MAX_BATCH_PATHS {
                            pending.push(path);
                        }
                        continue;
                    }
                    // The watcher was dropped; nothing more will arrive.
                    None => break,
                },
                _ = &mut quiet, if !pending.is_empty() => {
                    let paths = std::mem::take(&mut pending);
                    let event = komo_core::domain::trigger::ExternalEvent::FileChanged { paths };
                    // Spawned: a routine's turn is an agent turn, and this loop
                    // has to keep debouncing the writes still arriving.
                    let routines = self.routines.clone();
                    tokio::spawn(async move {
                        let fired = routines.on_event(&event).await;
                        if fired.routines > 0 {
                            info!(routines = fired.routines, "a file change fired routines");
                        }
                    });
                }
            }
        }
        info!("file watcher stopped");
        Ok(())
    }

    /// Bring the watched set in line with the active `FileChanged` routines.
    ///
    /// Adding only: a root that stops being watched costs one idle OS watch
    /// until the next restart, and its events match no routine anyway — whereas
    /// unwatching a root two routines share would silence the other one.
    async fn reconcile<W: Watcher>(&self, watcher: &mut W, watched: &mut BTreeSet<PathBuf>) {
        let jobs = match self.routines.jobs.list().await {
            Ok(jobs) => jobs,
            Err(error) => {
                warn!(%error, "could not read routines to update the file watches");
                return;
            }
        };
        let mut roots = BTreeSet::new();
        for job in jobs.iter().filter(|j| j.status == CronJobStatus::Active) {
            collect_roots(&job.trigger, &mut roots);
        }
        for root in roots {
            if watched.contains(&root) {
                continue;
            }
            match watcher.watch(&root, RecursiveMode::Recursive) {
                Ok(()) => {
                    info!(root = %root.display(), "watching a directory for a routine");
                    watched.insert(root);
                }
                // The directory was canonicalized and proven to exist when the
                // routine was created; it can still be renamed or unmounted
                // afterwards. Retried on the next rescan.
                Err(error) => {
                    warn!(%error, root = %root.display(), "could not watch a routine's directory")
                }
            }
        }
    }
}

fn collect_roots(trigger: &Trigger, into: &mut BTreeSet<PathBuf>) {
    match trigger {
        Trigger::FileChanged { root, .. } => {
            into.insert(root.clone());
        }
        Trigger::Any { triggers } => {
            for member in triggers {
                collect_roots(member, into);
            }
        }
        _ => {}
    }
}

/// Whether this OS event changed a file rather than merely touched it.
///
/// Stated as "everything but a read" rather than as a list of the three kinds
/// that count: what a platform reports a write as varies (FSEvents coalesces,
/// inotify splits, and both fall back to `Any`), and a rule written as a
/// whitelist would silently stop firing on whichever platform names things
/// differently. A read is the one thing that is unambiguously not a change.
fn is_content_change(kind: &EventKind) -> bool {
    !matches!(kind, EventKind::Access(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::cron::{
        CronAction, CronJob, CronJobRepository, FeishuMatch, RoutineRunStatus,
    };
    use std::sync::Mutex;

    #[test]
    fn every_watched_root_is_collected_once() {
        let mut roots = BTreeSet::new();
        collect_roots(
            &Trigger::Any {
                triggers: vec![
                    Trigger::FileChanged {
                        root: "/srv/notes".into(),
                        glob: "**/*.md".into(),
                    },
                    Trigger::FileChanged {
                        root: "/srv/notes".into(),
                        glob: "**/*.txt".into(),
                    },
                    Trigger::Feishu {
                        chat: "oc_x".into(),
                        matcher: FeishuMatch::Mention,
                    },
                    Trigger::cron("0 8 * * *"),
                ],
            },
            &mut roots,
        );
        // Two routines on one directory are one watch; the members that watch
        // no directory contribute none.
        assert_eq!(roots, BTreeSet::from([PathBuf::from("/srv/notes")]));
    }

    #[test]
    fn only_content_changes_reach_a_routine() {
        use notify::event::{AccessKind, CreateKind, ModifyKind, RemoveKind};
        assert!(is_content_change(&EventKind::Create(CreateKind::File)));
        assert!(is_content_change(&EventKind::Modify(ModifyKind::Any)));
        assert!(is_content_change(&EventKind::Remove(RemoveKind::File)));
        // What a platform cannot classify still counts as a change; only a read
        // is definitely not one.
        assert!(is_content_change(&EventKind::Any));
        assert!(!is_content_change(&EventKind::Access(AccessKind::Read)));
    }

    #[derive(Default)]
    struct MemoryJobs {
        jobs: Mutex<Vec<CronJob>>,
    }

    #[async_trait]
    impl CronJobRepository for MemoryJobs {
        async fn save(&self, job: &CronJob) -> anyhow::Result<()> {
            self.jobs.lock().unwrap().push(job.clone());
            Ok(())
        }
        async fn list(&self) -> anyhow::Result<Vec<CronJob>> {
            Ok(self.jobs.lock().unwrap().clone())
        }
        async fn find_by_name(&self, name: &str) -> anyhow::Result<Option<CronJob>> {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .iter()
                .find(|j| j.name == name)
                .cloned())
        }
        async fn update(&self, job: &CronJob) -> anyhow::Result<()> {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(slot) = jobs.iter_mut().find(|j| j.id == job.id) {
                *slot = job.clone();
            }
            Ok(())
        }
        async fn delete(&self, _name: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    struct SilentNotifier;

    #[async_trait]
    impl komo_core::domain::notify::Notifier for SilentNotifier {
        async fn notify(&self, _title: &str, _body: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A routine turn that just answers, so the run settles `ok`.
    struct EchoRuntime;

    #[async_trait]
    impl komo_core::domain::gateway::MessageHandler for EchoRuntime {
        async fn handle(&self, _session: &str, _message: String) -> anyhow::Result<String> {
            Ok("reindexed".to_string())
        }
    }

    /// The headline of §5.14, against the real OS watcher: fifty files written
    /// inside one debounce window fire the routine **once**, and a file the
    /// glob does not name fires it not at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn fifty_writes_in_one_window_fire_the_routine_once() {
        let root = std::env::temp_dir().join(format!("komo_filewatch_{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        // Canonical, as a stored trigger's root always is: on macOS the temp
        // directory is a symlink, and the paths the OS reports are the real
        // ones — a root that did not match them would match nothing.
        let root = root.canonicalize().unwrap();

        let jobs = Arc::new(MemoryJobs::default());
        jobs.save(&CronJob::new(
            "reindex",
            Trigger::FileChanged {
                root: root.clone(),
                glob: "**/*.md".into(),
            },
            CronAction::Agent {
                prompt: "重建索引".into(),
                skills: vec![],
                workspace: None,
            },
            0,
        ))
        .await
        .unwrap();

        let routines = Arc::new(RoutineEventSource {
            jobs: jobs.clone(),
            notifier: Arc::new(SilentNotifier),
            runtime: Some(Arc::new(EchoRuntime)),
            wakeups: None,
            triggers: None,
        });
        let (stop_tx, stop_rx) = watch::channel(false);
        let watcher = FileWatcher::new(routines);
        let handle = tokio::spawn(async move { watcher.watch(stop_rx).await });
        // The first reconcile is on the interval's immediate first tick.
        tokio::time::sleep(Duration::from_millis(500)).await;

        for i in 0..50 {
            std::fs::write(root.join(format!("note-{i}.md")), "hi").unwrap();
        }
        // Two files the glob does not name, in the same window.
        std::fs::write(root.join("shot.png"), "x").unwrap();

        tokio::time::sleep(DEBOUNCE + Duration::from_secs(2)).await;
        let stored = jobs.list().await.unwrap();
        let runs = &stored[0].runs;
        assert_eq!(
            runs.len(),
            1,
            "fifty writes are one thing happening: {:?}",
            runs.iter().map(|r| &r.event).collect::<Vec<_>>()
        );
        assert_eq!(runs[0].status, RoutineRunStatus::Ok);
        assert!(runs[0].event.contains("文件变更"), "{}", runs[0].event);

        // A window with nothing the glob names leaves the history alone.
        std::fs::write(root.join("другой.png"), "x").unwrap();
        tokio::time::sleep(DEBOUNCE + Duration::from_secs(1)).await;
        assert_eq!(jobs.list().await.unwrap()[0].runs.len(), 1);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        let _ = std::fs::remove_dir_all(&root);
    }
}
