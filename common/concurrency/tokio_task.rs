/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{future::Future};

use tokio::{
    time::{self, Duration},
};
use tokio::sync::oneshot;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

/// Manages background tasks that should finish gracefully on shutdown.
///
/// # Design
/// - A cloneable [`TokioTaskSpawner`] is handed to lower-level components to spawn tasks.
/// - The tracker listens to a global `watch::Receiver<()>` shutdown signal.
/// - When shutdown is observed, the tracker:
///   1. `close()`s to stop accepting new tasks;
///   2. `wait()`s until all tracked tasks finish.
///
/// # Guarantees
/// - Tasks spawned through the spawner are kept alive by the Tokio runtime as usual.
/// - **To guarantee** that long-running tasks (e.g., intervals) complete their final
///   work on shutdown (like “act_on_shutdown”), the owner **must call** [`TokioTaskTracker::join`]
///   before letting the runtime drop.
///
/// This avoids blocking in `Drop` (which can deadlock a Tokio worker) while still
/// giving you a single place to wait for all background work to finish.
/// For more information, see https://tokio.rs/tokio/topics/shutdown.
#[derive(Debug)]
pub struct TokioTaskTracker {
    tracker: TaskTracker,
    shutdown_receiver: Receiver<()>,
    done_receiver: oneshot::Receiver<()>,
    shutdown_task: JoinHandle<()>,
}

impl TokioTaskTracker {
    /// Create a new `TokioTaskTracker` wired to the provided shutdown receiver.
    pub fn new(shutdown_receiver: Receiver<()>) -> Self {
        let tracker = TaskTracker::new();
        let (done_sender, done_receiver) = oneshot::channel();

        let mut shutdown_receiver_clone = shutdown_receiver.clone();
        let tracker_clone = tracker.clone();
        let shutdown_task = tokio::spawn(async move {
            let _ = shutdown_receiver_clone.changed().await;

            tracker_clone.close();
            tracker_clone.wait().await;
            let _ = done_sender.send(());
        });

        Self { tracker, done_receiver, shutdown_receiver, shutdown_task }
    }

    /// Get a cloneable spawner tied to this tracker and shutdown stream.
    pub fn get_spawner(&self) -> TokioTaskSpawner {
        TokioTaskSpawner {
            tracker: self.tracker.clone(),
            shutdown_receiver: self.shutdown_receiver.clone(),
        }
    }

    /// Wait for the shutdown supervisor to complete and for **all** tracked tasks to finish.
    ///
    /// Call this near the end of your application's workflow, **before** the Tokio
    /// runtime is dropped, to guarantee that finalizers have completed.
    pub async fn join(self) {
        let _ = self.shutdown_task.await;
        let _ = self.done_receiver.await;
    }
}

/// Cloneable spawner for background tasks. Tasks are tracked in the parent
/// [`TokioTaskTracker`], and can react to the same shutdown receiver.
#[derive(Debug, Clone)]
pub struct TokioTaskSpawner {
    tracker: TaskTracker,
    shutdown_receiver: Receiver<()>,
}

impl TokioTaskSpawner {
    /// Spawn an arbitrary background task and track it. The return handle can be used to await this
    /// single task's completion, but also can be ignored.
    pub fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tracker.spawn(task)
    }

    /// Spawn a periodic task that runs `action()` every `interval` until a shutdown signal is received.
    /// The return handle can be used to await this single task's completion (only on shutdown), but also can be ignored.
    pub fn spawn_interval<F>(&self, mut action: impl 'static + Send + FnMut() -> F, parameters: IntervalTaskParameters) -> JoinHandle<F::Output>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut shutdown_receiver = self.shutdown_receiver.clone();
        let IntervalTaskParameters { interval, initial_delay, act_on_shutdown } = parameters;

        self.spawn(async move {
            if !initial_delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(initial_delay) => (),
                    _ = shutdown_receiver.changed() => {
                        drop(action);
                        return;
                    }
                }
            }
            let mut interval_timer = time::interval(interval);
            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        action().await;
                    }
                    _ = shutdown_receiver.changed() => {
                        if act_on_shutdown {
                            action().await;
                        }
                        drop(action);
                        break;
                    }
                }
            }
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub struct IntervalTaskParameters {
    /// The interval between consecutive executions of the `action()`.
    pub interval: Duration,

    /// The initial delay before the first execution of the `action()` after spawning.
    ///
    /// If the shutdown signal is received during this delay, the task is cancelled
    /// immediately and **will not** execute the first action (or the shutdown action).
    pub initial_delay: Duration,

    /// Whether to run the `action()` **one final time** when a shutdown signal is received.
    ///
    /// This is useful for performing final cleanup or state flushes before exit.
    pub act_on_shutdown: bool,
}

impl IntervalTaskParameters {
    pub fn new_no_delay(interval: Duration, act_on_shutdown: bool) -> Self {
        Self::new_with_delay(interval, Duration::ZERO, act_on_shutdown)
    }

    pub fn new_with_delay(interval: Duration, initial_delay: Duration, act_on_shutdown: bool) -> Self {
        Self {
            interval,
            act_on_shutdown,
            initial_delay,
        }
    }
}

//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::{
//         atomic::{AtomicUsize, Ordering},
//         Arc,
//     };
//     use tokio::sync::watch;
//     use tokio::time::{self, advance, pause, Duration};
//
//     fn setup() -> (TokioTaskTracker, TokioTaskSpawner, watch::Sender<()>) {
//         let (tx, rx) = watch::channel(());
//         let tracker = TokioTaskTracker::new(rx);
//         let spawner = tracker.get_spawner();
//         (tracker, spawner, tx)
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn spawn_returns_handle_and_is_tracked() {
//         pause();
//         let (tracker, spawner, tx) = setup();
//
//         let flag = Arc::new(AtomicUsize::new(0));
//         let flag2 = flag.clone();
//
//         let h = spawner.spawn(async move {
//             time::sleep(Duration::from_secs(2)).await;
//             flag2.store(1, Ordering::SeqCst);
//         });
//
//         // Awaiting the handle should complete the task
//         advance(Duration::from_secs(2)).await;
//         h.await.unwrap();
//         assert_eq!(flag.load(Ordering::SeqCst), 1);
//
//         // Shutdown and ensure tracker join still finishes (even though the task already ended)
//         let _ = tx.send(());
//         tracker.join().await;
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn tracked_even_if_handle_dropped() {
//         pause();
//         let (tracker, spawner, tx) = setup();
//
//         let flag = Arc::new(AtomicUsize::new(0));
//         let flag2 = flag.clone();
//
//         let _h = spawner.spawn(async move {
//             time::sleep(Duration::from_secs(1)).await;
//             flag2.store(7, Ordering::SeqCst);
//         });
//         // Drop the handle intentionally
//         drop(_h);
//
//         advance(Duration::from_secs(1)).await;
//
//         // Signal shutdown and join — tracker should still wait for completion.
//         let _ = tx.send(());
//         tracker.join().await;
//
//         assert_eq!(flag.load(Ordering::SeqCst), 7);
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn aborting_handle_cancels_task_and_tracker_still_completes() {
//         pause();
//         let (tracker, spawner, tx) = setup();
//
//         let flag = Arc::new(AtomicUsize::new(0));
//         let flag2 = flag.clone();
//
//         let h = spawner.spawn(async move {
//             // would complete at t=5s, but we'll abort earlier
//             time::sleep(Duration::from_secs(5)).await;
//             flag2.store(999, Ordering::SeqCst);
//         });
//
//         // Advance 2s, then abort
//         advance(Duration::from_secs(2)).await;
//         h.abort();
//         // Awaiting returns JoinError::Cancelled
//         assert!(h.await.is_err());
//
//         // Tracker should still consider it "finished" (cancelled), so join doesn’t hang.
//         let _ = tx.send(());
//         tracker.join().await;
//
//         // The task body never set the flag
//         assert_eq!(flag.load(Ordering::SeqCst), 0);
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn interval_final_action_runs_on_shutdown() {
//         pause();
//         let (tracker, spawner, tx) = setup();
//
//         let calls = Arc::new(AtomicUsize::new(0));
//         let calls2 = calls.clone();
//
//         let _h = spawner.spawn_interval(
//             move || {
//                 let c = calls2.clone();
//                 async move { c.fetch_add(1, Ordering::SeqCst); }
//             },
//             IntervalTaskParameters::new_no_delay(Duration::from_secs(1), /*act_on_shutdown=*/ true),
//         );
//
//         advance(Duration::from_secs(3)).await; // ~3 ticks
//         let before = calls.load(Ordering::SeqCst);
//
//         let _ = tx.send(()); // triggers final action
//         tracker.join().await;
//
//         let after = calls.load(Ordering::SeqCst);
//         assert!(
//             after >= before + 1,
//             "expected at least one final call on shutdown (before={before}, after={after})"
//         );
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn initial_delay_is_preempted_and_still_runs_final_action() {
//         pause();
//         let (tracker, spawner, tx) = setup();
//
//         let calls = Arc::new(AtomicUsize::new(0));
//         let calls2 = calls.clone();
//
//         let _h = spawner.spawn_interval(
//             move || {
//                 let c = calls2.clone();
//                 async move { c.fetch_add(1, Ordering::SeqCst); }
//             },
//             IntervalTaskParameters::new_with_delay(
//                 Duration::from_secs(10),
//                 Duration::from_secs(60), // long initial delay
//                 /*act_on_shutdown=*/ true,
//             ),
//         );
//
//         // Shutdown during the initial delay
//         let _ = tx.send(());
//         tracker.join().await;
//
//         // Even though no ticks happened, final action should run once.
//         assert_eq!(calls.load(Ordering::SeqCst), 1);
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn interval_no_final_action_when_disabled() {
//         pause();
//         let (tracker, spawner, tx) = setup();
//
//         let calls = Arc::new(AtomicUsize::new(0));
//         let calls2 = calls.clone();
//
//         let _h = spawner.spawn_interval(
//             move || {
//                 let c = calls2.clone();
//                 async move { c.fetch_add(1, Ordering::SeqCst); }
//             },
//             IntervalTaskParameters::new_no_delay(Duration::from_secs(10), /*act_on_shutdown=*/ false),
//         );
//
//         // Immediately shutdown; no ticks due to long interval, and no final action expected.
//         let _ = tx.send(());
//         tracker.join().await;
//
//         assert_eq!(calls.load(Ordering::SeqCst), 0);
//     }
// }
