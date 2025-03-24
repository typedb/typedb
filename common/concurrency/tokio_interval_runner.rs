/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{future::Future, sync::mpsc as std_mpsc};

use tokio::{
    sync::mpsc as tokio_mpsc,
    time::{self, Duration},
};

#[derive(Debug)]
pub struct TokioIntervalRunner {
    shutdown_sender: tokio_mpsc::UnboundedSender<std_mpsc::SyncSender<()>>,
}

impl TokioIntervalRunner {
    pub fn new<F>(action: impl 'static + Send + FnMut() -> F, interval: Duration, act_on_destroy: bool) -> Self
    where
        F: Future<Output = ()> + Sync + Send + 'static,
    {
        Self::new_with_initial_delay(action, interval, Duration::ZERO, act_on_destroy)
    }

    pub fn new_with_initial_delay<F>(
        mut action: impl 'static + Send + FnMut() -> F,
        interval: Duration,
        initial_delay: Duration,
        act_on_destroy: bool,
    ) -> Self
    where
        F: Future<Output = ()> + Sync + Send + 'static,
    {
        let (shutdown_sender, mut shutdown_receiver) = tokio_mpsc::unbounded_channel::<std_mpsc::SyncSender<()>>();
        tokio::spawn(async move {
            if !initial_delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(initial_delay) => (),
                    done_sender = shutdown_receiver.recv() => {
                        drop(action);
                        done_sender.unwrap().send(()).unwrap();
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
                    done_sender = shutdown_receiver.recv() => {
                        if act_on_destroy {
                            action().await;
                        }
                        drop(action);
                        done_sender.unwrap().send(()).unwrap();
                        break;
                    }
                }
            }
        });

        Self { shutdown_sender }
    }
}

impl Drop for TokioIntervalRunner {
    fn drop(&mut self) {
        let (sender, receiver) = std_mpsc::sync_channel(1);
        self.shutdown_sender.send(sender).expect("Expected shutdown signal sending");
        receiver.recv().expect("Expected shutdown finalization")
    }
}
