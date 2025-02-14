/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender},
    thread,
    time::Duration,
};

#[derive(Debug)]
pub struct IntervalRunner {
    shutdown_sink: SyncSender<SyncSender<()>>,
}

impl IntervalRunner {
    const ZERO_DURATION: Duration = Duration::from_secs(0);

    pub fn new(action: impl FnMut() + Send + 'static, interval: Duration) -> Self {
        Self::new_with_initial_delay(action, interval, Self::ZERO_DURATION)
    }

    pub fn new_with_initial_delay(
        mut action: impl FnMut() + Send + 'static,
        interval: Duration,
        initial_delay: Duration,
    ) -> Self {
        let (shutdown_sender, shutdown_receiver) = sync_channel::<SyncSender<()>>(1);
        thread::spawn(move || {
            match shutdown_receiver.recv_timeout(initial_delay) {
                Ok(done_sender) => {
                    drop(action);
                    done_sender.send(()).unwrap();
                    return;
                }
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return, // TODO log?
            }

            loop {
                action();
                match shutdown_receiver.recv_timeout(interval) {
                    Ok(done_sender) => {
                        drop(action);
                        done_sender.send(()).unwrap();
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => (),
                    Err(RecvTimeoutError::Disconnected) => break, // TODO log?
                }
            }
        });
        Self { shutdown_sink: shutdown_sender }
    }
}

impl Drop for IntervalRunner {
    fn drop(&mut self) {
        let (done_sender, done_receiver) = sync_channel(1);
        self.shutdown_sink.send(done_sender).expect("Expected interval runner shutdown signal sending");
        done_receiver.recv().expect("Expected interval runner shutdown finishing")
    }
}
