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

pub struct IntervalRunner {
    shutdown_sink: SyncSender<SyncSender<()>>,
}

impl IntervalRunner {
    pub fn new(mut action: impl FnMut() + Send + 'static, interval: Duration) -> Self {
        let (sender, receiver) = sync_channel::<SyncSender<()>>(0);
        thread::spawn(move || {
            loop {
                action();
                match receiver.recv_timeout(interval) {
                    Ok(done_sender) => {
                        drop(action);
                        done_sender.send(()).unwrap();
                        break;
                    },
                    Err(RecvTimeoutError::Timeout) => (),
                    Err(RecvTimeoutError::Disconnected) => break, // TODO log?
                }
            }
        });
        Self { shutdown_sink: sender }
    }
}

impl Drop for IntervalRunner {
    fn drop(&mut self) {
        let (done_sender, done_receiver) = sync_channel(0);
        self.shutdown_sink.send(done_sender).unwrap();
        done_receiver.recv().unwrap()
    }
}
