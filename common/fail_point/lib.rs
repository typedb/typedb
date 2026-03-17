/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::HashMap,
    ops::ControlFlow,
    str::FromStr,
    sync::{Mutex, OnceLock},
};

use itertools::Itertools;
use rand::Rng;
use tracing::info;

pub const FAIL_POINT_ENV: &str = "FAILPOINTS";

macro_rules! fail_points {
    ($($name:ident),* $(,)?) => {
        const COUNT: usize = [$($name),*].len();
        pub const ALL: [&str; COUNT] = [$($name),*];
        $(
        pub const $name: &str = stringify!($name);
        )*
    };
}

fail_points! {
    CHECKPOINT_CLEANUP_FAIL,
    CHECKPOINT_CLEANUP_PARTIAL_FAIL,
    CHECKPOINT_DIR_CREATE_FAIL,
    CHECKPOINT_FILE_EMPTY,
    CHECKPOINT_FILE_SYNC_FAIL,
    CHECKPOINT_METADATA_WRITE_FAIL,
    COMMIT_APPLIED_WITHOUT_PERSISTING_STATUS,
    COMMIT_DATA_UNSYNC_IN_WAL,
    COMMIT_REJECTED_WITHOUT_PERSISTING_STATUS,
    KEYSPACE_CHECKPOINT_FAIL,
    KEYSPACE_DELETE_FAIL,
    KEYSPACE_OPEN_FAIL,
    RECOVERY_PARTIAL_WRITE,
    STORAGE_DELETED_KEYSPACES_BUT_NOT_WAL,
    STORAGE_EMPTY_STORAGE_DIR,
    STORAGE_MISSING_STORAGE_DIR,
    UNFINISHED_CHECKPOINT,
    WAL_EMPTY_WAL_DIR,
    WAL_PARTIAL_HEADER_SEQ,
    WAL_PARTIAL_HEADER_SEQ_LEN,
    WAL_RECORD_ONLY_HEADER,
    WAL_RECORD_UNFLUSHED,
}

#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        #[cfg(debug_assertions)]
        $crate::eval($name)
    };
}

type Registry = HashMap<String, FailPoint>;

static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn init_registry() -> Registry {
    let Ok(config) = std::env::var(FAIL_POINT_ENV) else { return Registry::default() };
    config
        .split(';')
        .map(|cfg| {
            let Some((key, value)) = cfg.split_once('=') else {
                panic!("Could not parse failpoint configuration '{cfg}'")
            };
            (key.trim().to_owned(), value.parse().unwrap())
        })
        .collect()
}

#[inline(always)]
pub fn eval(key: &'static str) {
    let registry = REGISTRY.get_or_init(init_registry);
    if let Some(p) = registry.get(key) {
        p.eval(key)
    }
}

struct FailPoint {
    actions: Mutex<Vec<Action>>,
}

impl FailPoint {
    fn eval(&self, key: &str) {
        for action in &mut *self.actions.lock().unwrap() {
            if let ControlFlow::Break(()) = action.eval(key) {
                break;
            }
        }
    }
}

impl FromStr for FailPoint {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { actions: Mutex::new(s.trim().split("->").map(Action::from_str).try_collect()?) })
    }
}

struct Action {
    task: Task,
    frequency: f64,
    max_cnt: Option<u32>,
}

impl Action {
    fn new(task: Task, frequency: f64, max_cnt: Option<u32>) -> Self {
        Self { task, frequency, max_cnt }
    }

    fn eval(&mut self, key: &str) -> ControlFlow<()> {
        let mut rng = rand::thread_rng();
        if self.max_cnt == Some(0) {
            return ControlFlow::Continue(());
        }
        if self.frequency < 1.0 && !rng.gen_bool(self.frequency) {
            return ControlFlow::Continue(());
        }
        if let Some(max_cnt) = &mut self.max_cnt {
            *max_cnt -= 1;
        }

        match self.task {
            Task::Off => (),
            Task::Panic => panic!("failpoint {key} triggered"),
            Task::Print => info!("failpoint {key} reached"),
        }

        ControlFlow::Break(())
    }
}

impl FromStr for Action {
    type Err = String;

    /// Parse an action.
    ///
    /// `s` should be in the format `[p%][cnt*]task`, `p%` is the frequency,
    /// `cnt` is the max times the action can be triggered.

    fn from_str(s: &str) -> Result<Action, String> {
        fn partition(s: &str, pattern: char) -> (&str, Option<&str>) {
            let mut splits = s.splitn(2, pattern);
            (splits.next().unwrap(), splits.next())
        }

        let mut remain = s.trim();

        let mut frequency = 1f64;
        let (first, second) = partition(remain, '%');
        if let Some(second) = second {
            remain = second;
            match first.parse::<f64>() {
                Err(e) => return Err(format!("failed to parse frequency: {}", e)),
                Ok(freq) => frequency = freq / 100.0,
            }
        }

        let mut max_cnt = None;
        let (first, second) = partition(remain, '*');
        if let Some(second) = second {
            remain = second;
            match first.parse() {
                Err(e) => return Err(format!("failed to parse count: {}", e)),
                Ok(cnt) => max_cnt = Some(cnt),
            }
        }

        let task = match remain {
            "off" => Task::Off,
            "panic" => Task::Panic,
            "print" => Task::Print,
            _ => return Err(format!("unrecognized command {:?}", remain)),
        };

        Ok(Action::new(task, frequency, max_cnt))
    }
}

enum Task {
    Off,
    Panic,
    Print,
}
