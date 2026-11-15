/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    num::NonZeroUsize,
    panic,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

// maps the tasks across up to one scoped thread per cpu, collecting outputs in no particular order;
// an error stops every worker, and a worker's panic is resumed on the caller
pub fn parallel_flat_map<Task, Output, Outputs, Error>(
    tasks: &[Task],
    map: impl Fn(&Task) -> Result<Outputs, Error> + Sync,
) -> Result<Vec<Output>, Error>
where
    Task: Sync,
    Output: Send,
    Outputs: IntoIterator<Item = Output>,
    Error: Send,
{
    let next_task = AtomicUsize::new(0);
    let work = || -> Result<Vec<Output>, Error> {
        let mut outputs = Vec::new();
        while let Some(task) = tasks.get(next_task.fetch_add(1, Ordering::Relaxed)) {
            match map(task) {
                Ok(task_outputs) => outputs.extend(task_outputs),
                Err(error) => {
                    next_task.store(tasks.len(), Ordering::Relaxed);
                    return Err(error);
                }
            }
        }
        Ok(outputs)
    };

    let worker_count = thread::available_parallelism().map_or(1, NonZeroUsize::get).min(tasks.len());
    let results: Vec<_> = thread::scope(|scope| {
        let workers: Vec<_> = (0..worker_count).map(|_| scope.spawn(&work)).collect();
        workers.into_iter().map(|worker| worker.join().unwrap_or_else(|err| panic::resume_unwind(err))).collect()
    });
    let mut outputs = Vec::new();
    for result in results {
        outputs.extend(result?);
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_task_is_mapped_once() {
        let tasks: Vec<u64> = (0..100).collect();
        let mut outputs = parallel_flat_map(&tasks, |&task| Ok::<_, ()>([task, task + 1000])).unwrap();
        outputs.sort();
        let mut expected: Vec<u64> = tasks.iter().flat_map(|&task| [task, task + 1000]).collect();
        expected.sort();
        assert_eq!(outputs, expected);
    }

    #[test]
    fn no_tasks_map_to_nothing() {
        let outputs = parallel_flat_map(&[] as &[u64], |&task| Ok::<_, ()>([task])).unwrap();
        assert!(outputs.is_empty());
    }

    #[test]
    fn an_error_is_returned() {
        let tasks: Vec<u64> = (0..10).collect();
        let result = parallel_flat_map(&tasks, |&task| if task == 7 { Err(task) } else { Ok([task]) });
        assert_eq!(result, Err(7));
    }

    #[test]
    #[should_panic(expected = "task failed")]
    fn a_panic_is_resumed() {
        let _ = parallel_flat_map(&[1u64], |_| -> Result<[u64; 0], ()> { panic!("task failed") });
    }
}
