/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

use common::*;

#[test]
fn readers_see_whole_commits_and_search_agrees_with_scan_while_a_writer_commits() {
    const COMMITS: usize = 40;
    const VECTORS_PER_COMMIT: usize = 3;
    const READERS: usize = 4;

    let (_tmp_dir, database) = setup();
    let vector = |index: usize| {
        let angle = index as f32 * std::f32::consts::FRAC_PI_2 / (COMMITS * VECTORS_PER_COMMIT) as f32;
        format!("[{:.6}, {:.6}, 0.0]", angle.cos(), angle.sin())
    };
    let done = AtomicBool::new(false);

    thread::scope(|scope| {
        scope.spawn(|| {
            for commit in 0..COMMITS {
                let inserts: Vec<String> = (0..VECTORS_PER_COMMIT)
                    .map(|i| {
                        let vector = vector(commit * VECTORS_PER_COMMIT + i);
                        format!(r#"$i{i} isa item, has embedding vector({vector}, "float32");"#)
                    })
                    .collect();
                commit_write(database.clone(), &format!("insert {}", inserts.join(" ")));
            }
            done.store(true, Ordering::Release);
        });
        for _ in 0..READERS {
            scope.spawn(|| {
                while !done.load(Ordering::Acquire) {
                    let reader = open_read(&database);
                    let visible = count(&reader, ALL);
                    assert_eq!(visible % VECTORS_PER_COMMIT, 0, "a commit must be visible as a whole");
                    assert_eq!(count(&reader, &nearest_to(B)), visible, "search and scan must agree in one snapshot");
                }
            });
        }
    });

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), COMMITS * VECTORS_PER_COMMIT);
    assert_eq!(count(&later, &nearest_to(B)), COMMITS * VECTORS_PER_COMMIT);
}
