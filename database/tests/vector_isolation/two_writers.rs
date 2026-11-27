/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use common::*;
use storage::isolation_manager::IsolationConflict;

#[test]
fn concurrent_writers_do_not_see_each_others_uncommitted_vectors() {
    let (_tmp_dir, database) = setup();

    let first = run_write(open_write(&database), &insert(A));
    let second = run_write(open_write(&database), &insert(B));

    assert_eq!(count(&first, ALL), 1);
    assert_eq!(count(&first, &equal_to(B)), 0);
    assert_eq!(count(&first, &nearest_to(B)), 1);
    assert_eq!(count(&second, ALL), 1);
    assert_eq!(count(&second, &equal_to(A)), 0);
    assert_eq!(count(&second, &nearest_to(A)), 1);

    try_commit(first).unwrap();
    try_commit(second).unwrap();

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 2);
    assert_eq!(count(&later, &nearest_to(A)), 2);
}

#[test]
fn concurrent_writers_of_the_same_new_vector_one_fails() {
    let (_tmp_dir, database) = setup();

    let first = run_write(open_write(&database), &insert(A));
    let second = run_write(open_write(&database), &insert(A));
    try_commit(first).unwrap();
    assert_eq!(isolation_conflict(try_commit(second)), IsolationConflict::ExclusiveLock);

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 1);
    assert_eq!(count(&later, OWNERSHIPS), 1);
    assert_eq!(count(&later, &nearest_to(A)), 1);
}

#[test]
fn concurrent_writers_of_an_existing_vector_both_commit() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));

    let first = run_write(open_write(&database), &insert(A));
    let second = run_write(open_write(&database), &insert(A));
    try_commit(first).unwrap();
    try_commit(second).unwrap();

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 1);
    assert_eq!(count(&later, OWNERSHIPS), 3);
    assert_eq!(count(&later, &nearest_to(A)), 1);
}

#[test]
fn concurrent_attach_and_delete_of_a_vector_the_attach_fails_when_the_delete_commits_first() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));

    let deleter = run_write(open_write(&database), &delete(A));
    let attacher = run_write(open_write(&database), &insert(A));
    try_commit(deleter).unwrap();
    assert_eq!(isolation_conflict(try_commit(attacher)), IsolationConflict::RequireDeletedKey);

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 0);
    assert_eq!(count(&later, &nearest_to(A)), 0);
}

#[test]
fn concurrent_attach_and_delete_of_a_vector_the_delete_fails_when_the_attach_commits_first() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));

    let deleter = run_write(open_write(&database), &delete(A));
    let attacher = run_write(open_write(&database), &insert(A));
    try_commit(attacher).unwrap();
    assert_eq!(isolation_conflict(try_commit(deleter)), IsolationConflict::DeletingRequiredKey);

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 1);
    assert_eq!(count(&later, OWNERSHIPS), 2);
    assert_eq!(count(&later, &nearest_to(A)), 1);
}

#[test]
fn concurrent_attach_of_a_vector_and_delete_of_its_owner_one_fails() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), r#"insert $i isa item, has name "x";"#);

    let deleter = run_write(open_write(&database), r#"match $i isa item, has name "x"; delete $i;"#);
    let attacher = run_write(open_write(&database), &attach("x", A));
    try_commit(deleter).unwrap();
    assert_eq!(isolation_conflict(try_commit(attacher)), IsolationConflict::RequireDeletedKey);

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 0);
    assert_eq!(count(&later, &nearest_to(A)), 0);
}

#[test]
fn concurrent_vector_writes_to_an_owner_with_cardinality_one_one_fails() {
    let (_tmp_dir, database) = setup();
    commit_schema(database.clone(), "define entity photo owns name, owns embedding @card(0..1);");
    commit_write(database.clone(), r#"insert $p isa photo, has name "p";"#);

    let set = |vector: &str| {
        format!(r#"match $p isa photo, has name "p"; insert $p has embedding vector({vector}, "float32");"#)
    };
    let first = run_write(open_write(&database), &set(A));
    let second = run_write(open_write(&database), &set(B));
    try_commit(first).unwrap();
    assert_eq!(isolation_conflict(try_commit(second)), IsolationConflict::ExclusiveLock);

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 1);
    assert_eq!(count(&later, &equal_to(B)), 0);
    assert_eq!(count(&later, &nearest_to(B)), 1);
}

#[test]
fn concurrent_deletes_of_the_same_vector_both_commit() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));
    commit_write(database.clone(), &insert(B));

    let first = run_write(open_write(&database), &delete(A));
    let second = run_write(open_write(&database), &delete(A));
    try_commit(first).unwrap();
    try_commit(second).unwrap();

    let later = open_read(&database);
    assert_eq!(count(&later, ALL), 1);
    assert_eq!(count(&later, &equal_to(A)), 0);
    assert_eq!(count(&later, &nearest_to(A)), 1);
}

#[test]
fn a_vector_detached_by_two_concurrent_transactions_is_gone() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert_named("x", A));
    commit_write(database.clone(), &insert_named("y", A));

    let first = run_write(open_write(&database), &detach("x"));
    let second = run_write(open_write(&database), &detach("y"));
    try_commit(first).unwrap();
    try_commit(second).unwrap();

    let later = open_read(&database);
    assert_eq!(count(&later, OWNERSHIPS), 0);
    assert_eq!(count(&later, ALL), 0);
    assert_eq!(count(&later, &equal_to(A)), 0);
    assert_eq!(count(&later, &nearest_to(A)), 0);
}

#[test]
fn two_writers_that_search_then_insert_both_commit() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));

    let mut first = open_write(&database);
    let mut second = open_write(&database);
    assert_eq!(count(&first, &nearest_to(B)), 1);
    assert_eq!(count(&second, &nearest_to(B)), 1);
    first = run_write(first, &insert(B));
    second = run_write(second, &insert(C));

    try_commit(first).unwrap();
    try_commit(second).unwrap();

    let later = open_read(&database);
    assert_eq!(count(&later, &nearest_to(B)), 3);
}

#[test]
fn attach_after_a_concurrent_delete_and_a_checkpoint_is_rejected_at_commit() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));

    let mut attacher = open_write(&database);
    commit_write(database.clone(), &delete(A));
    checkpoint(&database);

    attacher = run_write(attacher, &insert(A));
    assert_eq!(isolation_conflict(try_commit(attacher)), IsolationConflict::RequireDeletedKey);
}
