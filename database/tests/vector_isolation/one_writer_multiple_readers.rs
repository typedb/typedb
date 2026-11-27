/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use std::sync::Arc;

use common::*;
use database::{
    Database,
    transaction::{TransactionSchema, TransactionWrite},
};
use storage::durability_client::WALClient;

const D: &str = "[0.0, 1.0, 0.0, 0.0]";

const DELETE_ALL: &str = "match $e isa embedding; delete $e;";
const UNDEFINE: &str = "undefine embedding;";
const DEFINE_WITH_FOUR_DIMENSIONS: &str = r#"define
    attribute embedding, value vector(4, "float32") @index(hnsw:cosine);
    entity item owns embedding @card(0..);
"#;

enum Step {
    Schema(&'static str),
    Write(String),
}

enum End {
    Commit,
    RollBack,
    Close,
}

type Seen = Option<&'static [&'static str]>;

const NO_TYPE: Seen = None;

struct Case {
    setup: Vec<Step>,
    change: Vec<Step>,
    end: End,
    checkpoint: bool,
    before: Seen,
    own: Seen,
    after: Seen,
}

impl Case {
    fn new(setup: Vec<Step>, change: Vec<Step>, before: Seen, own: Seen, after: Seen) -> Self {
        Self { setup, change, end: End::Commit, checkpoint: false, before, own, after }
    }

    fn ending_with(self, end: End) -> Self {
        Self { end, after: self.before, ..self }
    }

    fn then_checkpoint(self) -> Self {
        Self { checkpoint: true, ..self }
    }
}

enum Writer {
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
}

impl Writer {
    fn open(database: &Arc<Database<WALClient>>, change: &[Step]) -> Self {
        if change.iter().any(|step| matches!(step, Step::Schema(_))) {
            Self::Schema(open_schema(database))
        } else {
            Self::Write(open_write(database))
        }
    }

    fn apply(self, step: &Step) -> Self {
        match (self, step) {
            (Self::Write(tx), Step::Write(query)) => Self::Write(run_write(tx, query)),
            (Self::Schema(tx), Step::Write(query)) => Self::Schema(run_write_in_schema(tx, query)),
            (Self::Schema(tx), Step::Schema(query)) => Self::Schema(run_schema(tx, query)),
            (Self::Write(_), Step::Schema(_)) => unreachable!("a schema step opens a schema transaction"),
        }
    }

    fn assert_sees(&self, expected: Seen) {
        match self {
            Self::Write(tx) => assert_sees("t2, before it ends", tx, expected),
            Self::Schema(tx) => assert_sees("t2, before it ends", tx, expected),
        }
    }

    fn end(self, end: &End) {
        match (self, end) {
            (Self::Write(tx), End::Commit) => try_commit(tx).unwrap(),
            (Self::Write(mut tx), End::RollBack) => {
                tx.rollback();
                try_commit(tx).unwrap()
            }
            (Self::Write(tx), End::Close) => tx.close(),
            (Self::Schema(tx), End::Commit) => commit_schema_transaction(tx),
            (Self::Schema(_), _) => unimplemented!("a schema transaction only commits here"),
        }
    }
}

fn elements(vector: &str) -> Vec<f32> {
    vector.trim_matches(['[', ']']).split(',').map(|element| element.trim().parse().unwrap()).collect()
}

fn assert_sees(who: &str, tx: &impl Transaction, expected: Seen) {
    let expected_count = expected.map(|vectors| vectors.len());
    assert_eq!(try_count(tx, ALL), expected_count, "{who}, by scan");

    let expected_values = expected.map(|vectors| {
        let mut values: Vec<Vec<f32>> = vectors.iter().map(|vector| elements(vector)).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        values
    });
    assert_eq!(values(tx), expected_values, "{who}, by values");

    let probe = expected.and_then(|vectors| vectors.first().copied()).unwrap_or(A);
    assert_eq!(try_count(tx, &nearest_to(probe)), expected_count, "{who}, by search");
}

fn run(case: Case) {
    let (_tmp_dir, database) = create();
    for step in &case.setup {
        match step {
            Step::Schema(query) => commit_schema(database.clone(), query),
            Step::Write(query) => commit_write(database.clone(), query),
        }
    }

    let r1 = open_read(&database);
    assert_sees("r1, when it opens", &r1, case.before);

    let mut t2 = Writer::open(&database, &case.change);
    for step in &case.change {
        t2 = t2.apply(step);
    }
    t2.assert_sees(case.own);
    assert_sees("r1, while t2 is open", &r1, case.before);
    let r2 = open_read(&database);

    t2.end(&case.end);
    if case.checkpoint {
        checkpoint(&database);
    }
    let r3 = open_read(&database);

    assert_sees("r1, after t2 ended", &r1, case.before);
    assert_sees("r2, after t2 ended", &r2, case.before);
    assert_sees("r3, opened after t2 ended", &r3, case.after);
}

fn schema() -> Step {
    Step::Schema(SCHEMA)
}

fn write(query: impl Into<String>) -> Step {
    Step::Write(query.into())
}

fn adds_a_vector_case() -> Case {
    Case::new(vec![schema(), write(insert(A))], vec![write(insert(B))], Some(&[A]), Some(&[A, B]), Some(&[A, B]))
}

fn deletes_a_vector_case() -> Case {
    Case::new(
        vec![schema(), write(insert(A)), write(insert(B))],
        vec![write(delete(A))],
        Some(&[A, B]),
        Some(&[B]),
        Some(&[B]),
    )
}

fn undefines_a_type_case() -> Case {
    Case::new(
        vec![schema(), write(insert(A))],
        vec![write(DELETE_ALL), Step::Schema(UNDEFINE)],
        Some(&[A]),
        NO_TYPE,
        NO_TYPE,
    )
}

fn redefines_a_type_case() -> Case {
    Case::new(
        vec![schema(), write(insert(A))],
        vec![write(DELETE_ALL), Step::Schema(UNDEFINE), Step::Schema(DEFINE_WITH_FOUR_DIMENSIONS), write(insert(D))],
        Some(&[A]),
        Some(&[D]),
        Some(&[D]),
    )
}

fn deletes_then_adds_the_same_vector_case() -> Case {
    Case::new(
        vec![schema(), write(insert(A))],
        vec![write(delete(A)), write(insert(A))],
        Some(&[A]),
        Some(&[A]),
        Some(&[A]),
    )
}

#[test]
fn adds_a_vector() {
    run(adds_a_vector_case());
}

#[test]
fn adds_the_first_vector_of_a_type() {
    run(Case::new(vec![schema()], vec![write(insert(A))], Some(&[]), Some(&[A]), Some(&[A])));
}

#[test]
fn deletes_a_vector() {
    run(deletes_a_vector_case());
}

#[test]
fn defines_a_type_and_adds_vectors() {
    run(Case::new(vec![], vec![schema(), write(insert(A))], NO_TYPE, Some(&[A]), Some(&[A])));
}

#[test]
fn undefines_a_type() {
    run(undefines_a_type_case());
}

#[test]
fn redefines_a_type_with_other_dimensions() {
    run(redefines_a_type_case());
}

#[test]
fn redefines_a_type_and_adds_the_same_vector() {
    run(Case::new(
        vec![schema(), write(insert(A)), write(insert(B))],
        vec![write(DELETE_ALL), Step::Schema(UNDEFINE), schema(), write(insert(A))],
        Some(&[A, B]),
        Some(&[A]),
        Some(&[A]),
    ));
}

#[test]
fn adds_then_deletes_the_same_vector() {
    let case = Case::new(
        vec![schema(), write(insert(A))],
        vec![write(insert(B)), write(delete(B))],
        Some(&[A]),
        Some(&[A]),
        Some(&[A]),
    );
    run(case);
}

#[test]
fn deletes_then_adds_the_same_vector() {
    run(deletes_then_adds_the_same_vector_case());
}

#[test]
fn attaches_then_deletes_an_existing_vector() {
    let case = Case::new(
        vec![schema(), write(insert(A))],
        vec![write(insert(A)), write(delete(A))],
        Some(&[A]),
        Some(&[]),
        Some(&[]),
    );
    run(case);
}

#[test]
fn adds_a_vector_then_rolls_back() {
    run(adds_a_vector_case().ending_with(End::RollBack));
}

#[test]
fn adds_a_vector_then_closes() {
    run(adds_a_vector_case().ending_with(End::Close));
}

#[test]
fn deletes_a_vector_then_rolls_back() {
    run(deletes_a_vector_case().ending_with(End::RollBack));
}

#[test]
fn deletes_a_vector_then_checkpoint() {
    run(deletes_a_vector_case().then_checkpoint());
}

#[test]
fn undefines_a_type_then_checkpoint() {
    run(undefines_a_type_case().then_checkpoint());
}

#[test]
fn redefines_a_type_then_checkpoint() {
    run(redefines_a_type_case().then_checkpoint());
}

#[test]
fn readers_keep_their_views_when_a_vector_is_deleted_and_inserted_again() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));
    let first = open_read(&database);

    commit_write(database.clone(), &delete(A));
    let second = open_read(&database);

    commit_write(database.clone(), &insert(A));
    let third = open_read(&database);

    assert_sees("opened before the delete", &first, Some(&[A]));
    assert_sees("opened between the delete and the insert", &second, Some(&[]));
    assert_sees("opened after the insert", &third, Some(&[A]));
}

#[test]
fn readers_keep_their_views_when_a_type_is_redefined_in_separate_commits() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));
    let earlier = open_read(&database);

    commit_write(database.clone(), DELETE_ALL);
    commit_schema(database.clone(), UNDEFINE);
    commit_schema(database.clone(), DEFINE_WITH_FOUR_DIMENSIONS);
    commit_write(database.clone(), &insert(D));
    let later = open_read(&database);

    assert_sees("opened before the type was redefined", &earlier, Some(&[A]));
    assert_sees("opened after the type was redefined", &later, Some(&[D]));
}

fn redefine_in_one_transaction(database: &Arc<Database<WALClient>>, define: &str, vector: &str) {
    let tx = run_write_in_schema(open_schema(database), DELETE_ALL);
    let tx = run_schema(run_schema(tx, UNDEFINE), define);
    commit_schema_transaction(run_write_in_schema(tx, &insert(vector)));
}

#[test]
fn readers_keep_their_views_when_a_type_id_is_reused_twice() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));
    let type_id = embedding_type_id(&database);
    let first = open_read(&database);

    redefine_in_one_transaction(&database, DEFINE_WITH_FOUR_DIMENSIONS, D);
    assert_eq!(embedding_type_id(&database), type_id);
    let second = open_read(&database);

    redefine_in_one_transaction(&database, SCHEMA, B);
    assert_eq!(embedding_type_id(&database), type_id);
    let third = open_read(&database);

    assert_sees("opened before the type was redefined", &first, Some(&[A]));
    assert_sees("opened after the first redefinition", &second, Some(&[D]));
    assert_sees("opened after the second redefinition", &third, Some(&[B]));
}

#[test]
fn a_deleted_vector_is_kept_only_while_an_earlier_transaction_is_open() {
    let (_tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));
    let reader = open_read(&database);
    let writer = open_write(&database);
    commit_write(database.clone(), &delete(A));

    checkpoint(&database);
    assert_eq!(set_aside_vector_count(&database), 1);

    reader.close();
    checkpoint(&database);
    assert_eq!(set_aside_vector_count(&database), 1, "the earlier writer can still read it");

    writer.close();
    checkpoint(&database);
    assert_eq!(set_aside_vector_count(&database), 0);
}

#[test]
fn reader_opened_before_any_commit_since_the_database_opened_keeps_a_deleted_vector() {
    let (tmp_dir, database) = setup();
    commit_write(database.clone(), &insert(A));
    let database = reopen(&tmp_dir, database);

    let reader = open_read(&database);
    commit_write(database.clone(), &delete(A));
    checkpoint(&database);
    checkpoint(&database);

    assert_sees("opened before the first commit since the database opened", &reader, Some(&[A]));
}
