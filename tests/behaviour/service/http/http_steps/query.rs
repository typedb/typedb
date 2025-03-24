/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::VecDeque, ops::Index, str::FromStr};

use cucumber::{gherkin::Step, given, then, when};
use futures::{future::join_all, StreamExt, TryStreamExt};
use itertools::Itertools;
use macro_rules_attribute::apply;
use params::{self, check_boolean, ContainsOrDoesnt};
use server::service::{http::message::query::QueryAnswerResponse, AnswerType, QueryType};

use crate::{
    assert_err, generic_step,
    message::{transactions_query, ConceptResponse},
    params::{ConceptKind, IsByVarIndex, IsOrNot, QueryAnswerType, Var},
    util::{iter_table, list_contains_json, parse_json},
    Context, HttpBehaviourTestError,
};

fn get_answers_column_names(answer: &serde_json::Value) -> Vec<String> {
    if let serde_json::Value::Object(answers) = answer {
        answers.keys().cloned().collect()
    } else {
        panic!("No object answers")
    }
}

fn get_answer_rows_var(context: &mut Context, index: usize, var: Var) -> Option<&serde_json::Value> {
    let concept_row = context.get_answer_row_index(index);
    concept_row.get(&var.name)
}

fn check_concept_is_type(concept: &ConceptResponse, is_type: params::Boolean) {
    check_boolean!(is_type, concept.is_type());
}

fn check_concept_is_instance(concept: &ConceptResponse, is_instance: params::Boolean) {
    check_boolean!(is_instance, concept.is_instance());
}

fn check_concept_is_entity_type(concept: &ConceptResponse, is_entity_type: params::Boolean) {
    check_boolean!(is_entity_type, matches!(concept, ConceptResponse::EntityType(_)));
}

fn check_concept_is_relation_type(concept: &ConceptResponse, is_relation_type: params::Boolean) {
    check_boolean!(is_relation_type, matches!(concept, ConceptResponse::RelationType(_)));
}

fn check_concept_is_role_type(concept: &ConceptResponse, is_role_type: params::Boolean) {
    check_boolean!(is_role_type, matches!(concept, ConceptResponse::RoleType(_)));
}

fn check_concept_is_attribute_type(concept: &ConceptResponse, is_attribute_type: params::Boolean) {
    check_boolean!(is_attribute_type, matches!(concept, ConceptResponse::AttributeType(_)));
}

fn check_concept_is_entity(concept: &ConceptResponse, is_entity: params::Boolean) {
    check_boolean!(is_entity, matches!(concept, ConceptResponse::Entity(_)));
}

fn check_concept_is_relation(concept: &ConceptResponse, is_relation: params::Boolean) {
    check_boolean!(is_relation, matches!(concept, ConceptResponse::Relation(_)));
}

fn check_concept_is_attribute(concept: &ConceptResponse, is_attribute: params::Boolean) {
    check_boolean!(is_attribute, matches!(concept, ConceptResponse::Attribute(_)));
}

fn check_concept_is_value(concept: &ConceptResponse, is_value: params::Boolean) {
    check_boolean!(is_value, matches!(concept, ConceptResponse::Value(_)));
}

fn check_concept_is_kind(concept: &ConceptResponse, concept_kind: ConceptKind, is_kind: params::Boolean) {
    match concept_kind {
        ConceptKind::Concept => (),
        ConceptKind::Type => check_concept_is_type(concept, is_kind),
        ConceptKind::Instance => check_concept_is_instance(concept, is_kind),
        ConceptKind::EntityType => check_concept_is_entity_type(concept, is_kind),
        ConceptKind::RelationType => check_concept_is_relation_type(concept, is_kind),
        ConceptKind::AttributeType => check_concept_is_attribute_type(concept, is_kind),
        ConceptKind::RoleType => check_concept_is_role_type(concept, is_kind),
        ConceptKind::Entity => check_concept_is_entity(concept, is_kind),
        ConceptKind::Relation => check_concept_is_relation(concept, is_kind),
        ConceptKind::Attribute => check_concept_is_attribute(concept, is_kind),
        ConceptKind::Value => check_concept_is_value(concept, is_kind),
    }
}

fn concept_get_type(concept: &ConceptResponse) -> ConceptResponse {
    match concept {
        ConceptResponse::Entity(entity) => ConceptResponse::EntityType(entity.type_.clone().unwrap()),
        ConceptResponse::Relation(relation) => ConceptResponse::RelationType(relation.type_.clone().unwrap()),
        ConceptResponse::Attribute(attribute) => ConceptResponse::AttributeType(attribute.type_.clone().unwrap()),
        _ => panic!("Only instances can have types"),
    }
}

pub fn unquote(value: &str) -> String {
    let mut result: &str = value;
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        result = &value[1..value.len() - 1];
    }
    result.to_string()
}

fn check_is_value(
    is_or_not: IsOrNot,
    expected_value: params::Value,
    actual_value: &serde_json::Value,
    actual_value_type: &str,
) {
    let actual_value_type_converted = params::ValueType::from_str(actual_value_type)
        .expect("Expected actual value type conversion")
        .into_typedb_static();
    let actual_value_converted = params::Value::from_str(&unquote(&actual_value.to_string()))
        .expect("Expected actual value conversion")
        .into_typedb(actual_value_type_converted.clone());
    is_or_not.compare(expected_value.into_typedb(actual_value_type_converted), actual_value_converted);
}

#[apply(generic_step)]
#[step(expr = "typeql schema query{typeql_may_error}")]
#[step(expr = "typeql write query{typeql_may_error}")]
#[step(expr = "typeql read query{typeql_may_error}")]
pub async fn typeql_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    context.cleanup_answers().await;
    may_error.check(transactions_query(&context.http_context, context.transaction(), step.docstring().unwrap()).await);
}

#[apply(generic_step)]
#[step(expr = "get answers of typeql schema query")]
#[step(expr = "get answers of typeql write query")]
#[step(expr = "get answers of typeql read query")]
pub async fn get_answers_of_typeql_query(context: &mut Context, step: &Step) {
    context.cleanup_answers().await;
    context
        .set_answer(transactions_query(&context.http_context, context.transaction(), step.docstring().unwrap()).await)
        .unwrap();
}

#[apply(generic_step)]
#[step(expr = r"concurrently get answers of typeql schema query {int} times")]
#[step(expr = r"concurrently get answers of typeql write query {int} times")]
#[step(expr = r"concurrently get answers of typeql read query {int} times")]
pub async fn concurrently_get_answers_of_typeql_query_times(context: &mut Context, count: usize, step: &Step) {
    context.cleanup_concurrent_answers().await;

    let queries = vec![step.docstring().unwrap(); count];
    let answers: Vec<QueryAnswerResponse> = join_all(
        queries.into_iter().map(|query| transactions_query(&context.http_context, context.transaction(), query)),
    )
    .await
    .into_iter()
    .map(|result| result.unwrap())
    .collect();

    context.set_concurrent_answers(answers);
}

#[apply(generic_step)]
#[step(expr = "answer type {is_or_not}: {query_answer_type}")]
pub async fn answer_type_is(context: &mut Context, is_or_not: IsOrNot, query_answer_type: QueryAnswerType) {
    let actual_answer_type = context.get_answer_type().unwrap();
    match query_answer_type {
        QueryAnswerType::Ok => is_or_not.check(matches!(actual_answer_type, AnswerType::Ok)),
        QueryAnswerType::ConceptRows => is_or_not.check(matches!(actual_answer_type, AnswerType::ConceptRows)),
        QueryAnswerType::ConceptDocuments => {
            is_or_not.check(matches!(actual_answer_type, AnswerType::ConceptDocuments))
        }
    }
}

#[apply(generic_step)]
#[step(expr = "answer unwraps as {query_answer_type}{may_error}")]
pub async fn answer_unwraps_as(context: &mut Context, query_answer_type: QueryAnswerType, may_error: params::MayError) {
    let expect = !may_error.expects_error();
    let response = context.get_answer().unwrap();
    match response.answer_type {
        AnswerType::Ok => {
            assert_eq!(
                expect,
                matches!(query_answer_type, QueryAnswerType::Ok),
                "Expected {expect} {query_answer_type}"
            )
        }
        AnswerType::ConceptRows => {
            assert_eq!(
                expect,
                matches!(query_answer_type, QueryAnswerType::ConceptRows),
                "Expected {expect} {query_answer_type}"
            )
        }
        AnswerType::ConceptDocuments => {
            assert_eq!(
                expect,
                matches!(query_answer_type, QueryAnswerType::ConceptDocuments),
                "Expected {expect} {query_answer_type}"
            )
        }
    }
}

#[apply(generic_step)]
#[step(expr = r"answer size is: {int}")]
pub async fn answer_size_is(context: &mut Context, size: usize) {
    let actual_size = context.get_answer().unwrap().answers.as_ref().unwrap().len();
    assert_eq!(actual_size, size, "Expected {size} answers, got {actual_size}");
}

#[apply(generic_step)]
#[step(expr = "concurrently process {int} row(s) from answers{may_error}")]
pub async fn concurrently_process_rows_from_answers(context: &mut Context, count: usize, may_error: params::MayError) {
    // Cannot actually process them because they are already collected. But we can at least check that these rows exist
    let expects_error = may_error.expects_error();
    let index = context.get_concurrent_answers_index();
    let new_index = index + count;
    let answers = context.get_concurrent_answers();

    let mut jobs = Vec::new();

    for answer in answers.iter() {
        let answer = answer.answers.as_ref().unwrap();
        let job = async {
            let mut failed = false;
            let mut rows = Vec::new(); // Some work

            for _ in 0..count {
                if let Some(row) = answer.get(index) {
                    rows.push(row.clone());
                } else {
                    failed = true;
                    break;
                }
            }

            assert_eq!(expects_error, failed, "Expected to fail? {expects_error}, but did it? {failed}");
        };

        jobs.push(job);
    }

    join_all(jobs).await;
    context.set_concurrent_answers_index(new_index);
}

#[apply(generic_step)]
#[step(expr = r"answer column names are:")]
pub async fn answer_column_names_are(context: &mut Context, step: &Step) {
    let actual_column_names: Vec<String> =
        get_answers_column_names(context.get_answer_row_index(0)).into_iter().sorted().collect();
    let expected_column_names: Vec<String> = iter_table(step).sorted().map(|s| s.to_string()).collect();
    assert_eq!(actual_column_names, expected_column_names);
}

#[apply(generic_step)]
#[step(expr = r"answer query type {is_or_not}: {query_type}")]
pub async fn answer_query_type_is(context: &mut Context, is_or_not: IsOrNot, query_type: crate::params::QueryType) {
    let real_query_type = context.get_answer_query_type().unwrap();
    is_or_not.compare(real_query_type, query_type.into());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) query type {is_or_not}: {query_type}")]
pub async fn answer_get_row_query_type_is(
    context: &mut Context,
    index: usize,
    is_or_not: IsOrNot,
    query_type: crate::params::QueryType,
) {
    // no-op
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get variable{is_by_var_index}\({var}\){may_error}")]
pub async fn answer_get_row_get_variable(
    context: &mut Context,
    index: usize,
    is_by_var_index: IsByVarIndex,
    var: Var,
    may_error: params::MayError,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let variable = var.name.clone();
    may_error.check(
        get_answer_rows_var(context, index, var).ok_or(HttpBehaviourTestError::UnavailableRowVariable { variable }),
    );
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get variable{is_by_var_index}\({var}\) {is_or_not} empty")]
pub async fn answer_get_row_get_variable_is_empty(
    context: &mut Context,
    index: usize,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    is_or_not.compare(get_answer_rows_var(context, index, var), None);
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get variable by index\({int}\){may_error}")]
pub async fn answer_get_row_get_variable_by_index(
    context: &mut Context,
    index: usize,
    variable_index: usize,
    may_error: params::MayError,
) {
    // no-op:
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get variable by index\({int}\) {is_or_not} empty")]
pub async fn answer_get_row_get_variable_by_index_is_empty(
    context: &mut Context,
    index: usize,
    variable_index: usize,
    is_or_not: IsOrNot,
) {
    // no op
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get variable{is_by_var_index}\({var}\) as {concept_kind}{may_error}")]
pub async fn answer_get_row_get_variable_as(
    context: &mut Context,
    index: usize,
    is_by_var_index: IsByVarIndex,
    var: Var,
    kind: ConceptKind,
    may_error: params::MayError,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    may_error.check((|| {
        kind.matches_concept(&concept).then(|| ()).ok_or(HttpBehaviourTestError::InvalidConceptConversion {})
    })());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is {concept_kind}: {boolean}")]
pub async fn answer_get_row_get_variable_is_kind(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    checked_kind: ConceptKind,
    is_kind: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_concept_is_kind(&concept, checked_kind, is_kind);
}

#[apply(generic_step)]
#[step(
    expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get type is {concept_kind}: {boolean}"
)]
pub async fn answer_get_row_get_variable_get_type_is_kind(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    checked_kind: ConceptKind,
    is_kind: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    let type_ = concept_get_type(&concept);
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_concept_is_kind(&type_, checked_kind, is_kind);
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get label {is_or_not} none")]
pub async fn answer_get_row_get_variable_try_get_label_is_none(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    is_or_not.check_none(&concept.try_get_label());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get label: {word}")]
pub async fn answer_get_row_get_variable_try_get_label(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    label: String,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    assert_eq!(label.as_str(), concept.get_label());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get label: {word}")]
pub async fn answer_get_row_get_variable_get_label(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    label: String,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    assert_eq!(label.as_str(), concept.get_label());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get type get label: {word}")]
pub async fn answer_get_row_get_variable_get_type_get_label(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    label: String,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    let type_ = concept_get_type(&concept);
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    assert_eq!(label.as_str(), type_.get_label());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) {contains_or_doesnt} iid")]
pub async fn answer_get_row_get_variable_get_iid_exists(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    contains_or_doesnt: params::ContainsOrDoesnt,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    match contains_or_doesnt {
        ContainsOrDoesnt::Contains => {
            assert!(concept.try_get_iid().is_some(), "Expected iid for concept {}", concept.get_label())
        }
        ContainsOrDoesnt::DoesNotContain => {
            assert!(concept.try_get_iid().is_none(), "Expected NO iid for concept {}", concept.get_label())
        }
    }
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get iid {is_or_not} none")]
pub async fn answer_get_row_get_variable_try_get_iid_is_none(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    // Basically the same as non-try version in Rust, but can differ in other drivers
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    is_or_not.check_none(&concept.try_get_iid());
}

#[apply(generic_step)]
#[step(
    expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get value type {is_or_not} none"
)]
pub async fn answer_get_row_get_variable_try_get_value_type_is_none(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    is_or_not.check_none(&concept.try_get_value_type());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get value type: {word}")]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get value type: {word}")]
pub async fn answer_get_row_get_variable_get_value_type(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    value_type: String,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    match value_type.as_str() {
        "none" => assert!(concept.get_value_type().is_none()),
        value_type => assert_eq!(value_type, concept.get_value_type().expect("Expected value type")),
    }
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get type get value type: {word}")]
pub async fn answer_get_row_get_variable_get_type_get_value_type(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    value_type: String,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    let type_ = concept_get_type(&concept);
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    match value_type.as_str() {
        "none" => assert!(concept.get_value_type().is_none()),
        value_type => assert_eq!(value_type, concept.get_value_type().expect("Expected value type")),
    }
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get value {is_or_not} none")]
pub async fn answer_get_row_get_variable_try_get_value_is_none(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    is_or_not.check_none(&concept.try_get_value());
}

#[apply(generic_step)]
#[step(
    expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get value {is_or_not}: {value}"
)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get value {is_or_not}: {value}")]
pub async fn answer_get_row_get_variable_get_value(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
    value: params::Value,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    let actual_value = concept.get_value();
    check_is_value(is_or_not, value, concept.get_value(), concept.get_value_type().unwrap());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get value{is_by_var_index}\({var}\) get {is_or_not}: {value}")]
pub async fn answer_get_row_get_value_get_specific_value(
    context: &mut Context,
    index: usize,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_or_not: IsOrNot,
    value: params::Value,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    answer_get_row_get_variable_get_value(context, index, ConceptKind::Value, is_by_var_index, var, is_or_not, value)
        .await
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get {value_type}{may_error}")]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get {value_type}{may_error}")]
pub async fn answer_get_row_get_variable_get_value_of_type(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    value_type: params::ValueType,
    may_error: params::MayError,
) {
    use HttpBehaviourTestError::InvalidValueRetrieval;
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    let result = if value_type.as_str() == concept.get_value_type().unwrap_or(value_type.as_str()) {
        Ok(())
    } else {
        Err(InvalidValueRetrieval { type_: value_type.as_str().to_string() })
    };
    may_error.check(result);
}

#[apply(generic_step)]
#[step(
    expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get {value_type} {is_or_not} none"
)]
pub async fn answer_get_row_get_variable_try_get_specific_value_is_none(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    value_type: params::ValueType,
    is_or_not: IsOrNot,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    is_or_not.check_none(&concept.try_get_value());
}

#[apply(generic_step)]
#[step(
    expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) try get {value_type} {is_or_not}: {value}"
)]
#[step(
    expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) get {value_type} {is_or_not}: {value}"
)]
pub async fn answer_get_row_get_variable_get_specific_value(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    value_type: params::ValueType,
    is_or_not: IsOrNot,
    value: params::Value,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    assert_eq!(value_type.as_str(), concept.get_value_type().unwrap());
    check_is_value(is_or_not, value, concept.get_value(), concept.get_value_type().unwrap());
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is boolean: {boolean}")]
pub async fn answer_get_row_get_variable_is_boolean(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_boolean: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_boolean, concept.get_value_type_or_none() == "boolean");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is integer: {boolean}")]
pub async fn answer_get_row_get_variable_is_integer(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_integer: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_integer, concept.get_value_type_or_none() == "integer");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is decimal: {boolean}")]
pub async fn answer_get_row_get_variable_is_decimal(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_decimal: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_decimal, concept.get_value_type_or_none() == "decimal");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is double: {boolean}")]
pub async fn answer_get_row_get_variable_is_double(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_double: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_double, concept.get_value_type_or_none() == "double");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is string: {boolean}")]
pub async fn answer_get_row_get_variable_is_string(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_string: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_string, concept.get_value_type_or_none() == "string");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is date: {boolean}")]
pub async fn answer_get_row_get_variable_is_date(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_date: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_date, concept.get_value_type_or_none() == "date");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is datetime: {boolean}")]
pub async fn answer_get_row_get_variable_is_datetime(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_datetime: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_datetime, concept.get_value_type_or_none() == "datetime");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is datetime-tz: {boolean}")]
pub async fn answer_get_row_get_variable_is_datetime_tz(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_datetime_tz: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_datetime_tz, concept.get_value_type_or_none() == "datetime-tz");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is duration: {boolean}")]
pub async fn answer_get_row_get_variable_is_duration(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_duration: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    check_boolean!(is_duration, concept.get_value_type_or_none() == "duration");
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get {concept_kind}{is_by_var_index}\({var}\) is struct: {boolean}")]
pub async fn answer_get_row_get_variable_is_struct(
    context: &mut Context,
    index: usize,
    var_kind: ConceptKind,
    is_by_var_index: IsByVarIndex,
    var: Var,
    is_struct: params::Boolean,
) {
    if matches!(is_by_var_index, IsByVarIndex::Is) {
        return; // http does not have indices
    }
    let concept = get_answer_rows_var(context, index, var).unwrap().clone().into();
    check_concept_is_kind(&concept, var_kind, params::Boolean::True);
    // todo: implement structs checks
}

#[apply(generic_step)]
#[step(expr = r"answer get row\({int}\) get concepts size is: {int}")]
pub async fn answer_get_row_get_concepts_size_is(context: &mut Context, index: usize, size: usize) {
    let serde_json::Value::Object(value_object) = context.get_answer_row_index(index) else {
        panic!("Expected object");
    };
    assert_eq!(size, value_object.len());
}

#[apply(generic_step)]
#[step(expr = r"answer {contains_or_doesnt} document:")]
pub async fn answer_contains_document(
    context: &mut Context,
    contains_or_doesnt: params::ContainsOrDoesnt,
    step: &Step,
) {
    let expected_document = parse_json(step.docstring().unwrap());
    let concept_documents = context.get_answer().unwrap().answers.as_ref().unwrap();
    contains_or_doesnt.check_bool(
        list_contains_json(concept_documents, &expected_document),
        &format!("Concept documents: {:?}", concept_documents),
    );
}
