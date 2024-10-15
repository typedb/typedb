/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr, sync::Arc};

use answer::{variable_value::VariableValue, Thing};
use compiler::VariablePosition;
use concept::{thing::object::ObjectAPI, type_::TypeAPI};
use cucumber::gherkin::Step;
use encoding::value::{label::Label, value_type::ValueType, ValueEncodable};
use executor::{
    batch::Batch,
    pipeline::stage::{ExecutionContext, StageAPI, StageIterator},
    ExecutionInterrupt,
};
use futures::TryFutureExt;
use itertools::Itertools;
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;
use query::{error::QueryError, query_manager::QueryManager};
use test_utils::assert_matches;

use crate::{
    connection::BehaviourConnectionTestExecutionError,
    generic_step, params,
    transaction_context::{
        with_read_tx, with_schema_tx, with_write_tx_deconstructed,
        ActiveTransaction::{Read, Schema},
    },
    util::iter_table_map,
    BehaviourTestExecutionError, Context,
};

fn batch_result_to_answer(
    batch: Batch,
    selected_outputs: HashMap<String, VariablePosition>,
) -> Vec<HashMap<String, VariableValue<'static>>> {
    batch
        .into_iterator_mut()
        .map_static(move |row| {
            let answer_map: HashMap<String, VariableValue<'static>> = selected_outputs
                .iter()
                .map(|(v, p)| (v.clone().to_owned(), row.get(*p).clone().into_owned()))
                .collect::<HashMap<_, _>>();
            answer_map
        })
        .collect::<Vec<HashMap<String, VariableValue<'static>>>>()
}

fn execute_read_query(
    context: &mut Context,
    query: typeql::Query,
) -> Result<Vec<HashMap<String, VariableValue<'static>>>, QueryError> {
    with_read_tx!(context, |tx| {
        let pipeline = QueryManager {}.prepare_read_pipeline(
            tx.snapshot.clone(),
            &tx.type_manager,
            tx.thing_manager.clone(),
            &tx.function_manager,
            &query.into_pipeline(),
        )?;
        let named_outputs = pipeline.rows_positions().unwrap().clone();
        let result_as_batch = match pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()) {
            Ok((iterator, _)) => iterator.collect_owned(),
            Err((err, _)) => {
                return Err(QueryError::ReadPipelineExecutionError { typedb_source: err });
            }
        };
        match result_as_batch {
            Ok(batch) => Ok(batch_result_to_answer(batch, named_outputs)),
            Err(typedb_source) => Err(QueryError::ReadPipelineExecutionError { typedb_source }),
        }
    })
}

fn execute_write_query(
    context: &mut Context,
    query: typeql::Query,
) -> Result<Vec<HashMap<String, VariableValue<'static>>>, BehaviourTestExecutionError> {
    if matches!(context.active_transaction.as_ref().unwrap(), Read(_)) {
        return Err(BehaviourTestExecutionError::UseInvalidTransactionAsWrite);
    }

    with_write_tx_deconstructed!(context, |snapshot, type_manager, thing_manager, function_manager, _db, _opts| {
        let pipeline = QueryManager {}
            .prepare_write_pipeline(
                Arc::into_inner(snapshot).unwrap(),
                &type_manager,
                thing_manager.clone(),
                &function_manager,
                &query.into_pipeline(),
            )
            .unwrap();
        let named_outputs = pipeline.rows_positions().unwrap().clone();

        let (result_as_batch, snapshot) = match final_stage.into_iterator(ExecutionInterrupt::new_uninterruptible()) {
            Ok((iterator, ExecutionContext { snapshot, .. })) => (iterator.collect_owned(), snapshot),
            Err((err, ExecutionContext { .. })) => {
                return Err(BehaviourTestExecutionError::Query(QueryError::ReadPipelineExecutionError {
                    typedb_source: err,
                }));
            }
        };

        match result_as_batch {
            Ok(batch) => (Ok(batch_result_to_answer(batch, named_outputs)), snapshot),
            Err(typedb_source) => (
                Err(BehaviourTestExecutionError::Query(QueryError::WritePipelineExecutionError { typedb_source })),
                snapshot,
            ),
        }
    })
}

#[apply(generic_step)]
#[step(expr = r"typeql schema query{typeql_may_error}")]
async fn typeql_schema_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query);
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_active_transaction();
        return;
    }
    let typeql_schema = parse_result.unwrap().into_schema();

    if !matches!(context.active_transaction.as_ref().unwrap(), Schema(_)) {
        may_error.check_logic::<(), BehaviourTestExecutionError>(Err(
            BehaviourTestExecutionError::UseInvalidTransactionAsSchema,
        ));
        return;
    }

    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            typeql_schema,
        );
        may_error.check_logic(result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql write query{typeql_may_error}")]
async fn typeql_write_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let parse_result = typeql::parse_query(step.docstring.as_ref().unwrap().as_str());
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_active_transaction();
        return;
    }
    let query = parse_result.unwrap();

    let result = execute_write_query(context, query);
    may_error.check_logic(result);
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql write query")]
async fn get_answers_of_typeql_write_query(context: &mut Context, step: &Step) {
    let query = typeql::parse_query(step.docstring.as_ref().unwrap().as_str()).unwrap();
    let result = execute_write_query(context, query);
    context.answers = result.unwrap();
}

#[apply(generic_step)]
#[step(expr = r"typeql read query{typeql_may_error}")]
async fn typeql_read_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let parse_result = typeql::parse_query(step.docstring.as_ref().unwrap().as_str());
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_active_transaction();
        return;
    }
    let query = parse_result.unwrap();
    let result = execute_read_query(context, query);
    may_error.check_logic(result);
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql read query")]
async fn get_answers_of_typeql_read_query(context: &mut Context, step: &Step) {
    let query = typeql::parse_query(step.docstring.as_ref().unwrap().as_str()).unwrap();
    context.answers = execute_read_query(context, query).unwrap();
}

#[apply(generic_step)]
#[step(expr = r"uniquely identify answer concepts")]
async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
    let num_specs = step.table().unwrap().rows.len() - 1;
    let num_answers = context.answers.len();
    assert_eq!(
        num_specs, num_answers,
        "expected the number of identifier entries to match the number of answers, found {} entries and {} answers",
        num_specs, num_answers
    );
    for row in iter_table_map(step) {
        let mut num_matches = 0;
        for answer_row in &context.answers {
            let is_a_match = row.iter().all(|(&var, &spec)| does_var_in_row_match_spec(context, answer_row, var, spec));
            if is_a_match {
                num_matches += 1;
            }
        }
        assert_eq!(
            num_matches, 1,
            "each identifier row must match exactly one answer map; found {num_matches} for row {row:?}"
        )
    }
}

#[apply(generic_step)]
#[step(expr = r"result is a single row with variable '{word}': {word}")]
async fn single_row_result_with_variable_value(
    context: &mut Context,
    variable_name: String,
    spec: String,
    step: &Step,
) {
    assert_eq!(context.answers.len(), 1, "Expected single row, received {}", context.answers.len());
    assert!(
        does_var_in_row_match_spec(context, &context.answers[0], variable_name.as_str(), spec.as_str()),
        "Result did not match expected: {:?} != {}",
        &context.answers[0],
        spec.as_str()
    );
}

fn does_var_in_row_match_spec(
    context: &Context,
    answer_row: &HashMap<String, VariableValue<'static>>,
    var: &str,
    spec: &str,
) -> bool {
    let var_value =
        answer_row.get(var).unwrap_or_else(|| panic!("no answer found for {var} in one of the answer rows"));
    if spec == "empty" {
        var_value == &VariableValue::Empty
    } else {
        let (kind, id) = spec.split_once(':').expect("answer concept specifier must be of the form `<kind>:<id>`");
        match kind {
            "label" => does_type_match(context, var_value, id),
            "key" => does_key_match(var, id, var_value, context),
            "attr" => does_attribute_match(id, var_value, context),
            "value" => does_value_match(id, var_value, context),
            _ => panic!("unrecognised concept kind: {kind}"),
        }
    }
}

fn does_key_match(var: &str, id: &str, var_value: &VariableValue<'_>, context: &Context) -> bool {
    let VariableValue::Thing(thing) = var_value else {
        return false;
    };
    let (key_label, key_value) =
        id.split_once(':').expect("key concept specifier must be of the form `key:<type>:<value>`");
    with_read_tx!(context, |tx| {
        let key_type = tx
            .type_manager
            .get_attribute_type(&*tx.snapshot, &Label::build(key_label))
            .unwrap()
            .unwrap_or_else(|| panic!("attribute type {key_label} not found"));
        let expected = params::Value::from_str(key_value).unwrap().into_typedb(
            key_type
                .get_value_type_without_source(&*tx.snapshot, &tx.type_manager)
                .unwrap()
                .unwrap_or_else(|| panic!("expected the key type {key_label} to have a value type")),
        );
        let mut has_iter = match thing {
            Thing::Entity(entity) => entity.get_has_type_unordered(&*tx.snapshot, &tx.thing_manager, key_type).unwrap(),
            Thing::Relation(relation) => {
                relation.get_has_type_unordered(&*tx.snapshot, &tx.thing_manager, key_type).unwrap()
            }
            Thing::Attribute(_) => return false,
        };
        let (attr, count) = has_iter
            .next()
            .unwrap_or_else(|| panic!("no attributes of type {key_label} found for {var}: {thing}"))
            .unwrap();
        assert_eq!(count, 1, "expected exactly one {key_label} for {var}, found {count}");
        let actual = attr.get_value(&*tx.snapshot, &tx.thing_manager);
        if actual.unwrap() != expected {
            return false;
        }
        assert_matches!(has_iter.next(), None, "multiple keys found for {}", var);
    });
    true
}

fn does_attribute_match(id: &str, var_value: &VariableValue<'_>, context: &Context) -> bool {
    let (label, value) = id.split_once(':').expect("attribute specifier must be of the form `attr:<type>:<value>`");
    let VariableValue::Thing(Thing::Attribute(attr)) = var_value else {
        return false;
    };
    with_read_tx!(context, |tx| {
        let attr_type = tx
            .type_manager
            .get_attribute_type(&*tx.snapshot, &Label::build(label))
            .unwrap()
            .unwrap_or_else(|| panic!("attribute type {label} not found"));
        let expected = params::Value::from_str(value).unwrap().into_typedb(
            attr_type
                .get_value_type_without_source(&*tx.snapshot, &tx.type_manager)
                .unwrap()
                .unwrap_or_else(|| panic!("expected the key type {label} to have a value type")),
        );
        let attr = attr.as_reference();
        let actual = attr.get_value(&*tx.snapshot, &tx.thing_manager).unwrap();
        actual == expected
    })
}

fn does_value_match(id: &str, var_value: &VariableValue<'_>, context: &Context) -> bool {
    let VariableValue::Value(value) = var_value else {
        return false;
    };
    let (id_type, id_value) = id.split_once(":").unwrap();
    let expected_value_type = match id_type {
        "long" => ValueType::Long,
        "double" => ValueType::Double,
        _ => todo!(),
    };
    let expected = params::Value::from_str(id_value).unwrap().into_typedb(expected_value_type);
    if expected.value_type() == ValueType::Double {
        let precision = id_value.split_once(".").map(|(_, decimal)| decimal.len()).unwrap_or(5) as i32;
        let epsilon = 0.5 * 10.0f64.powi(-1 * precision);
        f64::abs(expected.clone().unwrap_double() - var_value.as_value().clone().unwrap_double()) < epsilon
    } else {
        &expected == var_value.as_value()
    }
}

fn does_type_match(context: &Context, var_value: &VariableValue<'_>, expected: &str) -> bool {
    let VariableValue::Type(type_) = var_value else {
        return false;
    };
    let label = with_read_tx!(context, |tx| {
        match type_ {
            answer::Type::Entity(type_) => type_.get_label(&*tx.snapshot, &tx.type_manager),
            answer::Type::Relation(type_) => type_.get_label(&*tx.snapshot, &tx.type_manager),
            answer::Type::Attribute(type_) => type_.get_label(&*tx.snapshot, &tx.type_manager),
            answer::Type::RoleType(type_) => type_.get_label(&*tx.snapshot, &tx.type_manager),
        }
        .unwrap()
    });
    label.scoped_name().as_str() == expected
}

#[apply(generic_step)]
#[step(expr = r"answer size is: {int}")]
async fn answer_size_is(context: &mut Context, answer_size: i32) {
    assert_eq!(context.answers.len(), answer_size as usize)
}

#[apply(generic_step)]
#[step(expr = r"order of answer concepts is")]
async fn order_of_answers_is(context: &mut Context, step: &Step) {
    let num_specs = step.table().unwrap().rows.len() - 1;
    let num_answers = context.answers.len();
    assert_eq!(
        num_specs, num_answers,
        "expected the number of identifier entries to match the number of answers, found {} entries and {} answers",
        num_specs, num_answers
    );
    for (spec_row, answer_row) in iter_table_map(step).zip(&context.answers) {
        assert!(
            spec_row.iter().all(|(&var, &spec)| does_var_in_row_match_spec(context, answer_row, var, spec)),
            "The answer found did not match the specified row {spec_row:?}"
        );
    }
}
