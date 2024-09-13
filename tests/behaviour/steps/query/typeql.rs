/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr, sync::Arc};

use answer::{answer_map::AnswerMap, variable_value::VariableValue, Thing};
use compiler::{
    insert::WriteCompilationError,
    match_::{
        inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
        planner::{pattern_plan::MatchProgram, program_plan::ProgramPlan},
    },
    VariablePosition,
};
use concept::{
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{type_manager::TypeManager, TypeAPI},
};
use cucumber::gherkin::Step;
use database::transaction::{TransactionRead, TransactionSchema, TransactionWrite};
use encoding::value::label::Label;
use error::TypeDBError;
use executor::{
    batch::Batch,
    error::ReadExecutionError,
    pipeline::{
        stage::{StageAPI, StageContext, WritePipelineStage, WriteStageIterator},
        PipelineExecutionError,
    },
    program_executor::ProgramExecutor,
    row::{MaybeOwnedRow, Row},
    write::{insert::InsertExecutor, WriteError},
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use futures::TryFutureExt;
use ir::{
    program::function_signature::HashMapFunctionSignatureIndex,
    translation::{match_::translate_match, TranslationContext},
};
use itertools::{izip, Itertools};
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;
use primitive::either::Either;
use query::{error::QueryError, query_manager::QueryManager};
use storage::snapshot::WritableSnapshot;
use typeql::{query::Pipeline, Query};

use crate::{
    assert::assert_matches,
    generic_step, params,
    transaction_context::{
        with_read_tx, with_schema_tx, with_write_tx, with_write_tx_deconstructed, ActiveTransaction,
    },
    util::iter_table_map,
    Context,
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
) -> Result<Vec<HashMap<String, VariableValue<'static>>>, Either<WriteCompilationError, WriteError>> {
    with_write_tx_deconstructed!(context, |snapshot,
                                           type_manager,
                                           thing_manager,
                                           function_manager,
                                           database,
                                           _opts| {
        let pipeline = QueryManager {}
            .prepare_write_pipeline(
                Arc::into_inner(snapshot).unwrap(),
                &type_manager,
                thing_manager.clone(),
                &function_manager,
                &query.into_pipeline(),
            )
            .unwrap();
        let (_iterator, StageContext { snapshot, .. }) =
            pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        ((), snapshot)
    });

    let result_as_answers = match result_as_batch {
        Ok(batch) => Ok(batch_result_to_answer(batch, named_outputs)),
        Err(typedb_source) => Err(QueryError::WritePipelineExecutionError { typedb_source }),
    };

    (snapshot, result_as_answers)
}

#[apply(generic_step)]
#[step(expr = r"typeql define{typeql_may_error}")]
async fn typeql_define(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query);
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_transaction();
        return;
    }
    let typeql_define = parse_result.unwrap().into_schema();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            typeql_define,
        );
        may_error.check_logic(result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql redefine{typeql_may_error}")]
async fn typeql_redefine(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query);
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_transaction();
        return;
    }
    let typeql_redefine = parse_result.unwrap().into_schema();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            typeql_redefine,
        );
        may_error.check_logic(result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql write query{typeql_may_error}")]
async fn typeql_write(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let parse_result = typeql::parse_query(step.docstring.as_ref().unwrap().as_str());
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_transaction();
        return;
    }
    let query = parse_result.unwrap();
    let (tx, result) = execute_write_query(context.active_transaction.take().unwrap(), query);
    context.active_transaction = Some(tx);
    may_error.check_logic(result);
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql write query")]
async fn get_answers_of_typeql_write(context: &mut Context, step: &Step) {
    let query = typeql::parse_query(step.docstring.as_ref().unwrap().as_str()).unwrap();
    let tx = context.active_transaction.take().unwrap();
    let (tx, result) = execute_write_query(tx, query);
    context.active_transaction = Some(tx);
    context.answers = result.unwrap();
}

#[apply(generic_step)]
#[step(expr = r"typeql read query{typeql_may_error}")]
async fn typeql_read(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let parse_result = typeql::parse_query(step.docstring.as_ref().unwrap().as_str());
    if may_error.check_parsing(parse_result.as_ref()).is_some() {
        context.close_transaction();
        return;
    }
    let query = parse_result.unwrap();
    let result = execute_read_query(context, query);
    may_error.check_logic(result);
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql read query")]
async fn get_answers_of_typeql_read(context: &mut Context, step: &Step) {
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
            let is_a_match = row.iter().all(|(&var, &spec)| {
                let (kind, id) =
                    spec.split_once(':').expect("answer concept specifier must be of the form `<kind>:<id>`");
                let var_value = answer_row
                    .get(var)
                    .unwrap_or_else(|| panic!("no answer found for {var} in one of the answer rows"));
                match kind {
                    "label" => does_type_match(context, var_value, id),
                    "key" => does_key_match(var, id, var_value, context),
                    "attr" => does_attribute_match(id, var_value, context),
                    "value" => todo!("value: {spec}"),
                    _ => panic!("unrecognised concept kind: {kind}"),
                }
            });
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
