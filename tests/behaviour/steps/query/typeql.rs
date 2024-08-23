/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::{answer_map::AnswerMap, variable_value::VariableValue};
use compiler::{
    insert::WriteCompilationError,
    match_::{
        build_match_plan,
        inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
        planner::program_plan::ProgramPlan,
    },
};
use concept::error::ConceptReadError;
use cucumber::gherkin::Step;
use executor::{
    batch::Row,
    program_executor::ProgramExecutor,
    write::{insert::InsertExecutor, insert_executor::WriteError, WriteError},
};
use ir::{
    program::function_signature::HashMapFunctionSignatureIndex,
    translation::{match_::translate_match, TranslationContext},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;
use primitive::either::Either;
use query::query_manager::QueryManager;
use typeql::Query;

use crate::{
    generic_step,
    params::{self, check_boolean, MayError},
    transaction_context::{with_read_tx, with_schema_tx, with_write_tx},
    Context,
};

fn execute_match_query(
    context: &mut Context,
    query: &str,
) -> Result<Vec<VariableValue<'static>>, Either<WriteCompilationError, WriteError>> {
    let mut translation_context = TranslationContext::new();
    let typeql_match = typeql::parse_query(query).unwrap().into_pipeline().stages.pop().unwrap().into_match();
    let block = ir::translation::match_::translate_match(
        &mut translation_context,
        &HashMapFunctionSignatureIndex::empty(),
        &typeql_match,
    )
    .unwrap()
    .finish();

    let rows = with_read_tx!(context, |tx| {
        let (type_annotations, _) = infer_types(
            &block,
            Vec::new(),
            &*tx.snapshot,
            &tx.type_manager,
            &IndexedAnnotatedFunctions::empty(),
            &translation_context.variable_registry,
        )
        .unwrap();
        let match_plan = build_match_plan(&block, &type_annotations, &HashMap::new(), &tx.thing_manager);
        let program_plan = ProgramPlan::new(match_plan, HashMap::new(), HashMap::new());
        let executor = ProgramExecutor::new(&program_plan, &*tx.snapshot, &tx.thing_manager).map_err(Either::Second)?;
        executor
            .into_iterator(tx.snapshot.clone(), tx.thing_manager.clone())
            .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
            .into_iter()
            .try_collect::<_, Vec<_>, _>()
            .map_err(Either::Second)?
    });
    Ok(Vec::new())
}

fn execute_insert_query(
    context: &mut Context,
    query: Query,
) -> Result<Vec<VariableValue<'static>>, Either<WriteCompilationError, WriteError>> {
    // TODO: this needs to handle match-insert pipelines
    let typeql_insert = typeql::parse_query(query).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let mut translation_context = TranslationContext::new();
    let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();

    let mock_annotations = with_read_tx!(context, |tx| {
        let dummy_for_annotations = query.to_string().replacen("insert", "match", 1);
        let mut ctx = TranslationContext::new();
        let block = translate_match(
            &mut ctx,
            &HashMapFunctionSignatureIndex::empty(),
            &typeql::parse_query(dummy_for_annotations.as_str())
                .unwrap()
                .into_pipeline()
                .stages
                .pop()
                .unwrap()
                .into_match(),
        )
        .unwrap()
        .finish();
        compiler::match_::inference::type_inference::infer_types(
            &block,
            vec![],
            &tx.snapshot,
            &tx.type_manager,
            &IndexedAnnotatedFunctions::empty(),
            &translation_context.variable_registry,
        )
        .unwrap()
        .0
    });

    let insert_plan =
        compiler::insert::program::compile(block.conjunction().constraints(), &HashMap::new(), &mock_annotations)
            .map_err(Either::First)?;

    let mut output_vec = vec![VariableValue::Empty; insert_plan.n_created_concepts];
    with_write_tx!(context, |tx| {
        executor::write::insert_executor::execute_insert(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.thing_manager,
            &insert_plan,
            &Row::new(&mut [], &mut 1),
            Row::new(&mut output_vec, &mut 1),
        )
        .map_err(Either::Second)?;
    });

    Ok(output_vec)
}

#[apply(generic_step)]
#[step(expr = r"typeql define{typeql_may_error}")]
async fn typeql_define(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query_parsed = typeql::parse_query(step.docstring.as_ref().unwrap().as_str());
    if may_error.check_parsing(&query_parsed).is_some() {
        return;
    }

    let typeql_define = query_parsed.unwrap().into_schema();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            typeql_define,
        );
        may_error.check_logic(&result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql write query{may_error}")]
async fn typeql_write(context: &mut Context, may_error: MayError, step: &Step) {
    let result = execute_insert_query(context, step.docstring.as_ref().unwrap().as_str());
    assert_eq!(may_error.expects_error(), result.is_err());
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql write query")]
async fn get_answers_of_typeql_write(context: &mut Context, step: &Step) {
    execute_insert_query(context, step.docstring.as_ref().unwrap().as_str()).unwrap();
    todo!()
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql read query")]
async fn get_answers_of_typeql_read(context: &mut Context, step: &Step) {
    let query = step.docstring.as_ref().unwrap().as_str();
    execute_match_query(context, query).unwrap();
    with_read_tx!(context, |tx| {
        //
        todo!()
    });
}

#[apply(generic_step)]
#[step(expr = r"uniquely identify answer concepts")]
async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
    let table = step.table.as_ref().unwrap();
    with_read_tx!(context, |tx| {
        // Can't read_tx because execute always takes a mut snapshot
        todo!()
    });
}

#[apply(generic_step)]
#[step(expr = r"set time-zone is: {word}\/{word}")] // TODO: Maybe make time-zone a param
async fn set_timezone_is(context: &mut Context, step: &Step) {
    todo!()
}

#[apply(generic_step)]
#[step(expr = r"answer size is: {int}")] // TODO: Maybe make time-zone a param
async fn answer_size_is(context: &mut Context, answer_size: i32, step: &Step) {
    todo!()
}
