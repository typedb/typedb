/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{
    insert::{program::InsertProgram, WriteCompilationError},
    match_::inference::annotated_functions::IndexedAnnotatedFunctions,
};
use cucumber::gherkin::Step;
use executor::{batch::Row};
use ir::{
    program::function_signature::HashMapFunctionSignatureIndex,
    translation::{match_::translate_match, TranslationContext},
};
use itertools::Itertools;
use macro_rules_attribute::apply;
use executor::write::insert::InsertExecutor;
use executor::write::WriteError;
use primitive::either::Either;
use query::query_manager::QueryManager;

use crate::{
    generic_step,
    params::{check_boolean, MayError},
    transaction_context::{with_read_tx, with_schema_tx, with_write_tx},
    Context,
};

fn create_insert_plan(context: &mut Context, query_str: &str) -> Result<InsertProgram, WriteCompilationError> {
    with_write_tx!(context, |tx| {
        // TODO: this needs to handle match-insert pipelines
        let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
        let mut translation_context = TranslationContext::new();
        let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();
        let mock_annotations = {
            let mut dummy_for_annotations = query_str.clone().replace("insert", "match");
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
                tx.snapshot.as_ref(),
                tx.type_manager.as_ref(),
                &IndexedAnnotatedFunctions::empty(),
                &translation_context.variable_registry,
            )
            .unwrap()
            .0
        };
        compiler::insert::program::compile(block.conjunction().constraints(), &HashMap::new(), &mock_annotations)
    })
}

fn execute_insert_plan(
    context: &mut Context,
    insert_plan: InsertProgram,
) -> Result<Vec<VariableValue<'static>>, WriteError> {
    let mut output_vec = (0..insert_plan.output_row_schema.len()).map(|_| VariableValue::Empty).collect_vec();

    with_write_tx!(context, |tx| {
        InsertExecutor::new(insert_plan).execute_insert(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.thing_manager,
            &mut Row::new(output_vec.as_mut_slice(), &mut 1),
        )?;
    });
    Ok(output_vec)
}

fn execute_insert_query(
    context: &mut Context,
    query: &str,
) -> Result<Vec<VariableValue<'static>>, Either<WriteCompilationError, WriteError>> {
    let insert_plan = create_insert_plan(context, query).map_err(Either::First)?;
    execute_insert_plan(context, insert_plan).map_err(Either::Second)
}

#[apply(generic_step)]
#[step(expr = r"typeql define{may_error}")]
async fn typeql_define(context: &mut Context, may_error: MayError, step: &Step) {
    let typeql_define = typeql::parse_query(step.docstring.as_ref().unwrap().as_str()).unwrap().into_schema();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            typeql_define,
        );
        may_error.check(&result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql insert{may_error}")]
async fn typeql_insert(context: &mut Context, may_error: MayError, step: &Step) {
    let result = execute_insert_query(context, step.docstring.as_ref().unwrap().as_str());
    may_error.check_either_err(&result);
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql insert")]
async fn get_answers_of_typeql_insert(context: &mut Context, step: &Step) {
    let result = execute_insert_query(context, step.docstring.as_ref().unwrap().as_str());
    match result {
        Err(Either::First(err)) => panic!("{:?}", err),
        Err(Either::Second(err)) => panic!("{:?}", err),
        _ => {}
    }
    println!("insert is done; get is ignored for get answers of typeql insert");
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql get")]
async fn get_answers_of_typeql_get(context: &mut Context, step: &Step) {
    let typeql_get = step.docstring.as_ref().unwrap().as_str();
    with_read_tx!(context, |tx| {
        // Can't read_tx because execute always takes a mut snapshot
        // let result = QueryManager::new().execute(Arc::get_mut(&mut tx.snapshot).unwrap(), &tx.type_manager, typeql_get);
        println!("TypeQL get is ignored!")
    });
}

#[apply(generic_step)]
#[step(expr = r"uniquely identify answer concepts")]
async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
    let table = step.table.as_ref().unwrap();
    with_read_tx!(context, |tx| {
        // Can't read_tx because execute always takes a mut snapshot
        println!("Uniquely identify is ignored!")
    });
}

#[apply(generic_step)]
#[step(expr = r"set time-zone is: {word}\/{word}")] // TODO: Maybe make time-zone a param
async fn set_timezone_is(context: &mut Context, step: &Step) {
    println!("Set timezone is ignored!")
}

#[apply(generic_step)]
#[step(expr = r"answer size is: {int}")] // TODO: Maybe make time-zone a param
async fn answer_size_is(context: &mut Context, answer_size: i32, step: &Step) {
    println!("answer size is ignored!")
}
