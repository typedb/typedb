/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use compiler::inference::annotated_functions::AnnotatedCommittedFunctions;
use cucumber::gherkin::Step;
use executor::{batch::Row, insert_executor::InsertExecutor};
use ir::program::{function_signature::HashMapFunctionIndex, program::Program};
use macro_rules_attribute::apply;
use query::query_manager::QueryManager;

use crate::{
    generic_step,
    params::{self, check_boolean, MayError},
    transaction_context::{with_read_tx, with_schema_tx, with_write_tx},
    Context,
};

#[apply(generic_step)]
#[step(expr = r"typeql define{may_error}")]
async fn typeql_define(context: &mut Context, may_error: MayError, step: &Step) {
    let typeql_define = step.docstring.as_ref().unwrap().as_str();
    with_schema_tx!(context, |tx| {
        let result = QueryManager::new().execute(&mut tx.snapshot, &tx.type_manager, typeql_define);
        assert_eq!(may_error.expects_error(), result.is_err(), "{:?}", result);
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql insert{may_error}")]
async fn typeql_insert(context: &mut Context, may_error: MayError, step: &Step) {
    with_write_tx!(context, |tx| {
        let typeql_insert = typeql::parse_query(step.docstring.as_ref().unwrap().as_str())
            .unwrap()
            .into_pipeline()
            .stages
            .pop()
            .unwrap()
            .into_insert();
        let block =
            ir::translation::insert::translate_insert(&HashMapFunctionIndex::empty(), &typeql_insert).unwrap().finish();
        let annotated_program = compiler::inference::type_inference::infer_types(
            Program::new(block, vec![]),
            &tx.snapshot,
            &tx.type_manager,
            Arc::new(AnnotatedCommittedFunctions::new(vec![].into_boxed_slice(), vec![].into_boxed_slice())),
        )
        .unwrap();
        let insert_plan = compiler::planner::insert_planner::build_insert_plan(
            &HashMap::new(),
            annotated_program.get_entry_annotations(),
            annotated_program.get_entry().conjunction().constraints(),
        )
        .unwrap();

        println!("{:?}", &insert_plan.instructions);
        insert_plan.debug_info.iter().for_each(|(k, v)| {
            println!("{:?} -> {:?}", k, annotated_program.get_entry().context().get_variables_named().get(v))
        });
        let mut executor = InsertExecutor::new(insert_plan);
        executor::insert_executor::execute(
            &mut tx.snapshot,
            &tx.thing_manager,
            &mut executor,
            &Row::new(vec![].as_mut_slice(), &mut 1),
        )
        .unwrap();
    });
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql get")]
async fn typeql_get(context: &mut Context, step: &Step) {
    let typeql_get = step.docstring.as_ref().unwrap().as_str();
    with_read_tx!(context, |tx| {
        // Can't read_tx because execute always takes a mut snapshot
        // let result = QueryManager::new().execute(&mut tx.snapshot, &tx.type_manager, typeql_get);
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
