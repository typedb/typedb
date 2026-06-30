/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt::Write, iter, str::FromStr, sync::Arc};

use answer::{Thing, variable_value::VariableValue};
use compiler::VariablePosition;
use concept::{
    error::ConceptReadError,
    thing::{ThingAPI, attribute::Attribute, entity::Entity, object::ObjectAPI, relation::Relation},
    type_::TypeAPI,
};
use cucumber::gherkin::Step;
use encoding::{
    Prefixed,
    graph::{
        thing::{
            ThingVertex,
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_object::{ObjectID, ObjectVertex},
        },
        type_::vertex::TypeID,
    },
    layout::prefix::Prefix,
    value::{ValueEncodable, label::Label, value_type::ValueType},
};
use executor::{
    ExecutionInterrupt,
    batch::Batch,
    pipeline::stage::{ExecutionContext, StageIterator},
};
use futures::StreamExt;
use itertools::{Either, Itertools};
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;
use query::{
    analyse::AnalysedQuery,
    error::QueryError,
    given_rows::{GivenRowEntry, GivenRowsSimple},
    query_manager::{QueryContext, TranslatedQuery, translate_pipeline},
};
use resource::profile::StorageCounters;
use server::service::http::message::analyze::{
    annotations::bdd::{
        encode_fetch_annotations_as_functor, encode_function_annotations_as_functor,
        encode_pipeline_annotations_as_functor, encode_pipeline_given_annotations_as_functor,
    },
    encode_analyzed_query,
    structure::bdd::{
        encode_function_structure_as_functor, encode_pipeline_given_as_functor, encode_pipeline_structure_as_functor,
    },
};
use test_utils::assert_matches;

use crate::{
    BehaviourTestExecutionError, Context, generic_step,
    query_answer_context::{QueryAnswer, with_rows_answer},
    transaction_context::{
        ActiveTransaction::{Read, Schema},
        with_read_tx, with_schema_tx, with_write_tx_deconstructed,
    },
    util::{iter_table_map, list_contains_json, parse_json},
};

fn row_batch_result_to_answer(
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
            iter::repeat(answer_map).take(row.get_multiplicity() as usize)
        })
        .into_iter()
        .flatten()
        .collect::<Vec<HashMap<String, VariableValue<'static>>>>()
}

fn row_answers_to_string(rows: &[HashMap<String, VariableValue<'static>>]) -> String {
    rows.iter().map(|row| row.iter().map(|(key, value)| format!("{key}: {value}")).join(", ")).join("\n")
}

fn execute_read_query(
    context: &Context,
    query: typeql::Query,
    given_rows: Option<GivenRowsSimple>,
    source_query: &str,
) -> Result<QueryAnswer, Box<QueryError>> {
    with_read_tx!(context, |tx| {
        let parsed_pipeline = query.into_structure().into_pipeline();
        let translated =
            translate_pipeline(tx.snapshot.as_ref(), &tx.function_manager, &parsed_pipeline, source_query)?;
        let pipeline = tx.query_manager.prepare_read_pipeline(
            tx.snapshot.clone(),
            &tx.type_manager,
            tx.thing_manager.clone(),
            tx.function_manager.clone(),
            TranslatedQuery::uninstrumented(source_query.to_string(), translated),
            given_rows,
        )?;
        if pipeline.has_fetch() {
            match pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()) {
                Ok((iterator, ExecutionContext { parameters, .. })) => {
                    let documents = iterator.try_collect().map_err(|err| QueryError::ReadPipelineExecution {
                        source_query: source_query.to_string(),
                        typedb_source: err,
                    })?;
                    Ok(QueryAnswer::ConceptDocuments(documents, parameters))
                }
                Err((err, _)) => Err(Box::new(QueryError::ReadPipelineExecution {
                    source_query: source_query.to_string(),
                    typedb_source: err,
                })),
            }
        } else {
            let named_outputs = pipeline.rows_positions().expect("Expected unfetched result").clone();
            let result_as_batch = match pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()) {
                Ok((iterator, _)) => iterator.collect_owned(),
                Err((err, _)) => {
                    return Err(Box::new(QueryError::ReadPipelineExecution {
                        source_query: source_query.to_string(),
                        typedb_source: err,
                    }));
                }
            };
            match result_as_batch {
                Ok(batch) => Ok(QueryAnswer::ConceptRows(row_batch_result_to_answer(batch, named_outputs))),
                Err(typedb_source) => Err(Box::new(QueryError::ReadPipelineExecution {
                    source_query: source_query.to_string(),
                    typedb_source,
                })),
            }
        }
    })
}

fn execute_write_query(
    context: &mut Context,
    query: typeql::Query,
    given_rows: Option<GivenRowsSimple>,
    source_query: &str,
) -> Result<QueryAnswer, BehaviourTestExecutionError> {
    if matches!(context.transaction().expect("Expected an active transaction"), Read(_)) {
        return Err(BehaviourTestExecutionError::UseInvalidTransactionAsWrite);
    }

    with_write_tx_deconstructed!(context, |snapshot,
                                           type_manager,
                                           thing_manager,
                                           function_manager,
                                           query_manager,
                                           _db,
                                           _opts| {
        let parsed_pipeline = query.into_structure().into_pipeline();
        let translated_result =
            translate_pipeline(snapshot.as_ref(), &function_manager, &parsed_pipeline, source_query);
        match translated_result {
            Err(error) => (Err(BehaviourTestExecutionError::Query(*error)), snapshot),
            Ok(translated) => {
                let pipeline_result = query_manager.prepare_write_pipeline(
                    Arc::try_unwrap(snapshot).unwrap_or_else(|_| panic!("Expected unique ownership of snapshot")),
                    &type_manager,
                    thing_manager.clone(),
                    function_manager.clone(),
                    TranslatedQuery::uninstrumented(source_query.to_string(), translated),
                    given_rows,
                );

                match pipeline_result {
                    Err((snapshot, error)) => (Err(BehaviourTestExecutionError::Query(*error)), Arc::new(snapshot)),
                    Ok(pipeline) => {
                        if pipeline.has_fetch() {
                            match pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()) {
                                Ok((iterator, ExecutionContext { parameters, snapshot, .. })) => (
                                    match iterator.collect() {
                                        Ok(documents) => Ok(QueryAnswer::ConceptDocuments(documents, parameters)),
                                        Err(err) => Err(BehaviourTestExecutionError::Query(
                                            QueryError::WritePipelineExecution {
                                                source_query: source_query.to_string(),
                                                typedb_source: err,
                                            },
                                        )),
                                    },
                                    snapshot,
                                ),
                                Err((err, ExecutionContext { snapshot, .. })) => (
                                    Err(BehaviourTestExecutionError::Query(QueryError::WritePipelineExecution {
                                        source_query: source_query.to_string(),
                                        typedb_source: err,
                                    })),
                                    snapshot,
                                ),
                            }
                        } else {
                            let named_outputs = pipeline.rows_positions().expect("Expected unfetched result").clone();
                            match pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()) {
                                Ok((iterator, ExecutionContext { snapshot, .. })) => {
                                    let result_as_batch = iterator.collect_owned();
                                    match result_as_batch {
                                        Ok(batch) => (
                                            Ok(QueryAnswer::ConceptRows(row_batch_result_to_answer(
                                                batch,
                                                named_outputs,
                                            ))),
                                            snapshot,
                                        ),
                                        Err(typedb_source) => (
                                            Err(BehaviourTestExecutionError::Query(
                                                QueryError::WritePipelineExecution {
                                                    source_query: source_query.to_string(),
                                                    typedb_source,
                                                },
                                            )),
                                            snapshot,
                                        ),
                                    }
                                }
                                Err((err, ExecutionContext { snapshot, .. })) => (
                                    Err(BehaviourTestExecutionError::Query(QueryError::ReadPipelineExecution {
                                        source_query: source_query.to_string(),
                                        typedb_source: err,
                                    })),
                                    snapshot,
                                ),
                            }
                        }
                    }
                }
            }
        }
    })
}

fn execute_analyze(
    context: &mut Context,
    query: typeql::Query,
    source_query: &str,
) -> Result<AnalysedQuery, BehaviourTestExecutionError> {
    with_read_tx!(context, |tx| {
        let parsed_pipeline = query.into_structure().into_pipeline();
        let translated =
            match translate_pipeline(tx.snapshot.as_ref(), &tx.function_manager, &parsed_pipeline, source_query) {
                Ok(translated) => translated,
                Err(source) => return Err(BehaviourTestExecutionError::Query(*source)),
            };
        tx.query_manager
            .analyse(
                tx.snapshot.clone(),
                &tx.type_manager,
                &tx.function_manager,
                TranslatedQuery::uninstrumented(source_query.to_string(), translated),
            )
            .map_err(|source| BehaviourTestExecutionError::Query(*source))
    })
}

fn may_take_given_rows(context: &mut Context, with_given: params::WithGiven) -> Option<GivenRowsSimple> {
    (with_given == params::WithGiven::True).then(|| context.take_given_rows().expect("Expected given rows available"))
}

#[cucumber::given("query is given rows")]
#[cucumber::when("query is given rows")]
async fn given_rows(context: &mut Context, step: &Step) {
    let table = step.table.as_ref().expect("Expected table for given rows");
    let length = table.rows.len();
    let width = table.rows.first().unwrap().len() as u32;
    // First row is the variables
    let variables = table.rows[0].iter().map(|s| s.split(":").next().unwrap().to_owned()).collect();
    let rows = table.rows[1..].iter().map(|row| row.iter().map(parse_query_given_row_entry).collect()).collect();

    context.given_rows = Some(GivenRowsSimple { variables, rows })
}

#[apply(generic_step)]
#[step(expr = "typeql schema query{typeql_may_error}")]
async fn typeql_schema_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let typeql_schema = parse_result.unwrap().into_structure().into_schema();

    if !matches!(context.transaction().expect("Expected an active transaction"), Schema(_)) {
        may_error.check_logic::<(), BehaviourTestExecutionError>(Err(
            BehaviourTestExecutionError::UseInvalidTransactionAsSchema,
        ));
        return;
    }

    with_schema_tx!(context, |tx| {
        let result = tx.query_manager.execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            &tx.function_manager,
            QueryContext::uninstrumented(query.to_string()),
            &typeql_schema,
        );
        if let Either::Right(_err) = may_error.check_logic(result) {
            context.close_active_transaction();
        }
    });
}

#[apply(generic_step)]
#[step(expr = "typeql write query{with_given}{typeql_may_error}")]
async fn typeql_write_query(
    context: &mut Context,
    with_given: params::WithGiven,
    may_error: params::TypeQLMayError,
    step: &Step,
) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query_str);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let query = parse_result.unwrap();
    let given_rows = may_take_given_rows(context, with_given);
    let result = execute_write_query(context, query, given_rows, query_str);
    if let Either::Right(_) = may_error.check_logic(result) {
        context.close_active_transaction();
    }
}

#[apply(generic_step)]
#[step(expr = "get answers of typeql write query{with_given}")]
async fn get_answers_of_typeql_write_query(context: &mut Context, with_given: params::WithGiven, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let query = typeql::parse_query(query_str).unwrap();
    let given_rows = may_take_given_rows(context, with_given);
    let result = execute_write_query(context, query, given_rows, query_str);
    context.query_answer = Some(result.unwrap());
}

#[apply(generic_step)]
#[step(expr = "typeql read query{with_given}{typeql_may_error}")]
async fn typeql_read_query(
    context: &mut Context,
    with_given: params::WithGiven,
    may_error: params::TypeQLMayError,
    step: &Step,
) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query_str);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let query = parse_result.unwrap();
    let given_rows = may_take_given_rows(context, with_given);
    let result = execute_read_query(context, query, given_rows, query_str);
    may_error.check_logic(result); // we don't close read transactions with logical errors
}

fn record_answers_of_typeql_read_query(context: &mut Context, query_str: &str, given_rows: Option<GivenRowsSimple>) {
    let query = typeql::parse_query(query_str).unwrap();
    context.query_answer = match execute_read_query(context, query, given_rows, query_str) {
        Ok(answers) => Some(answers),
        Err(error) => panic!("Unexpected get answers error: {:?}", error),
    }
}

#[apply(generic_step)]
#[step(expr = "get answers of typeql read query{with_given}")]
async fn get_answers_of_typeql_read_query(context: &mut Context, with_given: params::WithGiven, step: &Step) {
    let given_rows = may_take_given_rows(context, with_given);
    record_answers_of_typeql_read_query(context, step.docstring.as_ref().unwrap().as_str(), given_rows);
}

#[cucumber::when("get answers of templated typeql read query")]
#[cucumber::then("get answers of templated typeql read query")]
async fn get_answers_of_templated_typeql_read_query(context: &mut Context, step: &Step) {
    let rows = context.query_answer.as_ref().unwrap().as_rows();
    let [answer] = rows else { panic!("Expected single answer, found {}", rows.len()) };
    let templated_query = step.docstring.as_ref().unwrap().as_str();
    record_answers_of_typeql_read_query(context, &apply_query_template(templated_query, answer), None);
}

#[apply(generic_step)]
#[step("uniquely identify answer concepts")]
async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
    let num_specs = step.table().unwrap().rows.len() - 1;
    with_rows_answer!(context, |query_answer| {
        let num_answers = query_answer.len();
        if num_specs != num_answers {
            panic!(
                "expected the number of identifier entries to match the number of answers, found {} expected entries and {} real answers. Real answers: \n{}",
                num_specs,
                num_answers,
                row_answers_to_string(query_answer)
            )
        }
        for row in iter_table_map(step) {
            let mut num_matches = 0;
            for answer_row in query_answer {
                let table_row_within_answer =
                    row.iter().all(|(&var, &spec)| does_var_in_row_match_spec(context, answer_row, var, spec));
                if table_row_within_answer {
                    num_matches += 1;
                }
            }
            assert_eq!(
                num_matches, 1,
                "each identifier row must match exactly one answer map; found {num_matches} for row {row:?}"
            )
        }
    });
}

#[cucumber::then(expr = "result is a single row with variable '{word}': {word}")]
async fn single_row_result_with_variable_value(context: &mut Context, variable_name: String, spec: String) {
    with_rows_answer!(context, |query_answer| {
        assert_eq!(query_answer.len(), 1, "Expected single row, received {}", query_answer.len());
        assert!(
            does_var_in_row_match_spec(context, &query_answer[0], variable_name.as_str(), spec.as_str()),
            "Result did not match expected: {:?} != {}",
            &query_answer[0],
            spec.as_str()
        );
    });
}

fn does_var_in_row_match_spec(
    context: &Context,
    answer_row: &HashMap<String, VariableValue<'static>>,
    var: &str,
    spec: &str,
) -> bool {
    let var_value =
        answer_row.get(var).unwrap_or_else(|| panic!("no answer found for {var} in one of the answer rows"));
    if spec == "none" {
        var_value == &VariableValue::None
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
            .get_attribute_type(&*tx.snapshot, &Label::build(key_label, None))
            .unwrap()
            .unwrap_or_else(|| panic!("attribute type {key_label} not found"));
        let expected = params::Value::from_str(key_value).unwrap().into_typedb(
            key_type
                .get_value_type_without_source(&*tx.snapshot, &tx.type_manager)
                .unwrap()
                .unwrap_or_else(|| panic!("expected the key type {key_label} to have a value type")),
        );
        let mut attr_iter: Box<dyn Iterator<Item = Result<(Attribute, u64), Box<ConceptReadError>>>> = match thing {
            Thing::Entity(entity) => Box::new(
                entity
                    .get_has_type_unordered(&*tx.snapshot, &tx.thing_manager, key_type, &.., StorageCounters::DISABLED)
                    .unwrap(),
            ),
            Thing::Relation(relation) => Box::new(
                relation
                    .get_has_type_unordered(&*tx.snapshot, &tx.thing_manager, key_type, &.., StorageCounters::DISABLED)
                    .unwrap(),
            ),
            Thing::Attribute(_) => return false,
        };
        let (attr, count) = Iterator::next(&mut attr_iter)
            .unwrap_or_else(|| panic!("no attributes of type {key_label} found for {var}: {thing}"))
            .unwrap();
        assert_eq!(count, 1, "expected exactly one {key_label} for {var}, found {count}");
        let actual = attr.get_value(&*tx.snapshot, &tx.thing_manager, StorageCounters::DISABLED);
        if actual.unwrap() != expected {
            return false;
        }
        assert_matches!(attr_iter.next(), None, "multiple keys found for {}", var);
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
            .get_attribute_type(&*tx.snapshot, &Label::build(label, None))
            .unwrap()
            .unwrap_or_else(|| panic!("attribute type {label} not found"));
        let expected = params::Value::from_str(value).unwrap().into_typedb(
            attr_type
                .get_value_type_without_source(&*tx.snapshot, &tx.type_manager)
                .unwrap()
                .unwrap_or_else(|| panic!("expected the key type {label} to have a value type")),
        );
        let actual = attr.get_value(&*tx.snapshot, &tx.thing_manager, StorageCounters::DISABLED).unwrap();
        actual == expected
    })
}

fn parse_value_type(value_type_string: &str) -> ValueType {
    match value_type_string {
        "boolean" => ValueType::Boolean,
        "integer" => ValueType::Integer,
        "double" => ValueType::Double,
        "decimal" => ValueType::Decimal,
        "date" => ValueType::Date,
        "datetime" => ValueType::DateTime,
        "datetime-tz" => ValueType::DateTimeTZ,
        "duration" => ValueType::Duration,
        "string" => ValueType::String,
        _ => todo!("TypeQL test value type is not covered"),
    }
}

fn does_value_match(id: &str, var_value: &VariableValue<'_>, _context: &Context) -> bool {
    let VariableValue::Value(value) = var_value else {
        return false;
    };
    let (id_type, id_value) = id.split_once(":").unwrap();
    let expected_value_type = parse_value_type(id_type);
    let expected = params::Value::from_str(id_value).unwrap().into_typedb(expected_value_type);
    if expected.value_type() == ValueType::Double {
        let precision = id_value.split_once(".").map(|(_, decimal)| decimal.len()).unwrap_or(5) as i32;
        let epsilon = 0.5 * 10.0f64.powi(-precision);
        f64::abs(expected.clone().unwrap_double() - value.clone().unwrap_double()) < epsilon
    } else {
        &expected == value
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
#[step(expr = "answer size is: {int}")]
async fn answer_size_is(context: &mut Context, answer_size: i32) {
    assert_eq!(context.query_answer.as_ref().unwrap().len(), answer_size as usize)
}

#[cucumber::then("order of answer concepts is")]
async fn order_of_answers_is(context: &mut Context, step: &Step) {
    with_rows_answer!(context, |query_answer| {
        let num_specs = step.table().unwrap().rows.len() - 1;
        let num_answers = query_answer.len();
        assert_eq!(
            num_specs, num_answers,
            "expected the number of identifier entries to match the number of answers, found {} entries and {} answers",
            num_specs, num_answers
        );
        for (spec_row, answer_row) in iter_table_map(step).zip(query_answer) {
            assert!(
                spec_row.iter().all(|(&var, &spec)| does_var_in_row_match_spec(context, answer_row, var, spec)),
                "The answer found did not match the specified row {spec_row:?}"
            );
        }
    });
}

#[cucumber::then(expr = "answer {contains_or_doesnt} document:")]
async fn answer_contains_document(context: &mut Context, contains_or_doesnt: params::ContainsOrDoesnt, step: &Step) {
    let expected_document = parse_json(step.docstring().unwrap());
    with_read_tx!(context, |tx| {
        let documents = context.query_answer.as_ref().unwrap().as_documents_json(
            &*tx.snapshot,
            &tx.type_manager,
            &tx.thing_manager,
        );
        contains_or_doesnt.check_bool(
            list_contains_json(&documents, &expected_document),
            &format!("\nConcept documents: {:?}\nGiven document: {:?}", documents, expected_document),
        );
    });
}

#[cucumber::then(expr = "answers do not contain variable: {word}")]
async fn answers_do_not_contain_variable(context: &mut Context, variable: String) {
    context.query_answer.as_ref().unwrap().as_rows().iter().all(|row| !row.contains_key(&variable));
}

#[cucumber::then("each answer satisfies")]
async fn each_answer_satisfies(context: &mut Context, step: &Step) {
    let templated_query = step.docstring().unwrap();
    for answer in context.query_answer.as_ref().unwrap().as_rows() {
        let query_string = apply_query_template(templated_query, answer);
        let query = typeql::parse_query(&query_string).unwrap();
        let answer_size = match execute_read_query(context, query, None, &query_string) {
            Ok(answers) => answers.len(),
            Err(error) => panic!("Unexpected get answers error: {:?}", error),
        };
        assert_eq!(answer_size, 1);
    }
}

fn apply_query_template(mut template: &str, answer: &HashMap<String, VariableValue<'static>>) -> String {
    fn split_placeholder(template: &str) -> Option<(&str, &str, &str)> {
        let (prefix, tail) = template.split_once('<')?;
        let (placeholder, tail) = tail.split_once('>')?;
        assert!(
            placeholder.starts_with("answer.") && placeholder.ends_with(".iid"),
            "Cannot replace template not based on ID: <{placeholder:?}>"
        );
        let var = placeholder.strip_prefix("answer.")?.strip_suffix(".iid")?;
        Some((prefix, var, tail))
    }

    let mut buf = String::with_capacity(template.len());
    while let Some((prefix, var, tail)) = split_placeholder(template) {
        buf.push_str(prefix);
        let thing = answer.get(var).unwrap().as_thing();
        write!(buf, "{:x}", thing.iid()).unwrap();
        template = tail;
    }
    buf.push_str(template);
    buf
}

#[cucumber::then("verify answer set is equivalent for query")]
async fn verify_answer_set(context: &mut Context, step: &Step) {
    if true {
        eprintln!("TODO: Implement step: verify answer set is equivalent for query");
        return;
    }
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let query = typeql::parse_query(query_str).unwrap();
    let verify_answers = execute_read_query(context, query, None, query_str).unwrap();
    match (&context.query_answer.as_ref().unwrap(), verify_answers) {
        (QueryAnswer::ConceptRows(actual), QueryAnswer::ConceptRows(expected)) => {
            assert_eq!(
                expected.len(),
                actual.len(),
                "expected the number of identifier entries to match the number of answers, found {} entries and {} answers",
                expected.len(),
                actual.len()
            );
            assert!(expected.iter().all(|answer| actual.contains(answer)));
        }
        (
            QueryAnswer::ConceptDocuments(_actual_tree, _actual_params),
            QueryAnswer::ConceptDocuments(_expected_tree, _expected_params),
        ) => {
            todo!()
        }
        (QueryAnswer::ConceptDocuments(_, _), QueryAnswer::ConceptRows(_)) => {
            panic!("Expected rows, found documents")
        }
        (QueryAnswer::ConceptRows(_), QueryAnswer::ConceptDocuments(_, _)) => {
            panic!("Expected documents, found rows")
        }
    }
    let _num_answers = context.query_answer.as_ref().unwrap().as_rows().len();
}

#[cucumber::when("get answers of typeql analyze")]
async fn get_answers_of_typeql_analyze(context: &mut Context, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let query = typeql::parse_query(query_str).unwrap();
    let analyzed_unencoded = execute_analyze(context, query, query_str).unwrap();
    let analyzed = with_read_tx!(context, |tx| {
        encode_analyzed_query(&(*tx.snapshot), &tx.type_manager, analyzed_unencoded).unwrap()
    });
    context.analyzed = Some(analyzed);
}

#[cucumber::then(expr = r"typeql analyze{typeql_may_error}")]
async fn typeql_analyze_may_error(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query_str);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let query = parse_result.unwrap();
    let result = execute_analyze(context, query, query_str);
    may_error.check_logic(result);
}

#[cucumber::then("analyzed query given structure is:")]
async fn analyzed_query_given_is(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed.as_ref().unwrap();
    let actual_functor = encode_pipeline_given_as_functor(&analyzed.query, &analyzed.given.as_ref().unwrap());

    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(&expected_functor));
}

#[cucumber::then("analyzed query pipeline structure is:")]
async fn analyzed_query_pipeline_is(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed.as_ref().unwrap();
    let actual_functor = encode_pipeline_structure_as_functor(&analyzed.query);

    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(&expected_functor));
}

#[cucumber::then("analyzed query preamble contains:")]
async fn analyzed_query_preamble_contains(context: &mut Context, step: &Step) {
    let expected_functor = normalize_functor_for_compare(step.docstring().unwrap());
    let analyzed = context.analyzed.as_ref().unwrap();
    let preamble_functors = analyzed
        .preamble
        .iter()
        .map(encode_function_structure_as_functor)
        .map(|s| normalize_functor_for_compare(s.as_str()))
        .collect::<Vec<_>>();

    assert!(
        preamble_functors.contains(&expected_functor),
        "Looking for\n\t{}\nin any of:\n\t{}",
        expected_functor,
        preamble_functors.iter().join("\n\t")
    );
}

#[cucumber::then("analyzed query given annotations are:")]
async fn analyzed_query_given_annotations_is(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed.as_ref().unwrap();
    let actual_functor =
        encode_pipeline_given_annotations_as_functor(&analyzed.query, analyzed.given.as_ref().unwrap());

    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(expected_functor));
}

#[cucumber::then("analyzed query pipeline annotations are:")]
async fn analyzed_query_annotations_is(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed.as_ref().unwrap();
    let actual_functor = encode_pipeline_annotations_as_functor(&analyzed.query);

    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(expected_functor));
}

#[cucumber::then("analyzed preamble annotations contains:")]
async fn analyzed_preamble_annotations_contains(context: &mut Context, step: &Step) {
    let expected_functor = normalize_functor_for_compare(step.docstring().unwrap());
    let analyzed = context.analyzed.as_ref().unwrap();
    let preamble_functors = analyzed
        .preamble
        .iter()
        .map(encode_function_annotations_as_functor)
        .map(|s| normalize_functor_for_compare(s.as_str()))
        .collect::<Vec<_>>();

    assert!(
        preamble_functors.contains(&expected_functor),
        "Looking for\n\t{}\nin any of:\n\t{}",
        expected_functor,
        preamble_functors.iter().join("\n\t")
    );
}

#[cucumber::then("analyzed fetch annotations are:")]
async fn analyzed_fetch_annotations_are(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed.as_ref().unwrap();
    let actual_functor = encode_fetch_annotations_as_functor(analyzed);

    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(&expected_functor));
}

fn normalize_functor_for_compare(functor: &str) -> String {
    let mut normalized = functor.to_lowercase();
    normalized.retain(|c| !c.is_whitespace());
    normalized
}

fn parse_query_given_row_entry(entry: &String) -> GivenRowEntry {
    fn hex_to_u64(hex: &str) -> u64 {
        u64::from_str_radix(hex, 16).expect("Bad hex in iid: {hex}")
    }

    let mut parts = entry.split(":");
    match parts.next().unwrap() {
        "none" => GivenRowEntry::None,
        "value" => {
            let value_type_str = parts.next().expect("value:<value-type>:<value>");
            let value_str = parts.next().expect("value:<value-type>:<value>");
            let expected_value_type = parse_value_type(value_type_str);
            let value = params::Value::from_str(value_str).unwrap().into_typedb(expected_value_type);
            GivenRowEntry::Value(value)
        }
        "iid" => {
            let expected = "Expected iid:<kind>:<typeid-hex>:<instanceid-hex>";
            let kind = parts.next().unwrap();
            let type_id = TypeID::new(hex_to_u64(parts.next().expect(expected)) as u16);

            let thing = match kind {
                "entity" => {
                    let instance_id = hex_to_u64(parts.next().expect(expected));
                    Entity::new(ObjectVertex::build_entity(type_id, ObjectID::new(instance_id))).into()
                }
                "relation" => {
                    let instance_id = hex_to_u64(parts.next().expect(expected));
                    Relation::new(ObjectVertex::build_entity(type_id, ObjectID::new(instance_id))).into()
                }
                "attr" => {
                    debug_assert!(false, "Untested code. Remove this assert and try your luck");
                    let hex = parts.next().expect(expected);
                    let hex = hex.replace("_", "");
                    let bytes = (0..hex.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    Attribute::new(AttributeVertex::new(type_id, AttributeID::new(&bytes))).into()
                }
                other => panic!("Invalid kind: {other}"),
            };
            GivenRowEntry::Thing(thing)
        }
        other => panic!("Invalid entry type: {other}"),
    }
}
