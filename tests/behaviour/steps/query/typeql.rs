/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter, str::FromStr};

use answer::{variable_value::VariableValue, Thing};
use compiler::{query_structure::QueryStructure, VariablePosition};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, object::ObjectAPI, ThingAPI},
    type_::TypeAPI,
};
use cucumber::gherkin::Step;
use encoding::{
    value::{label::Label, value_type::ValueType, ValueEncodable},
    AsBytes,
};
use executor::{
    batch::Batch,
    pipeline::stage::{ExecutionContext, StageIterator},
    ExecutionInterrupt,
};
use itertools::{Either, Itertools};
use lending_iterator::LendingIterator;
use macro_rules_attribute::apply;
use params;
use query::{analyse::AnalysedQuery, error::QueryError};
use resource::profile::StorageCounters;
use server::service::http::message::query::{
    annotations::bdd::{encode_fetch_annotations_as_functor, encode_query_annotations_as_functor},
    encode_analyzed_query,
    query_structure::bdd::encode_query_structure_as_functor,
};
use storage::snapshot::SnapshotDropGuard;
use test_utils::assert_matches;

use crate::{
    generic_step,
    json::JSON,
    query_answer_context::{with_rows_answer, QueryAnswer},
    transaction_context::{
        with_read_tx, with_schema_tx, with_write_tx_deconstructed,
        ActiveTransaction::{Read, Schema},
    },
    util::{iter_table_map, list_contains_json, parse_json},
    BehaviourTestExecutionError, Context,
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
    source_query: &str,
) -> Result<QueryAnswer, Box<QueryError>> {
    with_read_tx!(context, |tx| {
        let pipeline = tx.query_manager.prepare_read_pipeline(
            tx.snapshot.clone_inner(),
            &tx.type_manager,
            tx.thing_manager.clone(),
            &tx.function_manager,
            &query.into_structure().into_pipeline(),
            source_query,
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
    source_query: &str,
) -> Result<QueryAnswer, BehaviourTestExecutionError> {
    if matches!(context.active_transaction.as_ref().unwrap(), Read(_)) {
        return Err(BehaviourTestExecutionError::UseInvalidTransactionAsWrite);
    }

    with_write_tx_deconstructed!(context, |snapshot,
                                           type_manager,
                                           thing_manager,
                                           function_manager,
                                           query_manager,
                                           _db,
                                           _opts| {
        let pipeline_result = query_manager.prepare_write_pipeline(
            snapshot.into_inner(),
            &type_manager,
            thing_manager.clone(),
            &function_manager,
            &query.into_structure().into_pipeline(),
            source_query,
        );

        match pipeline_result {
            Err((snapshot, error)) => {
                (Err(BehaviourTestExecutionError::Query(*error)), SnapshotDropGuard::new(snapshot))
            }
            Ok(pipeline) => {
                if pipeline.has_fetch() {
                    match pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible()) {
                        Ok((iterator, ExecutionContext { parameters, snapshot, .. })) => (
                            match iterator.collect() {
                                Ok(documents) => Ok(QueryAnswer::ConceptDocuments(documents, parameters)),
                                Err(err) => {
                                    Err(BehaviourTestExecutionError::Query(QueryError::WritePipelineExecution {
                                        source_query: source_query.to_string(),
                                        typedb_source: err,
                                    }))
                                }
                            },
                            SnapshotDropGuard::from_arc(snapshot),
                        ),
                        Err((err, ExecutionContext { snapshot, .. })) => (
                            Err(BehaviourTestExecutionError::Query(QueryError::WritePipelineExecution {
                                source_query: source_query.to_string(),
                                typedb_source: err,
                            })),
                            SnapshotDropGuard::from_arc(snapshot),
                        ),
                    }
                } else {
                    let named_outputs = pipeline.rows_positions().expect("Expected unfetched result").clone();
                    match pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()) {
                        Ok((iterator, ExecutionContext { snapshot, .. })) => {
                            let result_as_batch = iterator.collect_owned();
                            match result_as_batch {
                                Ok(batch) => (
                                    Ok(QueryAnswer::ConceptRows(row_batch_result_to_answer(batch, named_outputs))),
                                    SnapshotDropGuard::from_arc(snapshot),
                                ),
                                Err(typedb_source) => (
                                    Err(BehaviourTestExecutionError::Query(QueryError::WritePipelineExecution {
                                        source_query: source_query.to_string(),
                                        typedb_source,
                                    })),
                                    SnapshotDropGuard::from_arc(snapshot),
                                ),
                            }
                        }
                        Err((err, ExecutionContext { snapshot, .. })) => (
                            Err(BehaviourTestExecutionError::Query(QueryError::ReadPipelineExecution {
                                source_query: source_query.to_string(),
                                typedb_source: err,
                            })),
                            SnapshotDropGuard::from_arc(snapshot),
                        ),
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
        tx.query_manager
            .analyse_query(
                tx.snapshot.clone_inner(),
                &tx.type_manager,
                tx.thing_manager.clone(),
                &tx.function_manager,
                &query.into_structure().into_pipeline(),
                source_query,
            )
            .map_err(|source| BehaviourTestExecutionError::Query(*source))
    })
}

#[apply(generic_step)]
#[step(expr = r"typeql schema query{typeql_may_error}")]
async fn typeql_schema_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let typeql_schema = parse_result.unwrap().into_structure().into_schema();

    if !matches!(context.active_transaction.as_ref().expect("Expected an active tx"), Schema(_)) {
        may_error.check_logic::<(), BehaviourTestExecutionError>(Err(
            BehaviourTestExecutionError::UseInvalidTransactionAsSchema,
        ));
        return;
    }

    with_schema_tx!(context, |tx| {
        let result = tx.query_manager.execute_schema(
            tx.snapshot.as_mut().unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            &tx.function_manager,
            typeql_schema,
            query,
        );
        if let Either::Right(_err) = may_error.check_logic(result) {
            context.close_active_transaction();
        }
    });
}

#[apply(generic_step)]
#[step(expr = r"typeql write query{typeql_may_error}")]
async fn typeql_write_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query_str);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let query = parse_result.unwrap();

    let result = execute_write_query(context, query, query_str);
    if let Either::Right(_) = may_error.check_logic(result) {
        context.close_active_transaction();
    }
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql write query")]
async fn get_answers_of_typeql_write_query(context: &mut Context, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let query = typeql::parse_query(query_str).unwrap();
    let result = execute_write_query(context, query, query_str);
    context.query_answer = Some(result.unwrap());
}

#[apply(generic_step)]
#[step(expr = r"typeql read query{typeql_may_error}")]
async fn typeql_read_query(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query_str);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let query = parse_result.unwrap();
    let result = execute_read_query(context, query, query_str);
    may_error.check_logic(result); // we don't close read transactions with logical errors
}

fn record_answers_of_typeql_read_query(context: &mut Context, query_str: &str) {
    let query = typeql::parse_query(query_str).unwrap();
    context.query_answer = match execute_read_query(context, query, query_str) {
        Ok(answers) => Some(answers),
        Err(error) => panic!("Unexpected get answers error: {:?}", error),
    }
}

#[apply(generic_step)]
#[step(expr = r"get answers of typeql read query")]
async fn get_answers_of_typeql_read_query(context: &mut Context, step: &Step) {
    record_answers_of_typeql_read_query(context, step.docstring.as_ref().unwrap().as_str());
}

#[apply(generic_step)]
#[step(expr = r"get answers of templated typeql read query")]
async fn get_answers_of_templated_typeql_read_query(context: &mut Context, step: &Step) {
    let rows = context.query_answer.as_ref().unwrap().as_rows();
    let [answer] = rows else { panic!("Expected single answer, found {}", rows.len()) };
    let templated_query = step.docstring.as_ref().unwrap().as_str();
    record_answers_of_typeql_read_query(context, &apply_query_template(templated_query, answer));
}

#[apply(generic_step)]
#[step(expr = r"uniquely identify answer concepts")]
async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
    let num_specs = step.table().unwrap().rows.len() - 1;
    with_rows_answer!(context, |query_answer| {
        let num_answers = query_answer.len();
        if num_specs != num_answers {
            panic!("expected the number of identifier entries to match the number of answers, found {} expected entries and {} real answers. Real answers: \n{}",
            num_specs, num_answers, row_answers_to_string(query_answer))
        }
        for row in iter_table_map(step) {
            let mut num_matches = 0;
            for answer_row in query_answer {
                let table_row_within_answer =
                    row.iter().all(|(&var, &spec)| does_var_in_row_match_spec(context, answer_row, var, spec));
                if table_row_within_answer && row.len() == answer_row.len() {
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

#[apply(generic_step)]
#[step(expr = r"result is a single row with variable '{word}': {word}")]
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

fn does_value_match(id: &str, var_value: &VariableValue<'_>, _context: &Context) -> bool {
    let VariableValue::Value(value) = var_value else {
        return false;
    };
    let (id_type, id_value) = id.split_once(":").unwrap();
    let expected_value_type = match id_type {
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
    };
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
#[step(expr = r"answer size is: {int}")]
async fn answer_size_is(context: &mut Context, answer_size: i32) {
    assert_eq!(context.query_answer.as_ref().unwrap().len(), answer_size as usize)
}

#[apply(generic_step)]
#[step(expr = r"order of answer concepts is")]
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

#[apply(generic_step)]
#[step(expr = r"answer {contains_or_doesnt} document:")]
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

#[apply(generic_step)]
#[step(expr = r"answers do not contain variable: {word}")]
async fn answers_do_not_contain_variable(context: &mut Context, variable: String, step: &Step) {
    context.query_answer.as_ref().unwrap().as_rows().iter().all(|row| !row.contains_key(&variable));
}

#[apply(generic_step)]
#[step(expr = r"each answer satisfies")]
async fn each_answer_satisfies(context: &mut Context, step: &Step) {
    let templated_query = step.docstring().unwrap();
    for answer in context.query_answer.as_ref().unwrap().as_rows() {
        let query_string = apply_query_template(templated_query, answer);
        let query = typeql::parse_query(&query_string).unwrap();
        let answer_size = match execute_read_query(context, query, &query_string) {
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
        let iid = iid_of(thing);

        buf.push_str("0x");
        for byte in iid {
            buf.push_str(&format!("{byte:02X}"));
        }

        template = tail;
    }
    buf.push_str(template);
    buf
}

fn iid_of(thing: &Thing) -> Vec<u8> {
    match thing {
        Thing::Entity(entity) => entity.vertex().to_bytes().into(),
        Thing::Relation(relation) => relation.vertex().to_bytes().into(),
        Thing::Attribute(attribute) => attribute.vertex().to_bytes().into(),
    }
}

#[apply(generic_step)]
#[step(expr = r"verify answer set is equivalent for query")]
async fn verify_answer_set(context: &mut Context, step: &Step) {
    if true {
        eprintln!("TODO: Implement step: verify answer set is equivalent for query");
        return;
    }
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let query = typeql::parse_query(query_str).unwrap();
    let verify_answers = execute_read_query(context, query, query_str).unwrap();
    match (&context.query_answer.as_ref().unwrap(), verify_answers) {
        (QueryAnswer::ConceptRows(actual), QueryAnswer::ConceptRows(expected)) => {
            assert_eq!(
                expected.len(), actual.len(),
                "expected the number of identifier entries to match the number of answers, found {} entries and {} answers",
                expected.len(), actual.len()
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

#[apply(generic_step)]
#[step(expr = r"get answers of typeql analyze query")]
async fn get_answers_of_typeql_analyze_query(context: &mut Context, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let query = typeql::parse_query(query_str).unwrap();
    let analyzed_unencoded = execute_analyze(context, query, query_str).unwrap();
    let analyzed = with_read_tx!(context, |tx| {
        encode_query_structure_annotations(&(*tx.snapshot), &tx.type_manager, analyzed_unencoded).unwrap()
    });
    context.analyzed_query = Some(analyzed);
}

#[apply(generic_step)]
#[step(expr = r"typeql analyze query{typeql_may_error}")]
async fn typeql_analyze_query_may_error(context: &mut Context, may_error: params::TypeQLMayError, step: &Step) {
    let query_str = step.docstring.as_ref().unwrap().as_str();
    let parse_result = typeql::parse_query(query_str);
    if let Either::Right(_) = may_error.check_parsing(parse_result.as_ref()) {
        return;
    }
    let query = parse_result.unwrap();
    let result = execute_analyze(context, query, query_str);
    may_error.check_logic(result);
}

#[apply(generic_step)]
#[step(expr = r"analyzed query pipeline structure is:")]
async fn analyzed_query_pipeline_is(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed_query.as_ref().unwrap();
    let (actual_functor, _preamble) = encode_query_structure_as_functor(&analyzed);
    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(expected_functor));
}

#[apply(generic_step)]
#[step(expr = r"analyzed query preamble contains:")]
async fn analyzed_query_preamble_contains(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed_query.as_ref().unwrap();
    let (_pipeline, preamble_functors) = encode_query_structure_as_functor(&analyzed);

    assert!(
        preamble_functors.iter().any(|actual_functor| {
            normalize_functor_for_compare(actual_functor) == normalize_functor_for_compare(expected_functor)
        }),
        "Looking for\n\t{}\nin any of:\n\t{}",
        normalize_functor_for_compare(expected_functor),
        preamble_functors.iter().map(|s| normalize_functor_for_compare(s)).join("\n\t")
    );
}

#[apply(generic_step)]
#[step(expr = r"analyzed query pipeline annotations are:")]
async fn analyzed_query_annotations_is(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed_query.as_ref().unwrap();
    let (actual_functor, _preamble) = encode_query_annotations_as_functor(&analyzed);
    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(expected_functor));
}

#[apply(generic_step)]
#[step(expr = r"analyzed preamble annotations contains:")]
async fn analyzed_preamble_annotations_contains(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed_query.as_ref().unwrap();
    let (_pipeline, preamble_functors) = encode_query_annotations_as_functor(&analyzed);

    assert!(
        preamble_functors.iter().any(|actual_functor| {
            normalize_functor_for_compare(actual_functor) == normalize_functor_for_compare(expected_functor)
        }),
        "Looking for\n\t{}\nin any of:\n\t{}",
        normalize_functor_for_compare(expected_functor),
        preamble_functors.iter().map(|s| normalize_functor_for_compare(s)).join("\n\t")
    );
}

#[apply(generic_step)]
#[step(expr = r"analyzed fetch annotations are:")]
async fn analyzed_fetch_annotations_are(context: &mut Context, step: &Step) {
    let expected_functor = step.docstring().unwrap();
    let analyzed = context.analyzed_query.as_ref().unwrap();
    let actual_functor = encode_fetch_annotations_as_functor(&analyzed);

    assert_eq!(normalize_functor_for_compare(&actual_functor), normalize_functor_for_compare(expected_functor));
}

fn normalize_functor_for_compare(functor: &String) -> String {
    let mut normalized = functor.to_lowercase();
    normalized.retain(|c| !c.is_whitespace());
    normalized
}
