/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap},
    fmt, iter,
    sync::Arc,
    vec,
};

use answer::{Thing, Type, variable_value::VariableValue};
use compiler::{ExecutorVariable, executable::match_::instructions::thing::VectorSearchInstruction};
use concept::{
    error::ConceptReadError,
    thing::{ThingAPI, attribute::Attribute, thing_manager::ThingManager},
    type_::TypeAPI,
};
use encoding::{
    AsBytes, Keyable,
    graph::{
        Typed,
        thing::vertex_attribute::{AttributeID, AttributeVertex},
        type_::vertex::TypeVertexEncoding,
    },
    value::{ValueEncodable, value::Value, value_type::ValueType},
};
use ir::pattern::{ParameterID, constraint::VectorSearch};
use lending_iterator::AsLendingIterator;
use resource::profile::StorageCounters;
use storage::snapshot::{ReadableSnapshot, write::Write};

use crate::{
    instruction::{
        FilterMapUnchangedFn, VariableModes,
        checker::Checker,
        iterator::{NaiiveSeekable, SortedTupleIterator, TupleIterator},
        tuple::{Tuple, TuplePositions, TupleResult},
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return f64::NEG_INFINITY;
    }
    match <f32 as simsimd::SpatialSimilarity>::cosine(a, b) {
        Some(distance) => 1.0 - distance,
        None => f64::NEG_INFINITY,
    }
}

#[cfg(test)]
mod test {
    use super::cosine_similarity;

    #[test]
    fn cosine_similarity_kernel() {
        assert!((cosine_similarity(&[1.0, 0.0, 0.0], &[1.0, 0.0, 0.0]) - 1.0).abs() < 1e-5);
        assert!(cosine_similarity(&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]).abs() < 1e-5);
        let expected = 0.9 / (0.81f64 + 0.01).sqrt();
        assert!((cosine_similarity(&[1.0, 0.0, 0.0], &[0.9, 0.1, 0.0]) - expected).abs() < 1e-5);
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0, 0.0]), f64::NEG_INFINITY);
    }
}

pub(super) type VectorSearchItem = (VariableValue<'static>, f64);

pub(super) type VectorSearchFilterMapFn = FilterMapUnchangedFn<VectorSearchItem>;

pub(crate) type VectorSearchToTupleFn = fn(Result<VectorSearchItem, Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) type VectorSearchIterator = NaiiveSeekable<
    AsLendingIterator<
        iter::Map<
            iter::FilterMap<
                vec::IntoIter<Result<VectorSearchItem, Box<ConceptReadError>>>,
                Box<VectorSearchFilterMapFn>,
            >,
            VectorSearchToTupleFn,
        >,
    >,
>;

pub(super) const EXTRACT_SEARCHED_ATTRIBUTE: fn(&VectorSearchItem) -> VariableValue<'_> =
    |(value, _)| value.as_reference();
pub(super) const EXTRACT_SIMILARITY: fn(&VectorSearchItem) -> VariableValue<'_> =
    |(_, similarity)| VariableValue::Value(Value::Double(*similarity));

fn to_tuple(res: Result<VectorSearchItem, Box<ConceptReadError>>) -> TupleResult<'static> {
    match res {
        Ok((value, similarity)) => Ok(Tuple::Pair([value, VariableValue::Value(Value::Double(similarity))])),
        Err(err) => Err(err),
    }
}

pub(crate) struct VectorSearchExecutor {
    vector_search: VectorSearch<ExecutorVariable>,
    attribute_bound: bool,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    types: Arc<std::collections::BTreeSet<Type>>,
    checker: Checker<VectorSearchItem>,
}

impl fmt::Debug for VectorSearchExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VectorSearchExecutor")
    }
}

impl VectorSearchExecutor {
    pub(crate) fn new(
        instruction: VectorSearchInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        _sort_by: ExecutorVariable,
    ) -> Self {
        let VectorSearchInstruction { vector_search, types, checks, inputs } = instruction;
        let var = vector_search.attribute().as_variable().unwrap();
        let attribute_bound = inputs.iter().any(|&input| input == var);
        let similarity_var = vector_search.similarity().as_variable();
        let tuple_positions = TuplePositions::Pair([Some(var), similarity_var]);
        let checker = Checker::<VectorSearchItem>::new(
            checks,
            [(Some(var), EXTRACT_SEARCHED_ATTRIBUTE), (similarity_var, EXTRACT_SIMILARITY)]
                .into_iter()
                .filter_map(|(var, extractor)| Some((var?, extractor)))
                .collect(),
        );
        Self { vector_search, attribute_bound, variable_modes, tuple_positions, types, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let check = self.checker.filter_fn_for_row(context, &row, storage_counters.clone());
        let filter_for_row: Box<VectorSearchFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();
        let query_vertex = self.vector_search.query();
        let query =
            resolve_query_vector(context, &row, query_vertex.as_variable(), query_vertex.as_parameter().cloned())?;
        let threshold = resolve_threshold(context, self.vector_search.threshold());

        for type_ in self.types.iter() {
            let attribute_type = type_.as_attribute_type();
            if let Some(ValueType::Vector(parameters)) =
                attribute_type.get_value_type_without_source(snapshot, thing_manager.type_manager())?
            {
                if parameters.length as usize != query.len() {
                    return Err(Box::new(ConceptReadError::VectorSearchQueryDimensionMismatch {
                        attribute_type,
                        expected: parameters.length,
                        provided: query.len(),
                    }));
                }
            }
        }

        let mut matching: Vec<Result<VectorSearchItem, Box<ConceptReadError>>> = Vec::new();
        if self.attribute_bound {
            let ExecutorVariable::RowPosition(position) = self.vector_search.attribute().as_variable().unwrap() else {
                unreachable!("bound vector search attribute must have a row position")
            };
            if let VariableValue::Thing(Thing::Attribute(attribute)) = row.get(position) {
                if self.types.contains(&Type::Attribute(attribute.type_())) {
                    let value = attribute.get_value(snapshot, thing_manager, storage_counters.clone())?;
                    if let Value::Vector(vector) = &value {
                        let similarity = cosine_similarity(&query, vector.as_ref());
                        if similarity >= threshold {
                            matching.push(Ok((VariableValue::Thing(Thing::Attribute(attribute.clone())), similarity)));
                        }
                    }
                }
            }
            let as_tuples = matching.into_iter().filter_map(filter_for_row).map(to_tuple as VectorSearchToTupleFn);
            let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
            return Ok(TupleIterator::VectorSearch(SortedTupleIterator::new(
                lending_tuples,
                self.tuple_positions.clone(),
                &self.variable_modes,
            )));
        }
        for type_ in self.types.iter() {
            let attribute_type = type_.as_attribute_type();
            let type_id = attribute_type.vertex().type_id_();
            let vector_store = thing_manager.vector_store();

            // candidate set deduplicates index hits against this transaction's buffered writes
            // (a put of an already-committed vector appears in both)
            // BTreeSet: downstream tuple iterators require attribute-sorted yield order
            let mut candidates: BTreeSet<AttributeVertex> = BTreeSet::new();

            // 1) committed vectors: ANN search on the vector store index.
            // ponytail: threshold-only semantics on a top-k index via k-widening; replace with a
            // planner-provided k when TypeQL exposes one
            let total = vector_store.indexed_vector_count(type_id);
            if total > 0 {
                let mut k = 128.min(total);
                loop {
                    let results = vector_store.search(type_id, &query, k);
                    let tail_above_threshold = results.iter().all(|&(_, distance)| 1.0 - distance as f64 >= threshold);
                    let exhausted = results.len() >= total || k >= total;
                    if !tail_above_threshold || exhausted {
                        candidates.extend(
                            results.into_iter().map(|(id, _)| AttributeVertex::new(type_id, AttributeID::Vector(id))),
                        );
                        break;
                    }
                    k = (k * 4).min(total);
                }
            }

            // 2) this transaction's buffered vector writes: not yet in the index
            for (key, write) in snapshot.iterate_writes() {
                if matches!(write, Write::Delete)
                    || !AttributeVertex::is_vector_attribute_vertex(key.keyspace_id(), key.bytes())
                {
                    continue;
                }
                let vertex = AttributeVertex::decode(key.bytes());
                if vertex.type_id_() == type_id {
                    candidates.insert(vertex);
                }
            }

            for vertex in candidates {
                // MVCC re-check: the vector store is versionless; whether the attribute exists
                // for this snapshot is decided by the KV key (delete tombstones, visibility)
                let exists = snapshot
                    .get_mapped(vertex.into_storage_key().as_reference(), |_| (), storage_counters.clone())
                    .map_err(|error| Box::new(ConceptReadError::SnapshotGet { source: error }))?
                    .is_some();
                if !exists {
                    continue;
                }
                let attribute = Attribute::new(vertex);
                let value = attribute.get_value(snapshot, thing_manager, storage_counters.clone())?;
                if let Value::Vector(vector) = &value {
                    let similarity = cosine_similarity(&query, vector.as_ref());
                    if similarity >= threshold {
                        matching.push(Ok((VariableValue::Thing(Thing::Attribute(attribute)), similarity)));
                    }
                }
            }
        }

        let as_tuples = matching.into_iter().filter_map(filter_for_row).map(to_tuple as VectorSearchToTupleFn);
        let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
        Ok(TupleIterator::VectorSearch(SortedTupleIterator::new(
            lending_tuples,
            self.tuple_positions.clone(),
            &self.variable_modes,
        )))
    }
}

pub(crate) fn resolve_query_vector(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &MaybeOwnedRow<'_>,
    variable: Option<ExecutorVariable>,
    parameter: Option<ParameterID>,
) -> Result<Vec<f32>, Box<ConceptReadError>> {
    if let Some(parameter) = parameter {
        match context.parameters().value_unchecked(&parameter) {
            Value::Vector(vector) => Ok(vector.as_ref().clone()),
            other => unreachable!("vector search query parameter is not a vector: {other}"),
        }
    } else {
        let ExecutorVariable::RowPosition(position) =
            variable.expect("vector search query must be a parameter or a variable")
        else {
            unreachable!("vector search query variable must have a row position")
        };
        match row.get(position) {
            VariableValue::Value(Value::Vector(vector)) => Ok(vector.as_ref().clone()),
            other => {
                let actual_type = match other {
                    VariableValue::Value(value) => value.value_type().to_string(),
                    other => format!("{other:?}"),
                };
                Err(Box::new(ConceptReadError::VectorSearchQueryNotAVector { actual_type }))
            }
        }
    }
}

pub(crate) fn resolve_threshold(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    threshold: ParameterID,
) -> f64 {
    match context.parameters().value_unchecked(&threshold) {
        Value::Double(double) => *double,
        other => unreachable!("vector search threshold parameter is not a double: {other}"),
    }
}

impl fmt::Display for VectorSearchExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", &self.vector_search)
    }
}
