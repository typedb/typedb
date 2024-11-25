/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::thing::IidInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, object::Object, ThingAPI},
};
use encoding::graph::thing::{vertex_attribute::AttributeVertex, vertex_object::ObjectVertex};
use ir::pattern::constraint::Iid;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    AsHkt, AsNarrowingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{Tuple, TuplePositions, TupleResult},
        Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct IidExecutor {
    iid: Iid<ExecutorVariable>,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    filter_fn: Arc<IidFilterFn>,
    checker: Checker<AsHkt![VariableValue<'_>]>,
}

pub(crate) type IidToTupleFn = for<'a> fn(Result<VariableValue<'a>, Box<ConceptReadError>>) -> TupleResult<'a>;

pub(super) type IidTupleIterator<I> = Map<
    TryFilter<I, Box<IidFilterFn>, AsHkt![VariableValue<'_>], Box<ConceptReadError>>,
    IidToTupleFn,
    AsHkt![TupleResult<'_>],
>;

pub(super) type IidFilterFn = FilterFn<AsHkt![VariableValue<'_>]>;

pub(crate) type IidIterator = IidTupleIterator<
    AsNarrowingIterator<
        <Option<Result<VariableValue<'static>, Box<ConceptReadError>>> as IntoIterator>::IntoIter,
        Result<AsHkt![VariableValue<'_>], Box<ConceptReadError>>,
    >,
>;

type IidVariableValueExtractor = for<'a, 'b> fn(&'a VariableValue<'b>) -> VariableValue<'a>;

pub(super) const EXTRACT_LHS: IidVariableValueExtractor = |lhs| lhs.as_reference();

fn iid_to_tuple(res: Result<VariableValue<'_>, Box<ConceptReadError>>) -> Result<Tuple<'_>, Box<ConceptReadError>> {
    match res {
        Ok(value) => Ok(Tuple::Single([value])),
        Err(err) => Err(err),
    }
}

impl IidExecutor {
    pub(crate) fn new(
        iid: IidInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        _sort_by: ExecutorVariable,
    ) -> Self {
        let IidInstruction { iid, types, checks } = iid;

        let var = iid.var().as_variable().unwrap();
        let output_tuple_positions = TuplePositions::Single([Some(var)]);
        let checker = Checker::<VariableValue<'_>>::new(checks, HashMap::from_iter([(var, EXTRACT_LHS)]));

        let filter_fn: Arc<IidFilterFn> = Arc::new(move |res| match res {
            Ok(value) => Ok(types.contains(&value.as_thing().type_())),
            Err(err) => Err(err.clone()),
        });

        Self { iid, variable_modes, tuple_positions: output_tuple_positions, filter_fn, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<IidFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let iid_parameter = self.iid.iid().as_parameter().unwrap();
        let bytes = context.parameters().iid(iid_parameter).unwrap();
        eprintln!("[{}:{}] bytes = {:x?}", file!(), line!(), bytes);

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        let instance = if let Some(object) = ObjectVertex::try_from_bytes(bytes) {
            let object = Object::new(object);
            thing_manager
                .instance_exists(snapshot, object.as_reference())
                .map(move |exists| exists.then_some(VariableValue::Thing(object.into_owned().into())))
        } else if let Some(attribute) = AttributeVertex::try_from_bytes(bytes) {
            let attribute = Attribute::new(attribute);
            thing_manager
                .instance_exists(snapshot, attribute.as_reference())
                .map(move |exists| exists.then_some(VariableValue::Thing(attribute.into_owned().into())))
        } else {
            Ok(None)
        };

        fn as_tuples<T>(it: T) -> Map<T, IidToTupleFn, AsHkt![TupleResult<'_>]>
        where
            T: for<'a> LendingIterator<Item<'a> = Result<VariableValue<'a>, Box<ConceptReadError>>>,
        {
            it.map::<TupleResult<'_>, IidToTupleFn>(iid_to_tuple)
        }

        let iterator = AsNarrowingIterator::new(instance.transpose());
        let as_tuples = as_tuples(iterator.try_filter::<_, IidFilterFn, VariableValue<'_>, _>(filter_for_row));
        Ok(TupleIterator::Iid(SortedTupleIterator::new(as_tuples, self.tuple_positions.clone(), &self.variable_modes)))
    }
}
