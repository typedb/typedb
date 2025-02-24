/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, iter, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{executable::match_::instructions::thing::IidInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, object::Object, ThingAPI},
};
use encoding::graph::thing::{vertex_attribute::AttributeVertex, vertex_object::ObjectVertex};
use ir::pattern::constraint::Iid;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{Tuple, TuplePositions, TupleResult},
        Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct IidExecutor {
    iid: Iid<ExecutorVariable>,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    filter_fn: Arc<IidFilterFn>,
    checker: Checker<VariableValue<'static>>,
}

impl fmt::Debug for IidExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IidExecutor")
    }
}

pub(crate) type IidToTupleFn = fn(Result<VariableValue<'static>, Box<ConceptReadError>>) -> TupleResult<'static>;

pub(super) type IidTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<IidFilterMapFn>>, IidToTupleFn>;

pub(super) type IidFilterFn = FilterFn<VariableValue<'static>>;
pub(super) type IidFilterMapFn = FilterMapUnchangedFn<VariableValue<'static>>;

pub(crate) type IidIterator =
    IidTupleIterator<<Option<Result<VariableValue<'static>, Box<ConceptReadError>>> as IntoIterator>::IntoIter>;

type IidVariableValueExtractor = for<'a, 'b> fn(&'a VariableValue<'b>) -> VariableValue<'a>;

pub(super) const EXTRACT_IDENTITY: IidVariableValueExtractor = |lhs| lhs.as_reference();

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
        let checker = Checker::<VariableValue<'_>>::new(checks, HashMap::from_iter([(var, EXTRACT_IDENTITY)]));

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
        let filter_for_row: Box<IidFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        let iid_parameter = self.iid.iid().as_parameter().unwrap();
        let bytes = context.parameters().iid(iid_parameter).unwrap();

        let instance = if let Some(object) = ObjectVertex::try_from_bytes(bytes) {
            let object = Object::new(object);
            thing_manager
                .instance_exists(snapshot, &object)
                .map(move |exists| exists.then_some(VariableValue::Thing(object.into())))
        } else if let Some(attribute) = AttributeVertex::try_from_bytes(bytes) {
            let attribute = Attribute::new(attribute);
            thing_manager
                .instance_exists(snapshot, &attribute)
                .map(move |exists| exists.then_some(VariableValue::Thing(attribute.clone().into())))
        } else {
            Ok(None)
        };

        let iterator = instance.transpose();
        let as_tuples =
            iterator.into_iter().filter_map(filter_for_row).map::<TupleResult<'_>, IidToTupleFn>(iid_to_tuple);
        Ok(TupleIterator::Iid(SortedTupleIterator::new(as_tuples, self.tuple_positions.clone(), &self.variable_modes)))
    }
}

impl fmt::Display for IidExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", &self.iid)
    }
}
