/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::{variable_value::VariableValue, Thing};
use compiler::{
    modifiers::{ReduceOperation, ReduceProgram},
    VariablePosition,
};
use encoding::value::value::Value;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{
        stage::{ExecutionContext, StageAPI, StageIterator},
        PipelineExecutionError, WrittenRowsIterator,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct ReduceStageExecutor<PreviousStage> {
    program: ReduceProgram,
    previous: PreviousStage,
}

impl<PreviousStage> ReduceStageExecutor<PreviousStage> {
    pub fn new(program: ReduceProgram, previous: PreviousStage) -> Self {
        Self { program, previous }
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for ReduceStageExecutor<PreviousStage>
where
    Snapshot: ReadableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(
        self,
        interrupt: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        let Self { previous, program, .. } = self;
        let (previous_iterator, context) = previous.into_iterator(interrupt)?;
        let rows = match reduce_iterator(&context, program, previous_iterator) {
            Ok(rows) => rows,
            Err(err) => return Err((err, context)),
        };
        Ok((WrittenRowsIterator::new(rows), context))
    }
}

fn reduce_iterator<Snapshot: ReadableSnapshot>(
    context: &ExecutionContext<Snapshot>,
    program: ReduceProgram,
    iterator: impl StageIterator,
) -> Result<Batch, PipelineExecutionError> {
    let mut iterator = iterator;
    let mut grouped_reducer = GroupedReducer::new(program);
    while let Some(result) = iterator.next() {
        grouped_reducer.accept(&result?, &context)?;
    }
    Ok(grouped_reducer.finalise())
}

struct GroupedReducer {
    input_group_positions: Vec<VariablePosition>,
    grouped_aggregates: HashMap<Vec<VariableValue<'static>>, Vec<ReducerImpl>>,
    reused_group: Vec<VariableValue<'static>>,
    sample_reducers: Vec<ReducerImpl>,
}

impl GroupedReducer {
    fn new(program: ReduceProgram) -> Self {
        let sample_reducers: Vec<ReducerImpl> =
            program.reduction_inputs.iter().map(|reducer| ReducerImpl::build(reducer)).collect();
        let reused_group = Vec::with_capacity(program.input_group_positions.len());
        let mut grouped_aggregates = HashMap::new();
        // Empty result sets behave different for an empty grouping
        if program.input_group_positions.len() == 0 {
            grouped_aggregates.insert(Vec::new(), sample_reducers.clone());
        }
        Self {
            input_group_positions: program.input_group_positions,
            grouped_aggregates,
            reused_group,
            sample_reducers,
        }
    }

    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
    ) -> Result<(), PipelineExecutionError> {
        self.reused_group.clear();
        for pos in &self.input_group_positions {
            self.reused_group.push(row.get(pos.clone()).to_owned());
        }
        if !self.grouped_aggregates.contains_key(&self.reused_group) {
            self.grouped_aggregates.insert(self.reused_group.clone(), self.sample_reducers.clone());
        }
        let reducers = self.grouped_aggregates.get_mut(&self.reused_group).unwrap();
        for reducer in reducers {
            reducer.accept(row, context);
        }
        Ok(())
    }

    fn finalise(self) -> Batch {
        let Self { input_group_positions, sample_reducers, grouped_aggregates, .. } = self;
        let mut batch =
            Batch::new((input_group_positions.len() + sample_reducers.len()) as u32, grouped_aggregates.len());
        let mut reused_row = Vec::with_capacity(input_group_positions.len() + sample_reducers.len());
        let reused_multiplicity = 1;
        for (group, reducers) in grouped_aggregates.into_iter() {
            reused_row.clear();
            for value in group.into_iter() {
                reused_row.push(value);
            }
            for reducer in reducers.into_iter() {
                reused_row.push(reducer.finalise().unwrap_or(VariableValue::Empty));
            }
            batch.append(MaybeOwnedRow::new_borrowed(reused_row.as_slice(), &reused_multiplicity))
        }
        batch
    }
}

trait ReducerAPI {
    fn new(target: VariablePosition) -> Self;

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>);

    fn finalise(self) -> Option<VariableValue<'static>>;
}

fn extract_value<Snapshot: ReadableSnapshot>(
    row: &MaybeOwnedRow<'_>,
    position: &VariablePosition,
    context: &ExecutionContext<Snapshot>,
) -> Option<Value<'static>> {
    match row.get(position.clone()) {
        VariableValue::Empty => None,
        VariableValue::Value(value) => Some(value.clone().into_owned()),
        VariableValue::Thing(Thing::Attribute(attribute)) => {
            // As long as these are trivial, it's safe to unwrap
            let snapshot: &Snapshot = &context.snapshot;
            let value = attribute.get_value(snapshot, &context.thing_manager).unwrap();
            Some(value.clone().into_owned())
        }
        _ => unreachable!(),
    }
}

#[derive(Debug, Clone)]
enum ReducerImpl {
    SumLong(SumLongImpl),
    Count(CountImpl),
    SumDouble(SumDoubleImpl),
    MaxLong(MaxLongImpl),
    MaxDouble(MaxDoubleImpl),
    MinLong(MinLongImpl),
    MinDouble(MinDoubleImpl),
}

impl ReducerImpl {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        match self {
            ReducerImpl::SumLong(reducer) => reducer.accept(row, context),
            ReducerImpl::SumDouble(reducer) => reducer.accept(row, context),
            ReducerImpl::Count(reducer) => reducer.accept(row, context),
            ReducerImpl::MaxLong(reducer) => reducer.accept(row, context),
            ReducerImpl::MaxDouble(reducer) => reducer.accept(row, context),
            ReducerImpl::MinLong(reducer) => reducer.accept(row, context),
            ReducerImpl::MinDouble(reducer) => reducer.accept(row, context),
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        match self {
            ReducerImpl::SumLong(mut reducer) => reducer.finalise(),
            ReducerImpl::SumDouble(mut reducer) => reducer.finalise(),
            ReducerImpl::Count(mut reducer) => reducer.finalise(),
            ReducerImpl::MaxLong(mut reducer) => reducer.finalise(),
            ReducerImpl::MaxDouble(mut reducer) => reducer.finalise(),
            ReducerImpl::MinLong(mut reducer) => reducer.finalise(),
            ReducerImpl::MinDouble(mut reducer) => reducer.finalise(),
        }
    }
}

impl ReducerImpl {
    fn build(reduce_ir: &ReduceOperation<VariablePosition>) -> Self {
        match reduce_ir {
            ReduceOperation::Count(pos) => ReducerImpl::Count(CountImpl::new(pos.clone())),
            ReduceOperation::SumLong(pos) => ReducerImpl::SumLong(SumLongImpl::new(pos.clone())),
            ReduceOperation::SumDouble(pos) => ReducerImpl::SumDouble(SumDoubleImpl::new(pos.clone())),
            ReduceOperation::MaxLong(pos) => ReducerImpl::MaxLong(MaxLongImpl::new(pos.clone())),
            ReduceOperation::MaxDouble(pos) => ReducerImpl::MaxDouble(MaxDoubleImpl::new(pos.clone())),
            ReduceOperation::MinLong(pos) => ReducerImpl::MinLong(MinLongImpl::new(pos.clone())),
            ReduceOperation::MinDouble(pos) => ReducerImpl::MinDouble(MinDoubleImpl::new(pos.clone())),
            // ReduceOperation::MeanLong(_) => {}
            // ReduceOperation::MeanDouble(_) => {}
            // ReduceOperation::MedianLong(_) => {}
            // ReduceOperation::MedianDouble(_) => {}
            // ReduceOperation::StdLong(_) => {}
            // ReduceOperation::StdDouble(_) => {}
            _ => todo!()
        }
    }
}

#[derive(Debug, Clone)]
struct CountImpl {
    count: u64,
    target: VariablePosition,
}

impl ReducerAPI for CountImpl {
    fn new(target: VariablePosition) -> Self {
        Self { count: 0, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if &VariableValue::Empty != row.get(self.target.clone()) {
            self.count += row.get_multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Long(self.count as i64)))
    }
}

#[derive(Debug, Clone)]
struct SumLongImpl {
    sum: i64,
    target: VariablePosition,
}

impl ReducerAPI for SumLongImpl {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, &self.target, context) {
            self.sum += value.unwrap_long() * row.get_multiplicity() as i64;
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Long(self.sum)))
    }
}

#[derive(Debug, Clone)]
struct SumDoubleImpl {
    sum: f64,
    target: VariablePosition,
}

impl ReducerAPI for SumDoubleImpl {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0.0, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, &self.target, context) {
            self.sum += value.unwrap_double() * row.get_multiplicity() as f64;
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Double(self.sum)))
    }
}

#[derive(Debug, Clone)]
struct MaxLongImpl {
    max: Option<i64>,
    target: VariablePosition,
}

impl ReducerAPI for MaxLongImpl {
    fn new(target: VariablePosition) -> Self {
        Self { max: None, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, &self.target, context).map(|v| v.unwrap_long()) {
            if let Some(current) = self.max.as_ref() {
                if value > *current {
                    self.max = Some(value)
                }
            } else {
                self.max = Some(value);
            }
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        self.max.map(|v| VariableValue::Value(Value::Long(v)))
    }
}


#[derive(Debug, Clone)]
struct MaxDoubleImpl {
    max: Option<f64>,
    target: VariablePosition,
}

impl ReducerAPI for MaxDoubleImpl {
    fn new(target: VariablePosition) -> Self {
        Self { max: None, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, &self.target, context).map(|v| v.unwrap_double()) {
            if let Some(current) = self.max.as_ref() {
                if value > *current {
                    self.max = Some(value)
                }
            } else {
                self.max = Some(value);
            }
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        self.max.map(|v| VariableValue::Value(Value::Double(v)))
    }
}

#[derive(Debug, Clone)]
struct MinLongImpl {
    min: Option<i64>,
    target: VariablePosition,
}

impl ReducerAPI for crate::pipeline::reduce::MinLongImpl {
    fn new(target: VariablePosition) -> Self {
        Self { min: None, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, &self.target, context).map(|v| v.unwrap_long()) {
            if let Some(current) = self.min.as_ref() {
                if value < *current {
                    self.min = Some(value)
                }
            } else {
                self.min = Some(value);
            }
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        self.min.map(|v| VariableValue::Value(Value::Long(v)))
    }
}


#[derive(Debug, Clone)]
struct MinDoubleImpl {
    min: Option<f64>,
    target: VariablePosition,
}

impl ReducerAPI for MinDoubleImpl {
    fn new(target: VariablePosition) -> Self {
        Self { min: None, target }
    }

    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, &self.target, context).map(|v| v.unwrap_double()) {
            if let Some(current) = self.min.as_ref() {
                if value < *current {
                    self.min = Some(value)
                }
            } else {
                self.min = Some(value);
            }
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        self.min.map(|v| VariableValue::Value(Value::Double(v)))
    }
}
