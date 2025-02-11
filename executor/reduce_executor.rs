/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::{variable_value::VariableValue, Thing};
use compiler::{
    executable::reduce::{ReduceInstruction, ReduceRowsExecutable},
    VariablePosition,
};
use encoding::value::value::Value;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{stage::ExecutionContext, PipelineExecutionError},
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct GroupedReducer {
    rows_executable: Arc<ReduceRowsExecutable>, // for accessing input group positions
    grouped_reductions: HashMap<Vec<VariableValue<'static>>, Vec<ReducerExecutor>>,
    reused_group: Vec<VariableValue<'static>>,
    // Clone for efficient instantiation of reducers for a new group
    uninitialised_reducer_executors: Vec<ReducerExecutor>,
}

impl GroupedReducer {
    pub(crate) fn new(executable: Arc<ReduceRowsExecutable>) -> Self {
        let reducers: Vec<ReducerExecutor> = executable.reductions.iter().map(ReducerExecutor::build).collect();
        let reused_group = Vec::with_capacity(executable.input_group_positions.len());
        let mut grouped_reductions = HashMap::new();
        // Empty result sets behave different for an empty grouping
        if executable.input_group_positions.is_empty() {
            grouped_reductions.insert(Vec::new(), reducers.clone());
        }
        Self {
            rows_executable: executable,
            grouped_reductions,
            reused_group,
            uninitialised_reducer_executors: reducers,
        }
    }

    pub(crate) fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
    ) -> Result<(), Box<PipelineExecutionError>> {
        self.reused_group.clear();
        for &pos in &self.rows_executable.input_group_positions {
            self.reused_group.push(row.get(pos).to_owned());
        }
        if !self.grouped_reductions.contains_key(&self.reused_group) {
            self.grouped_reductions.insert(self.reused_group.clone(), self.uninitialised_reducer_executors.clone());
        }
        let reducers = self.grouped_reductions.get_mut(&self.reused_group).unwrap();
        for reducer in reducers {
            reducer.accept(row, context);
        }
        Ok(())
    }

    pub(crate) fn finalise(self) -> Batch {
        let Self {
            rows_executable: executable,
            uninitialised_reducer_executors: sample_reducers,
            grouped_reductions,
            ..
        } = self;
        let mut batch = Batch::new(
            (executable.input_group_positions.len() + sample_reducers.len()) as u32,
            grouped_reductions.len(),
        );
        let mut reused_row = Vec::with_capacity(executable.input_group_positions.len() + sample_reducers.len());
        let reused_multiplicity = 1;
        for (group, reducers) in grouped_reductions.into_iter() {
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
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>);

    fn finalise(self) -> Option<VariableValue<'static>>;
}

fn extract_value<Snapshot: ReadableSnapshot>(
    row: &MaybeOwnedRow<'_>,
    position: VariablePosition,
    context: &ExecutionContext<Snapshot>,
) -> Option<Value<'static>> {
    match row.get(position) {
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
enum ReducerExecutor {
    Count(CountExecutor),
    CountVar(CountVarExecutor),
    SumInteger(SumIntegerExecutor),
    SumDouble(SumDoubleExecutor),
    MaxInteger(MaxIntegerExecutor),
    MaxDouble(MaxDoubleExecutor),
    MinInteger(MinIntegerExecutor),
    MinDouble(MinDoubleExecutor),
    MeanInteger(MeanIntegerExecutor),
    MeanDouble(MeanDoubleExecutor),
    MedianInteger(MedianIntegerExecutor),
    MedianDouble(MedianDoubleExecutor),
    StdInteger(StdIntegerExecutor),
    StdDouble(StdDoubleExecutor),
}

impl ReducerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        match self {
            ReducerExecutor::Count(reducer) => reducer.accept(row, context),
            ReducerExecutor::CountVar(reducer) => reducer.accept(row, context),
            ReducerExecutor::SumInteger(reducer) => reducer.accept(row, context),
            ReducerExecutor::SumDouble(reducer) => reducer.accept(row, context),
            ReducerExecutor::MaxInteger(reducer) => reducer.accept(row, context),
            ReducerExecutor::MaxDouble(reducer) => reducer.accept(row, context),
            ReducerExecutor::MinInteger(reducer) => reducer.accept(row, context),
            ReducerExecutor::MinDouble(reducer) => reducer.accept(row, context),
            ReducerExecutor::MeanInteger(reducer) => reducer.accept(row, context),
            ReducerExecutor::MeanDouble(reducer) => reducer.accept(row, context),
            ReducerExecutor::MedianInteger(reducer) => reducer.accept(row, context),
            ReducerExecutor::MedianDouble(reducer) => reducer.accept(row, context),
            ReducerExecutor::StdInteger(reducer) => reducer.accept(row, context),
            ReducerExecutor::StdDouble(reducer) => reducer.accept(row, context),
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        match self {
            ReducerExecutor::Count(reducer) => reducer.finalise(),
            ReducerExecutor::CountVar(reducer) => reducer.finalise(),
            ReducerExecutor::SumInteger(reducer) => reducer.finalise(),
            ReducerExecutor::SumDouble(reducer) => reducer.finalise(),
            ReducerExecutor::MaxInteger(reducer) => reducer.finalise(),
            ReducerExecutor::MaxDouble(reducer) => reducer.finalise(),
            ReducerExecutor::MinInteger(reducer) => reducer.finalise(),
            ReducerExecutor::MinDouble(reducer) => reducer.finalise(),
            ReducerExecutor::MeanInteger(reducer) => reducer.finalise(),
            ReducerExecutor::MeanDouble(reducer) => reducer.finalise(),
            ReducerExecutor::MedianInteger(reducer) => reducer.finalise(),
            ReducerExecutor::MedianDouble(reducer) => reducer.finalise(),
            ReducerExecutor::StdInteger(reducer) => reducer.finalise(),
            ReducerExecutor::StdDouble(reducer) => reducer.finalise(),
        }
    }
}

impl ReducerExecutor {
    fn build(reduce_ir: &ReduceInstruction<VariablePosition>) -> Self {
        match *reduce_ir {
            ReduceInstruction::Count => ReducerExecutor::Count(CountExecutor::new()),
            ReduceInstruction::CountVar(pos) => ReducerExecutor::CountVar(CountVarExecutor::new(pos)),
            ReduceInstruction::SumInteger(pos) => ReducerExecutor::SumInteger(SumIntegerExecutor::new(pos)),
            ReduceInstruction::SumDouble(pos) => ReducerExecutor::SumDouble(SumDoubleExecutor::new(pos)),
            ReduceInstruction::MaxInteger(pos) => ReducerExecutor::MaxInteger(MaxIntegerExecutor::new(pos)),
            ReduceInstruction::MaxDouble(pos) => ReducerExecutor::MaxDouble(MaxDoubleExecutor::new(pos)),
            ReduceInstruction::MinInteger(pos) => ReducerExecutor::MinInteger(MinIntegerExecutor::new(pos)),
            ReduceInstruction::MinDouble(pos) => ReducerExecutor::MinDouble(MinDoubleExecutor::new(pos)),
            ReduceInstruction::MeanInteger(pos) => ReducerExecutor::MeanInteger(MeanIntegerExecutor::new(pos)),
            ReduceInstruction::MeanDouble(pos) => ReducerExecutor::MeanDouble(MeanDoubleExecutor::new(pos)),
            ReduceInstruction::MedianInteger(pos) => ReducerExecutor::MedianInteger(MedianIntegerExecutor::new(pos)),
            ReduceInstruction::MedianDouble(pos) => ReducerExecutor::MedianDouble(MedianDoubleExecutor::new(pos)),
            ReduceInstruction::StdInteger(pos) => ReducerExecutor::StdInteger(StdIntegerExecutor::new(pos)),
            ReduceInstruction::StdDouble(pos) => ReducerExecutor::StdDouble(StdDoubleExecutor::new(pos)),
        }
    }
}

#[derive(Debug, Clone)]
struct CountExecutor {
    count: u64,
}

impl CountExecutor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl ReducerAPI for CountExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, _: &ExecutionContext<Snapshot>) {
        self.count += row.multiplicity();
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Integer(self.count as i64)))
    }
}

#[derive(Debug, Clone)]
struct CountVarExecutor {
    count: u64,
    target: VariablePosition,
}
impl CountVarExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { count: 0, target }
    }
}

impl ReducerAPI for CountVarExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, _: &ExecutionContext<Snapshot>) {
        if &VariableValue::Empty != row.get(self.target) {
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Integer(self.count as i64)))
    }
}

#[derive(Debug, Clone)]
struct SumIntegerExecutor {
    sum: i64,
    target: VariablePosition,
}

impl SumIntegerExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0, target }
    }
}

impl ReducerAPI for SumIntegerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            self.sum += value.unwrap_integer() * row.multiplicity() as i64;
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Integer(self.sum)))
    }
}

#[derive(Debug, Clone)]
struct SumDoubleExecutor {
    sum: f64,
    target: VariablePosition,
}

impl SumDoubleExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0.0, target }
    }
}

impl ReducerAPI for SumDoubleExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            self.sum += value.unwrap_double() * row.multiplicity() as f64;
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Double(self.sum)))
    }
}

#[derive(Debug, Clone)]
struct MaxIntegerExecutor {
    max: Option<i64>,
    target: VariablePosition,
}

impl MaxIntegerExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { max: None, target }
    }
}

impl ReducerAPI for MaxIntegerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context).map(|v| v.unwrap_integer()) {
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
        self.max.map(|v| VariableValue::Value(Value::Integer(v)))
    }
}

#[derive(Debug, Clone)]
struct MaxDoubleExecutor {
    max: Option<f64>,
    target: VariablePosition,
}

impl MaxDoubleExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { max: None, target }
    }
}

impl ReducerAPI for MaxDoubleExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context).map(|v| v.unwrap_double()) {
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
struct MinIntegerExecutor {
    min: Option<i64>,
    target: VariablePosition,
}

impl MinIntegerExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { min: None, target }
    }
}

impl ReducerAPI for MinIntegerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context).map(|v| v.unwrap_integer()) {
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
        self.min.map(|v| VariableValue::Value(Value::Integer(v)))
    }
}

#[derive(Debug, Clone)]
struct MinDoubleExecutor {
    min: Option<f64>,
    target: VariablePosition,
}

impl MinDoubleExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { min: None, target }
    }
}

impl ReducerAPI for MinDoubleExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context).map(|v| v.unwrap_double()) {
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

#[derive(Debug, Clone)]
struct MeanIntegerExecutor {
    sum: i64,
    count: u64,
    target: VariablePosition,
}

impl MeanIntegerExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0, count: 0, target }
    }
}

impl ReducerAPI for MeanIntegerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            self.sum += value.unwrap_integer() * row.multiplicity() as i64;
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        if self.count > 0 {
            Some(VariableValue::Value(Value::Double(self.sum as f64 / self.count as f64)))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
struct MeanDoubleExecutor {
    sum: f64,
    count: u64,
    target: VariablePosition,
}

impl MeanDoubleExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0.0, count: 0, target }
    }
}

impl ReducerAPI for MeanDoubleExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            self.sum += value.unwrap_double() * row.multiplicity() as f64;
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        if self.count > 0 {
            Some(VariableValue::Value(Value::Double(self.sum / self.count as f64)))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
struct MedianIntegerExecutor {
    values: Vec<i64>,
    target: VariablePosition,
}

impl MedianIntegerExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { values: Vec::new(), target }
    }
}

impl ReducerAPI for MedianIntegerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            self.values.push(value.unwrap_integer())
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        let Self { mut values, .. } = self;
        values.sort();
        if values.is_empty() {
            None
        } else if values.len() % 2 == 0 {
            let pos = values.len() / 2;
            Some((values[pos - 1] + values[pos]) as f64 / 2.0)
        } else {
            let pos = values.len() / 2;
            Some(values[pos] as f64)
        }
        .map(|v| VariableValue::Value(Value::Double(v)))
    }
}

#[derive(Debug, Clone)]
struct MedianDoubleExecutor {
    values: Vec<f64>,
    target: VariablePosition,
}

impl MedianDoubleExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { values: Vec::new(), target }
    }
}

impl ReducerAPI for MedianDoubleExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            self.values.push(value.unwrap_double())
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        let Self { mut values, .. } = self;
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if values.is_empty() {
            None
        } else if values.len() % 2 == 0 {
            let pos = values.len() / 2;
            Some((values[pos - 1] + values[pos]) / 2.0)
        } else {
            let pos = values.len() / 2;
            Some(values[pos])
        }
        .map(|v| VariableValue::Value(Value::Double(v)))
    }
}

#[derive(Debug, Clone)]
struct StdIntegerExecutor {
    sum: i64,
    sum_squares: i128,
    count: u64,
    target: VariablePosition,
}

impl StdIntegerExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0, sum_squares: 0, count: 0, target }
    }
}

impl ReducerAPI for StdIntegerExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            let unwrapped = value.unwrap_integer();
            self.sum_squares += (unwrapped as i128 * unwrapped as i128) * row.multiplicity() as i128;
            self.sum += unwrapped * row.multiplicity() as i64;
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        if self.count > 1 {
            let sum = self.sum as f64;
            let sum_squares = self.sum_squares as f64;
            let n = self.count as f64;
            let mean = sum / n;
            let sample_variance: f64 = (sum_squares + n * mean * mean - 2.0 * sum * mean) / (n - 1.0);
            Some(VariableValue::Value(Value::Double(sample_variance.sqrt())))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
struct StdDoubleExecutor {
    sum: f64,
    sum_squares: f64,
    count: u64,
    target: VariablePosition,
}

impl StdDoubleExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: 0.0, sum_squares: 0.0, count: 0, target }
    }
}

impl ReducerAPI for StdDoubleExecutor {
    fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
        if let Some(value) = extract_value(row, self.target, context) {
            let unwrapped = value.unwrap_double();
            self.sum_squares += (unwrapped * unwrapped) * row.multiplicity() as f64;
            self.sum += unwrapped * row.multiplicity() as f64;
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        if self.count > 1 {
            let sum = self.sum;
            let sum_squares = self.sum_squares;
            let n = self.count as f64;
            let mean = sum / n;
            let sample_variance: f64 = (sum_squares + n * mean * mean - 2.0 * sum * mean) / (n - 1.0);
            Some(VariableValue::Value(Value::Double(sample_variance.sqrt())))
        } else {
            None
        }
    }
}
