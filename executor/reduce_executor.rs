/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use answer::{variable_value::VariableValue, Thing};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use compiler::{
    executable::reduce::{ReduceInstruction, ReduceRowsExecutable},
    VariablePosition,
};
use encoding::value::{decimal_value::Decimal, timezone::TimeZone, value::Value};
use paste::paste;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::Batch,
    pipeline::{stage::ExecutionContext, PipelineExecutionError},
    row::MaybeOwnedRow,
    Provenance,
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
        for (group, reducers) in grouped_reductions.into_iter() {
            batch.append(|mut row| {
                group
                    .into_iter()
                    .chain(reducers.into_iter().map(|reducer| reducer.finalise().unwrap_or(VariableValue::None)))
                    .enumerate()
                    .for_each(|(index, value)| row.set(VariablePosition::new(index as u32), value));
                // Reducers combine many rows. provenance is pointless
                row.set_multiplicity(1);
                row.set_provenance(Provenance::INITIAL)
            });
        }
        batch
    }
}

trait ReducerAPI {
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    );

    fn finalise(self) -> Option<VariableValue<'static>>;
}

fn extract_value<Snapshot: ReadableSnapshot>(
    row: &MaybeOwnedRow<'_>,
    position: VariablePosition,
    context: &ExecutionContext<Snapshot>,
    storage_counters: StorageCounters,
) -> Option<Value<'static>> {
    match row.get(position) {
        VariableValue::None => None,
        VariableValue::Value(value) => Some(value.clone().into_owned()),
        VariableValue::Thing(Thing::Attribute(attribute)) => {
            // As long as these are trivial, it's safe to unwrap
            let snapshot: &Snapshot = &context.snapshot;
            let value = attribute.get_value(snapshot, &context.thing_manager, storage_counters).unwrap();
            Some(value.clone().into_owned())
        }
        _ => unreachable!(),
    }
}

macro_rules! reducer_executor {
    ($count:ident, $($variant:ident),* $(,)?) => {
        paste! {
            #[derive(Debug, Clone)]
            enum ReducerExecutor {
                $count([<$count Executor>]),
                $($variant([<$variant Executor>])),*
            }

            impl ReducerExecutor {
                fn accept<Snapshot: ReadableSnapshot>(&mut self, row: &MaybeOwnedRow<'_>, context: &ExecutionContext<Snapshot>) {
                    let profile = context.profile.profile_stage(|| String::from("Reduce"), 0); // TODO executable id
                    let step_profile = profile.extend_or_get(0, || String::from("Reduce execution"));
                    let storage_counters = step_profile.storage_counters();
                    match self {
                        Self::$count(reducer) => reducer.accept(row, context, storage_counters),
                        $(Self::$variant(reducer) => reducer.accept(row, context, storage_counters)),*
                    }
                }

                fn finalise(self) -> Option<VariableValue<'static>> {
                    match self {
                        Self::$count(reducer) => reducer.finalise(),
                        $(Self::$variant(reducer) => reducer.finalise()),*
                    }
                }
            }

            impl ReducerExecutor {
                fn build(reduce_ir: &ReduceInstruction<VariablePosition>) -> Self {
                    match *reduce_ir {
                        ReduceInstruction::$count => ReducerExecutor::$count([<$count Executor>]::new()),
                        $(ReduceInstruction::$variant(pos) => Self::$variant([<$variant Executor>]::new(pos))),*
                    }
                }
            }
        }
    };
}

reducer_executor! {
    Count,
    CountVar,
    SumInteger, MaxInteger, MinInteger, MeanInteger, MedianInteger, StdInteger,
    SumDouble, MaxDouble, MinDouble, MeanDouble, MedianDouble, StdDouble,
    SumDecimal, MaxDecimal, MinDecimal, MeanDecimal, MedianDecimal, StdDecimal,
    MaxString, MinString,
    MaxDate, MinDate,
    MaxDateTime, MinDateTime,
    MaxDateTimeTZ, MinDateTimeTZ,
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        _: &ExecutionContext<Snapshot>,
        _: StorageCounters,
    ) {
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        _: &ExecutionContext<Snapshot>,
        _: StorageCounters,
    ) {
        if &VariableValue::None != row.get(self.target) {
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::Integer(self.count as i64)))
    }
}

macro_rules! sum_reducer_executors {
    ($($ty:ident($repr:ty) $u64_to_repr:expr),* $(,)?) => {$(
        paste! {
            #[derive(Debug, Clone)]
            struct [< Sum $ty Executor >] {
                sum: $repr,
                target: VariablePosition,
            }

            impl [< Sum $ty Executor >] {
                fn new(target: VariablePosition) -> Self {
                    Self { sum: $repr::default(), target }
                }
            }

            impl ReducerAPI for [< Sum $ty Executor >] {
                fn accept<Snapshot: ReadableSnapshot>(
                    &mut self,
                    row: &MaybeOwnedRow<'_>,
                    context: &ExecutionContext<Snapshot>,
                    storage_counters: StorageCounters,
                ) {
                    if let Some(value) = extract_value(row, self.target, context, storage_counters) {
                        self.sum += value.[< unwrap_ $ty:lower >]() * $u64_to_repr(row.multiplicity());
                    }
                }

                fn finalise(self) -> Option<VariableValue<'static>> {
                    Some(VariableValue::Value(Value::$ty(self.sum)))
                }
            }
        }
    )*};
}

sum_reducer_executors! {
    Integer(i64) (|x| x as i64),
    Double(f64) (|x| x as f64),
    Decimal(Decimal) (|x| x),
}

macro_rules! minmax_reducer_executors {
    ($($ty:ident::$lower:ident($repr:ty)),* $(,)?) => {$(
        paste! {
            #[derive(Debug, Clone)]
            struct [< Min $ty Executor >] {
                min: Option<$repr>,
                target: VariablePosition,
            }

            impl [< Min $ty Executor >] {
                fn new(target: VariablePosition) -> Self {
                    Self { min: None, target }
                }
            }

            impl ReducerAPI for [< Min $ty Executor >] {
                fn accept<Snapshot: ReadableSnapshot>(
                    &mut self,
                    row: &MaybeOwnedRow<'_>,
                    context: &ExecutionContext<Snapshot>,
                    storage_counters: StorageCounters,
                ) {
                    if let Some(value) = extract_value(row, self.target, context, storage_counters).map(|v| v.[< unwrap_ $lower >]()) {
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
                    Some(VariableValue::Value(Value::$ty(self.min?)))
                }
            }

            #[derive(Debug, Clone)]
            struct [< Max $ty Executor >] {
                max: Option<$repr>,
                target: VariablePosition,
            }

            impl [< Max $ty Executor >] {
                fn new(target: VariablePosition) -> Self {
                    Self { max: None, target }
                }
            }

            impl ReducerAPI for [< Max $ty Executor >] {
                fn accept<Snapshot: ReadableSnapshot>(
                    &mut self,
                    row: &MaybeOwnedRow<'_>,
                    context: &ExecutionContext<Snapshot>,
                    storage_counters: StorageCounters,
                ) {
                    if let Some(value) = extract_value(row, self.target, context, storage_counters).map(|v| v.[< unwrap_ $lower >]()) {
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
                    Some(VariableValue::Value(Value::$ty(self.max?)))
                }
            }
        }
    )*};
}

minmax_reducer_executors! {
    Integer::integer(i64),
    Double::double(f64),
    Decimal::decimal(Decimal),
    Date::date(NaiveDate),
    DateTime::date_time(NaiveDateTime),
    DateTimeTZ::date_time_tz(DateTime<TimeZone>),
}

#[derive(Debug, Clone)]
struct MinStringExecutor {
    min: Option<String>,
    target: VariablePosition,
}

impl MinStringExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { min: None, target }
    }
}

impl ReducerAPI for MinStringExecutor {
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters).map(|v| v.unwrap_string()) {
            if let Some(current) = self.min.as_ref() {
                if *value < **current {
                    self.min = Some(value.into_owned())
                }
            } else {
                self.min = Some(value.into_owned());
            }
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::String(Cow::Owned(self.min?))))
    }
}

#[derive(Debug, Clone)]
struct MaxStringExecutor {
    max: Option<String>,
    target: VariablePosition,
}

impl MaxStringExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { max: None, target }
    }
}

impl ReducerAPI for MaxStringExecutor {
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters).map(|v| v.unwrap_string()) {
            if let Some(current) = self.max.as_ref() {
                if *value > **current {
                    self.max = Some(value.into_owned())
                }
            } else {
                self.max = Some(value.into_owned());
            }
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        Some(VariableValue::Value(Value::String(Cow::Owned(self.max?))))
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
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
struct MeanDecimalExecutor {
    sum: Decimal,
    count: u64,
    target: VariablePosition,
}

impl MeanDecimalExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: Decimal::default(), count: 0, target }
    }
}

impl ReducerAPI for MeanDecimalExecutor {
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
            self.sum += value.unwrap_decimal() * row.multiplicity();
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        if self.count > 0 {
            Some(VariableValue::Value(Value::Decimal(self.sum / self.count)))
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
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
struct MedianDecimalExecutor {
    values: Vec<Decimal>,
    target: VariablePosition,
}

impl MedianDecimalExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { values: Vec::new(), target }
    }
}

impl ReducerAPI for MedianDecimalExecutor {
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
            self.values.push(value.unwrap_decimal())
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        let Self { mut values, .. } = self;
        values.sort();
        if values.is_empty() {
            None
        } else if values.len() % 2 == 0 {
            let pos = values.len() / 2;
            Some((values[pos - 1] + values[pos]) / 2)
        } else {
            let pos = values.len() / 2;
            Some(values[pos])
        }
        .map(|v| VariableValue::Value(Value::Decimal(v)))
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
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
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
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

#[derive(Debug, Clone)]
struct StdDecimalExecutor {
    sum: Decimal,
    sum_squares: Decimal,
    count: u64,
    target: VariablePosition,
}

impl StdDecimalExecutor {
    fn new(target: VariablePosition) -> Self {
        Self { sum: Decimal::default(), sum_squares: Decimal::default(), count: 0, target }
    }
}

impl ReducerAPI for StdDecimalExecutor {
    fn accept<Snapshot: ReadableSnapshot>(
        &mut self,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<Snapshot>,
        storage_counters: StorageCounters,
    ) {
        if let Some(value) = extract_value(row, self.target, context, storage_counters) {
            let unwrapped = value.unwrap_decimal();
            self.sum_squares += (unwrapped * unwrapped) * row.multiplicity();
            self.sum += unwrapped * row.multiplicity();
            self.count += row.multiplicity();
        }
    }

    fn finalise(self) -> Option<VariableValue<'static>> {
        if self.count > 1 {
            let sum = self.sum;
            let sum_squares = self.sum_squares;
            let n = self.count;
            let mean = sum / n;
            let sample_variance: Decimal = (sum_squares + n * mean * mean - 2 * sum * mean) / (n - 1);
            Some(VariableValue::Value(Value::Double(sample_variance.to_f64().sqrt())))
        } else {
            None
        }
    }
}
