/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use criterion::{BatchSize, BenchmarkGroup, Criterion, measurement::Measurement};

use crate::{Config, Context};

pub trait SimpleBenchmark {
    type IterInput;
    fn init_context(&mut self) -> Context {
        Context::init(Config::default())
    }

    fn name(&self) -> String;
    fn num_iterations(&self) -> usize {
        1
    }

    fn before_all(&mut self, context: &mut Context) {}

    fn prepare(&self, context: &Context) -> Self::IterInput;

    fn run(&self, context: &Context, input: &mut Self::IterInput);
}

pub fn run_simple_benchmark<M: Measurement>(group: &mut BenchmarkGroup<M>, mut benchmark: impl SimpleBenchmark) {
    let mut context = benchmark.init_context();
    benchmark.before_all(&mut context);
    group.bench_function(benchmark.name().as_str(), |b| {
        b.iter_batched_ref(
            || benchmark.prepare(&context),
            |input_ref| benchmark.run(&context, input_ref),
            BatchSize::PerIteration,
        );
    });
}
