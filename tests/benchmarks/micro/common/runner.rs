/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use clap::Parser;
use criterion::{BatchSize, Criterion, measurement::Measurement};

use crate::templates::{SimpleBenchmark, SimpleReport};

pub trait BenchmarkRunnerGroup {
    fn run_benchmark<T: SimpleBenchmark>(&mut self, b: T) -> Vec<T::IterOutput>;
}

pub trait BenchmarkRunner {
    fn new_group(&mut self, name: &str) -> impl BenchmarkRunnerGroup;
    fn summary(&mut self);
}

// Criterion
const CRITERION_SAMPLE_SIZE: usize = 20;

impl<M: Measurement> BenchmarkRunner for Criterion<M> {
    fn new_group(&mut self, name: &str) -> criterion::BenchmarkGroup<M> {
        let mut group = self.benchmark_group(name);
        group.sample_size(CRITERION_SAMPLE_SIZE);
        group
    }

    fn summary(&mut self) {
        self.final_summary()
    }
}

impl<'a, M: Measurement> BenchmarkRunnerGroup for criterion::BenchmarkGroup<'a, M> {
    fn run_benchmark<T: SimpleBenchmark>(&mut self, benchmark: T) -> Vec<T::IterOutput> {
        let mut context = benchmark.init_context();
        benchmark.before_all(&mut context);
        let mut outputs = Vec::with_capacity(CRITERION_SAMPLE_SIZE);
        self.bench_function(benchmark.name(), |bencher| {
            // This should also be run only once per "batch"
            // We create the database outside the batch creation so the Arc isn't dropped in the timed part
            let database = benchmark.create_database(&mut context);
            benchmark.prepare_database(&mut context, database.clone());
            bencher.iter_batched(
                || benchmark.prepare_iter(&context, database.clone()),
                |input| {
                    let result = benchmark.run_iter(&context, database.clone(), input);
                    outputs.push(result);
                },
                BatchSize::PerIteration,
            );
            drop(database);
        });
        outputs
    }
}

// Simple profile
#[derive(Parser)]
pub struct SimpleRunner {
    /// Only run benchmarks whose name contains this substring.
    #[arg(default_value = "")]
    pub filter: String,

    /// Record a flamegraph SVG for each benchmark run.
    #[arg(long)]
    pub flamegraph: bool,
}

impl BenchmarkRunner for SimpleRunner {
    fn new_group(&mut self, name: &str) -> impl BenchmarkRunnerGroup {
        SimpleRunnerGroup { name: name.to_owned(), runner: self }
    }

    fn summary(&mut self) {
        // nothing
    }
}

pub struct SimpleRunnerGroup<'runner> {
    name: String,
    runner: &'runner SimpleRunner,
}

impl<'runner> BenchmarkRunnerGroup for SimpleRunnerGroup<'runner> {
    fn run_benchmark<T: SimpleBenchmark>(&mut self, b: T) -> Vec<T::IterOutput> {
        let combined_name = format!("{}::{}", &self.name, b.name());
        if !combined_name.contains(&self.runner.filter) {
            return vec![];
        }
        println!("[.] SimpleRunner: Running benchmark {}", combined_name);
        let mut context = b.init_context();
        b.before_all(&mut context);
        let database = b.create_database(&mut context);
        b.prepare_database(&mut context, database.clone());
        let input = b.prepare_iter(&context, database.clone());

        let guard =
            self.runner.flamegraph.then(|| pprof::ProfilerGuard::new(100).expect("failed to start pprof profiler"));

        let iter_result = b.run_iter(&context, database.clone(), input);

        if let Some(guard) = guard {
            if let Ok(report) = guard.report().build() {
                let dir = std::env::current_dir().unwrap().join("flamegraphs");
                std::fs::create_dir_all(&dir).unwrap();
                let path = dir.join(format!("{}.svg", combined_name.replace("::", "_")));
                let file = std::fs::File::create(&path).unwrap();
                report.flamegraph(file).expect("failed to write flamegraph");
                println!("Wrote flamegraph to {}", path.display());
            }
        }

        drop(database);
        let outputs = vec![iter_result];
        <T::IterOutput as SimpleReport>::report(&outputs);
        outputs
    }
}
