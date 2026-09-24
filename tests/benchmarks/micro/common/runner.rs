/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use clap::Parser;

use crate::{benchmark::SimpleBenchmark, reports::SimpleReport};

pub trait BenchmarkRunnerGroup {
    fn run_benchmark<T: SimpleBenchmark>(&mut self, b: T) -> Vec<T::IterOutput>;
}

pub trait BenchmarkRunner {
    fn new_group(&mut self, name: &str) -> impl BenchmarkRunnerGroup;
    fn summary(&mut self);
}

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
        let input = b.prepare_run(&context, database.clone());

        let guard =
            self.runner.flamegraph.then(|| pprof::ProfilerGuard::new(100).expect("failed to start pprof profiler"));

        let iter_result = b.run_benchmark(&context, database.clone(), input);

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
