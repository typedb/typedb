/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.reasoner.benchmark.iam;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.WriterConfig;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ReasonerBenchmarkSuite {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

    protected final BenchmarkRunner benchmarker;
    final List<Benchmark> benchmarks;

    ReasonerBenchmarkSuite(String database) {
        this.benchmarker = new BenchmarkRunner(database);
        this.benchmarks = new ArrayList<>();
    }

    abstract void setUp() throws IOException;

    abstract void tearDown();

    public List<Benchmark> benchmarks() {
        return benchmarks;
    }

    void runBenchmark(Benchmark benchmark) {
        for (int i = 0; i < benchmark.nRuns; i++) {
            BenchmarkRunner.BenchmarkRun run = benchmarker.runMatchQuery(benchmark.query);
            benchmark.addRun(run);
            LOG.info("Completed run in {} ms. answersDiff: {}", run.timeTaken.toMillis(), run.answerCount - benchmark.expectedAnswers);
            LOG.info("perf_counters:\n{}", run.toJSON().toString(WriterConfig.PRETTY_PRINT));
        }
        benchmarks.add(benchmark);
    }

    public JsonObject jsonSummary() {
        JsonObject root = Json.object();
        iterate(benchmarks).forEachRemaining(summary -> {
            root.add(summary.name, summary.toJson());
        });
        return root;
    }
}
