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

import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class Benchmark {
    private static final double COUNTER_MARGIN = 1.5;
    static final AbstractProcessor.Context.ReasonerPerfCounters PERF_KEYS = new AbstractProcessor.Context.ReasonerPerfCounters(false);

    final String name;
    final String query;
    final long expectedAnswers;
    final int nRuns;
    final List<BenchmarkRunner.BenchmarkRun> runs;

    Benchmark(String name, String query, long expectedAnswers) {
        this(name, query, expectedAnswers, 1);
    }

    Benchmark(String name, String query, long expectedAnswers, int nRuns) {
        this.name = name;
        this.query = query;
        this.expectedAnswers = expectedAnswers;
        this.nRuns = nRuns;
        this.runs = new ArrayList<>();
    }

    void addRun(BenchmarkRunner.BenchmarkRun run) {
        runs.add(run);
    }

    public void assertAnswerCountCorrect() {
        assertEquals(iterate(runs).map(run -> expectedAnswers).toList(), iterate(runs).map(run -> run.answerCount).toList());
        assertEquals(nRuns, runs.size());
    }

    public void assertRunningTime(long maxTimeMs) {
        runs.forEach(run -> assertTrue(
                String.format("Time taken: %d <= %d", run.timeTaken.toMillis(), maxTimeMs),
                run.timeTaken.toMillis() <= maxTimeMs));
    }

    public void assertCounter(PerfCounters.Counter counter, long maxValue) {
        runs.forEach(run -> assertTrue(
                String.format("%s: %d <= %d", counter.name(), run.reasonerPerfCounters.get(counter.name()), maxValue),
                run.reasonerPerfCounters.get(counter.name()) <= maxValue));
    }

    public void assertCounters(long timePlanningMs, long countMaterialisations, long countConjunctionProcessors, long countCompoundStreams) {
        assertCounter(PERF_KEYS.timePlanningMs, Math.round(timePlanningMs * COUNTER_MARGIN));
        assertCounter(PERF_KEYS.countMaterialisations, Math.round(countMaterialisations * COUNTER_MARGIN));
        assertCounter(PERF_KEYS.countConjunctionProcessors, Math.round(countConjunctionProcessors * COUNTER_MARGIN));
        assertCounter(PERF_KEYS.countCompoundStreams, Math.round(countCompoundStreams * COUNTER_MARGIN));
    }
}
