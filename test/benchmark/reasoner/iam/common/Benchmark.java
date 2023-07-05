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

package com.vaticle.typedb.core.reasoner.benchmark.iam.common;

import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Benchmark {

    private static final int DEFAULT_NRUNS = 5;
    private static final double COUNTER_LOWER_MARGIN = 0.75;
    private static final double COUNTER_UPPER_MARGIN = 1.25;
    private static final double RUNNING_TIME_UPPER_MARGIN = 3.0;

    final String name;
    final String query;
    final long expectedAnswers;
    final int nRuns;
    final List<BenchmarkRun> runs;

    public Benchmark(String name, String query, long expectedAnswers) {
        this(name, query, expectedAnswers, DEFAULT_NRUNS);
    }

    public Benchmark(String name, String query, long expectedAnswers, int nRuns) {
        this.name = name;
        this.query = query;
        this.expectedAnswers = expectedAnswers;
        this.nRuns = nRuns;
        this.runs = new ArrayList<>();
    }

    void addRun(BenchmarkRun run) {
        runs.add(run);
    }

    public void assertAnswerCountCorrect() {
        assertEquals(iterate(runs).map(run -> expectedAnswers).toList(), iterate(runs).map(run -> run.answerCount).toList());
        assertEquals(nRuns, runs.size());
    }

    public void assertRunningTime(long maxTimeMs) {
        runs.forEach(run -> assertTrue(
                String.format("Time taken: %d <= %f * %d", run.timeTaken.toMillis(), RUNNING_TIME_UPPER_MARGIN, maxTimeMs),
                run.timeTaken.toMillis() <= Math.round(RUNNING_TIME_UPPER_MARGIN * maxTimeMs)));
    }

    public void assertCounterUpperBound(String counter, long refValue) {
        runs.forEach(run -> {
            assertTrue(
                    String.format("%s: %d <= %d", counter, run.reasonerPerfCounters.get(counter), Math.round(COUNTER_UPPER_MARGIN * refValue)),
                    run.reasonerPerfCounters.get(counter) <= Math.round(COUNTER_UPPER_MARGIN * refValue));
        });
    }

    public void assertCounterLowerBound(String counter, long refValue) {
        assertTrue( // If this error throws, It's time to revise the bound.
                String.format("[GOOD FAILURE!] Counter %s consistently better than lower bound of %d", counter, Math.round(COUNTER_LOWER_MARGIN * refValue)),
                iterate(runs).anyMatch(run -> run.reasonerPerfCounters.get(counter) >= Math.round(COUNTER_LOWER_MARGIN * refValue)));
    }

    public void assertCounters(long planningTimeMillis, long materialisations, long conjunctionProcessors, long compoundStreams, long compoundStreamMessagesReceived) {
        assertCounterUpperBound(ReasonerPerfCounters.PLANNING_TIME_NS, planningTimeMillis * 1_000_000);
        assertCounterUpperBound(ReasonerPerfCounters.MATERIALISATIONS, materialisations);
        assertCounterUpperBound(ReasonerPerfCounters.CONJUNCTION_PROCESSORS, conjunctionProcessors);
        assertCounterUpperBound(ReasonerPerfCounters.COMPOUND_STREAMS, compoundStreams);
        assertCounterUpperBound(ReasonerPerfCounters.COMPOUND_STREAM_MESSAGES_RECEIVED, compoundStreamMessagesReceived);

        // Do not assert lower bound for time planning. Times are too variable.
        assertCounterLowerBound(ReasonerPerfCounters.MATERIALISATIONS, materialisations);
        assertCounterLowerBound(ReasonerPerfCounters.CONJUNCTION_PROCESSORS, conjunctionProcessors);
        assertCounterLowerBound(ReasonerPerfCounters.COMPOUND_STREAMS, compoundStreams);
        assertCounterLowerBound(ReasonerPerfCounters.COMPOUND_STREAM_MESSAGES_RECEIVED, compoundStreamMessagesReceived);
    }

    public static class BenchmarkRun {
        final long answerCount;
        final Duration timeTaken;
        final Map<String, Long> reasonerPerfCounters;

        public BenchmarkRun(long answerCount, Duration timeTaken, PerfCounters reasonerPerfCounters) {
            this.answerCount = answerCount;
            this.timeTaken = timeTaken;
            this.reasonerPerfCounters = new HashMap<>();
            iterate(reasonerPerfCounters.counters())
                    .filter(counter -> BenchmarkRunner.CSVBuilder.perfCounterKeys.contains(counter.name()))
                    .forEachRemaining(counter -> this.reasonerPerfCounters.put(counter.name(), counter.get()));

        }

        @Override
        public String toString() {
            StringBuilder perfCounterStr = new StringBuilder();
            reasonerPerfCounters.forEach((k, v) -> perfCounterStr.append(String.format("|-- %-40s :\t%d\n", k, v)));
            return "Benchmark run:\n" +
                    "- TimeTaken      :\t" + timeTaken.toMillis() + " ms\n" +
                    "- Answers        :\t" + answerCount + "\n" +
                    "- PerfCounters   :\t\n" + perfCounterStr + "\n";
        }
    }
}
