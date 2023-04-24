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

import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.assertEquals;

class Benchmark {
    private static final PrintStream printTo = System.out;
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

    void mayPrintResults() {
        if (printTo != null) {
            printTo.println(toCSV());
        }
    }

    public String toCSV() {
        List<String> fields = new ArrayList<>();
        Arrays.stream(new String[] {
                "name", "expectedAnswers", "actualAnswers", "total_time_ms",
        }).forEach(fields::add);
        List<String> perfCounterKeys = new ArrayList<>(new ReasonerPerfCounters(false).toMapUnsynchronised().keySet());
        fields.addAll(perfCounterKeys);

        StringBuilder sb = new StringBuilder();
        appendCSVLine(sb, fields);
        runs.forEach(run ->  appendCSVLine(sb, run.toCSV(this, perfCounterKeys)));
        return sb.toString();
    }

    private static void appendCSVLine(StringBuilder sb, List<String> entries) {
        entries.forEach(entry -> sb.append(entry).append(","));
        sb.append("\n");
    }
}
