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
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
import junit.framework.AssertionFailedError;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.assertEquals;

class Benchmark {
    final String name;
    final String query;
    final long expectedAnswers;
    final int nRuns;
    final List<BenchmarkRunner.BenchmarkRun> runs;

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

    public List<BenchmarkRunner.BenchmarkRun> runs() {
        return runs;
    }

    public void assertAnswerCountCorrect() {
        assertEquals(iterate(runs).map(run -> expectedAnswers).toList(), iterate(runs()).map(run -> run.answerCount).toList());
        assertEquals(nRuns, runs.size());
    }

    public boolean allCorrect() {
        try {
            assertAnswerCountCorrect();
            return true;
        } catch (AssertionFailedError e) {
            return false;
        }
    }

    public JsonObject toJson() {
        JsonArray jsonRuns = Json.array();
        runs.forEach(run -> jsonRuns.add(run.toJSON()));
        return Json.object()
                .add("name", Json.value(name))
                .add("query", Json.value(query))
                .add("expected_answers", Json.value(expectedAnswers))
                .add("runs", jsonRuns)
                .add("all_correct", Json.value(allCorrect()));
    }
}
