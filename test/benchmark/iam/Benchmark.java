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
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.perfcounter.PerfCounterSet;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import junit.framework.AssertionFailedError;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Benchmark {

    private static final String RESOURCE_DIRECTORY = "test/benchmark/iam/resources/";
    private static CoreDatabaseManager databaseMgr;
    private final String database;
    private final boolean collectResults;
    private final List<BenchmarkSummary> results;

    Benchmark(String database, boolean collectResults) {
        this.database = database;
        this.collectResults = collectResults;
        this.results = new ArrayList<>();
    }

    void setUp() throws IOException {
        Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("iam-benchmark-conjunctions");
        com.vaticle.typedb.core.test.integration.util.Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(new Options.Database().dataDir(dataDir).storageDataCacheSize(MB).storageIndexCacheSize(MB));
        databaseMgr.create(database);
    }

    void tearDown() {
        databaseMgr.close();
    }

    private TypeDB.Session schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private TypeDB.Session dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    void loadSchema(String... filenames) {
        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                iterate(filenames).forEachRemaining(filename -> {
                    try {
                        TypeQLDefine defineQuery = TypeQL.parseQuery(Files.readString(Paths.get(RESOURCE_DIRECTORY + filename))).asDefine();
                        tx.query().define(defineQuery);
                    } catch (IOException e) {
                        fail("IOException when loading schema: " + e.getMessage());
                    }
                });
                tx.commit();
            }
        }
    }

    void loadData(String... filenames) {
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                iterate(filenames).forEachRemaining(filename -> {
                    try {
                        TypeQLInsert insertQuery = TypeQL.parseQuery(Files.readString(Paths.get(RESOURCE_DIRECTORY + filename))).asInsert();
                        tx.query().insert(insertQuery);
                    } catch (IOException e) {
                        fail("IOException when loading data: " + e.getMessage());
                    }
                });
                tx.commit();
            }
        }
    }

    BenchmarkSummary benchmarkMatchQuery(String name, String query, int expectedAnswers, int nRuns) {
        BenchmarkSummary summary = new BenchmarkSummary(name, query, expectedAnswers, nRuns);
        for (int i = 0; i < nRuns; i++) {
            summary.addRun(runMatchQuery(query));
        }
        if (collectResults) results.add(summary);
        return summary;
    }

    BenchmarkRun runMatchQuery(String query) {
        BenchmarkRun run;
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                Instant start = Instant.now();
                long nAnswers = tx.query().match(TypeQL.parseQuery(query).asMatch()).count();
                Duration timeTaken = Duration.between(start, Instant.now());
                run = new BenchmarkRun(nAnswers, timeTaken, ((CoreTransaction) tx).reasoner().controllerRegistry().perfCounters().toMapUnsynchronised());
            }
        }

        return run;
    }

    class BenchmarkSummary {

        final String name;
        final String query;
        final long expectedAnswers;
        private final int nRuns;
        final List<BenchmarkRun> runs;

        BenchmarkSummary(String name, String query, long expectedAnswers, int nRuns) {
            this.name = name;
            this.query = query;
            this.expectedAnswers = expectedAnswers;
            this.nRuns = nRuns;
            this.runs = new ArrayList<>();
        }

        void addRun(BenchmarkRun run) {
            runs.add(run);
        }

        public List<BenchmarkRun> runs() {
            return runs;
        }

        public JsonObject toJson() {
            return toJson(ReasonerPerfCounters.Key.values());
        }

        public JsonObject toJson(ReasonerPerfCounters.Key[] values) {
            JsonArray jsonRuns = Json.array();
            runs.forEach(run -> jsonRuns.add(run.toJSON(values)));
            return Json.object()
                    .add("name", Json.value(name))
                    .add("query", Json.value(query))
                    .add("expected_answers", Json.value(expectedAnswers))
                    .add("runs", jsonRuns);
        }

        public void assertAnswerCountCorrect() {
            assertEquals(iterate(runs).map(run -> expectedAnswers).toList(), iterate(runs()).map(run -> run.answerCount).toList());
            assertEquals(nRuns, runs.size());
        }

        public boolean isCorrect() {
            try {
                assertAnswerCountCorrect();
                return true;
            } catch (AssertionFailedError e) {
                return false;
            }
        }
    }

    static class BenchmarkRun {
        final long answerCount;
        final Duration timeTaken;
        final Exception exception;
        private final Map<PerfCounterSet.Key, Long> reasonerPerfCounters;

        public BenchmarkRun(Exception e) {
            this(-1, null, null, e);
        }

        public  BenchmarkRun(long answerCount, Duration timeTaken, Map<PerfCounterSet.Key, Long> reasonerPerfCounters) {
            this(answerCount, timeTaken, reasonerPerfCounters, null);
        }

        private BenchmarkRun(long answerCount, Duration timeTaken, Map<PerfCounterSet.Key, Long> reasonerPerfCounters, Exception e) {
            this.answerCount = answerCount;
            this.timeTaken = timeTaken;
            this.reasonerPerfCounters = reasonerPerfCounters;
            this.exception = e;
        }

        public JsonObject toJSON(ReasonerPerfCounters.Key[] values) {
            JsonObject json;
            if (exception == null) {
                json = Json.object()
                        .add("answer_count", Json.value(answerCount))
                        .add("time_taken_ms", Json.value(timeTaken.toMillis()));

                Arrays.stream(values).forEach(key -> {
                    json.add(key.name(), Json.value(reasonerPerfCounters.get(key)));
                });
            } else {
                json = Json.object().add("exception", Json.value(exception.toString()));
            }

            return json;
        }

        @Override
        public String toString() {
            return "Benchmark run:\n" +
                    "\tTimeTaken :\t" + timeTaken.toMillis() + " ms\n" +
                    "\tAnswers   :\t" + answerCount + "\n" +
                    PerfCounterSet.prettyPrint(reasonerPerfCounters);
        }
    }

    public abstract static class ReasonerBenchmarkSuite {
        final Benchmark benchmarker;
        private Exception exception;

        ReasonerBenchmarkSuite(String database, boolean collectResults) {
            benchmarker = new Benchmark(database, collectResults);
        }
        abstract void setUp() throws IOException;

        abstract void tearDown();

        public List<BenchmarkSummary> results() {
            return benchmarker.results;
        }

        public JsonObject jsonSummary() {
            JsonObject root = new JsonObject();
            iterate(results()).forEachRemaining(summary -> {
                root.add(summary.name, summary.toJson());
            });
            if (exception != null) root.add("exception", Json.value(exception.toString()));
            return root;
        }

        public void exception(Exception e) {
            this.exception = e;
        }

        public Exception exception() {
            return this.exception;
        }
    }
}
