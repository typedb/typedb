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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.migrator.database.DatabaseImporter;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typedb.core.server.Version;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

public class BenchmarkRunner {

    private static boolean warmupRunForEachQuery = true;
    private static final boolean PRINT_RESULTS = true;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);
    private static CoreDatabaseManager databaseMgr;
    private final String database;
    private final CSVBuilder csvBuilder;

    public BenchmarkRunner(String database) {
        this.database = database;
        this.csvBuilder = PRINT_RESULTS ? new CSVBuilder() : null;
    }

    public void setUp() throws IOException {
        Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
        if (Files.exists(dataDir)) {
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        Files.createDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(new Options.Database().dataDir(dataDir).storageDataCacheSize(MB).storageIndexCacheSize(MB));
    }

    public void tearDown() {
        databaseMgr.close();
        if (this.csvBuilder != null) System.out.println(csvBuilder.build());
    }

    public void reset() {

    }

    private TypeDB.Session schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private TypeDB.Session dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    public void loadDatabase(Path schemaFile, Path dataFile) {
        new DatabaseImporter(databaseMgr, database, schemaFile, dataFile, Version.VERSION).run();
    }

    public void loadSchema(Path schemaFile) {
        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                try {
                    TypeQLDefine defineQuery = TypeQL.parseQuery(Files.readString(schemaFile, UTF_8)).asDefine();
                    tx.query().define(defineQuery);
                } catch (IOException e) {
                    fail("IOException when loading schema: " + e.getMessage());
                }
                tx.commit();
            }
        }
    }

    public void warmUp() {
        // Best effort warm-up
        long start = System.nanoTime();
        // Populate LogicManager caches
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true).reasonerPerfCounters(true))) {
                tx.logic().rules().flatMap(rule -> iterate(rule.condition().disjunction().conjunctions()))
                        .flatMap(resolvableConjunction -> resolvableConjunction.allConcludables())
                        .forEachRemaining(concludable -> tx.logic().applicableRules(concludable));
            }
        }
        // A dummy reasoning query
        for (int i = 0; i < 1; i++) runMatchQuery("match $wr isa warm-up-relation, has warm-up-attribute $wa;");
        // Warm up all persisted data
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                tx.query().match(TypeQL.parseQuery("match $x isa thing;").asMatch()).count();
                tx.query().match(TypeQL.parseQuery("match $r ($x) isa relation;").asMatch()).count();
                tx.query().match(TypeQL.parseQuery("match $x has $a;").asMatch()).count();
            }
        }

        LOG.info("Warmup took: {} ms", (System.nanoTime() - start) / 1_000_000);
    }

    public void runBenchmark(Benchmark benchmark) {
        if (warmupRunForEachQuery) {
            LOG.info("Doing warmup query...");
            Benchmark.BenchmarkRun warmupRun = runMatchQuery(benchmark.query);
            LOG.info("Warmup query took {} ms", warmupRun.timeTaken.toMillis());
        }

        for (int i = 0; i < benchmark.nRuns; i++) {
            Benchmark.BenchmarkRun run = runMatchQuery(benchmark.query);
            benchmark.addRun(run);
            LOG.info("Completed run in {} ms; Summary:\n{}", run.timeTaken.toMillis(), run);
        }
        if (csvBuilder != null) csvBuilder.append(benchmark);
    }

    private Benchmark.BenchmarkRun runMatchQuery(String query) {
        Benchmark.BenchmarkRun run;
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true).reasonerPerfCounters(true))) {
                Instant start = Instant.now();
                long nAnswers = tx.query().match(TypeQL.parseQuery(query).asMatch()).count();
                Duration timeTaken = Duration.between(start, Instant.now());
                run = new Benchmark.BenchmarkRun(nAnswers, timeTaken, ((CoreTransaction) tx).reasoner().controllerRegistry().perfCounters());
            }
        }
        return run;
    }

    static class CSVBuilder {

        static final List<String> perfCounterKeys = list(
                ReasonerPerfCounters.PLANNING_TIME_NS,
                ReasonerPerfCounters.MATERIALISATIONS,
                ReasonerPerfCounters.CONJUNCTION_PROCESSORS,
                ReasonerPerfCounters.COMPOUND_STREAMS,
                ReasonerPerfCounters.COMPOUND_STREAM_MESSAGES_RECEIVED
        );

        private final StringBuilder sb;

        CSVBuilder() {
            sb = new StringBuilder();
            List<String> fields = new ArrayList<>();
            fields.addAll(list("name", "expectedAnswers", "actualAnswers", "total_time_ms"));
            fields.addAll(perfCounterKeys);
            appendLine(fields);
        }

        public void append(Benchmark benchmark) {
            iterate(benchmark.runs).map(run -> {
                List<String> entries = new ArrayList<>();
                entries.add(benchmark.name);
                entries.add(Long.toString(benchmark.expectedAnswers));
                entries.add(Long.toString(run.answerCount));
                entries.add(Long.toString(run.timeTaken.toMillis()));
                perfCounterKeys.forEach(key -> entries.add(Long.toString(run.reasonerPerfCounters.get(key))));
                return entries;
            }).forEachRemaining(this::appendLine);
        }

        private void appendLine(List<String> entries) {
            entries.forEach(entry -> sb.append(entry).append(","));
            sb.append("\n");
        }

        public String build() {
            return sb.toString();
        }
    }
}
