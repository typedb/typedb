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
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.migrator.data.DataImporter;
import com.vaticle.typedb.core.server.Version;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;

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
import java.util.Map;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.fail;

public class BenchmarkRunner {

    private static final String RESOURCE_DIRECTORY = "test/benchmark/iam/resources/";
    private static CoreDatabaseManager databaseMgr;
    private final String database;
    private final boolean collectResults;
    private final List<Benchmark> results;

    BenchmarkRunner(String database, boolean collectResults) {
        this.database = database;
        this.collectResults = collectResults;
        this.results = new ArrayList<>();
    }

    void setUp() throws IOException {
        Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("iam-benchmark-conjunctions");
        if (Files.exists(dataDir)) {
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        Files.createDirectory(dataDir);

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

    void loadData(String filename) {
        new DataImporter(databaseMgr, database, Paths.get(RESOURCE_DIRECTORY + filename), Version.VERSION).run();
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

    public static class BenchmarkRun {
        final long answerCount;
        final Duration timeTaken;
        final Map<String, Long> reasonerPerfCounters;

        public BenchmarkRun(long answerCount, Duration timeTaken, Map<String, Long> reasonerPerfCounters) {
            this.answerCount = answerCount;
            this.timeTaken = timeTaken;
            this.reasonerPerfCounters = reasonerPerfCounters;
        }

        public JsonObject toJSON() {
            JsonObject json;
            json = Json.object()
                    .add("answer_count", Json.value(answerCount))
                    .add("time_taken_ms", Json.value(timeTaken.toMillis()));
            reasonerPerfCounters.keySet().stream().sorted(String::compareTo)
                    .forEach(name -> json.add(name, Json.value(reasonerPerfCounters.get(name))));

            return json;
        }

        @Override
        public String toString() {
            return "Benchmark run:\n" +
                    "\tTimeTaken :\t" + timeTaken.toMillis() + " ms\n" +
                    "\tAnswers   :\t" + answerCount + "\n" +
                    PerfCounters.prettyPrint(reasonerPerfCounters);
        }
    }
}
