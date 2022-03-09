/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;

public class StatisticsTest {

    private static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("statistics-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "statistics-test";

    private CoreDatabaseManager databaseMgr;

    @Before
    public void setup() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        try (TypeDB.Session session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                txn.query().define(TypeQL.parseQuery("define " +
                        "person sub entity, owns name, plays friendship:friend, plays employment:employee;" +
                        "friendship sub relation, relates friend;" +
                        "name sub attribute, value string;" +
                        "nickname sub attribute, value string, owns nickname;" +
                        "company sub entity, owns address @key, owns name, plays employment:employer;" +
                        "employment sub relation, relates employer, relates employee;" +
                        "address sub attribute, value string;").asDefine());
                txn.commit();
            }
        }
    }

    @After
    public void tearDown() {
        databaseMgr.close();
    }

    @Test
    public void nonconcurrent_insert_delete() {
        int batches = 10;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            for (int i = 0; i < batches; i++) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.query().insert(TypeQL.parseQuery("insert " +
                            "$x isa person, has name 'name-" + i + "';" +
                            "$clone isa person, has name 'name-" + i + "', has name 'clone-" + i + "';" +
                            "$y isa person, has name 'name-" + (i + batches) + "';" +
                            "(friend: $x, friend: $y) isa friendship;" +
                            "$c isa company, has name 'company-name-" + i + "', has address 'Margaret Street Nr. " + i + "';" +
                            "(employee: $x, employer: $c) isa employment;"));
                    txn.commit();
                }
            }

            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();

                assertEquals(batches * 3, statistics.thingVertexCount(Label.of("person")));
                assertEquals(batches * 4, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(batches * 4, statistics.thingVertexCount(Label.of("name")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("friendship")));
                assertEquals(batches * 2, statistics.thingVertexCount(Label.of("friend", "friendship")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("company")));
                assertEquals(batches, statistics.hasEdgeCount(Label.of("company"), Label.of("name")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("address")));
                assertEquals(batches, statistics.hasEdgeCount(Label.of("company"), Label.of("address")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employment")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employer", "employment")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employee", "employment")));
                assertEquals(batches * 11, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("thing"))));
                assertEquals(batches * 4, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("role", "relation"))));
            }

            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                txn.query().delete(TypeQL.parseQuery("match " +
                        "$x isa person, has name $n; $n 'name-1'; not { $x has name 'clone-1';};" +
                        "$c isa company, has name $a; $a 'company-name-9';" +
                        "$r (employer: $c) isa employment;" +
                        "delete " +
                        "$x isa person; $n isa name;" +
                        "$c has $a;" +
                        "$r (employer: $c);"));
                txn.commit();
            }
            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();

                assertEquals(batches * 3 - 1, statistics.thingVertexCount(Label.of("person")));
                // deleting 'name-1' should remove it from first person and the clone
                assertEquals(batches * 4 - 2, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(batches * 4 - 1, statistics.thingVertexCount(Label.of("name")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("friendship")));
                assertEquals(batches * 2 - 1, statistics.thingVertexCount(Label.of("friend", "friendship")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("company")));
                assertEquals(batches - 1, statistics.hasEdgeCount(Label.of("company"), Label.of("name")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("address")));
                assertEquals(batches, statistics.hasEdgeCount(Label.of("company"), Label.of("address")));
                assertEquals(batches, statistics.thingVertexCount(Label.of("employment")));
                // deleted one role explicitly, and another by deleting a person
                assertEquals(batches - 1, statistics.thingVertexCount(Label.of("employer", "employment")));
                assertEquals(batches - 1, statistics.thingVertexCount(Label.of("employee", "employment")));
                assertEquals(batches * 11 - 2, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("thing"))));
                // deleted one friendship:friend, one employment:employee, one employment:employer
                assertEquals(batches * 4 - 3, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("role", "relation"))));
            }
        }
    }

    @Test
    public void concurrent_attribute_inserts_are_corrected() throws InterruptedException, ExecutionException {
        int batches = 10;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            List<CoreTransaction> transactions = new ArrayList<>();
            for (int i = 0; i < batches; i++) transactions.add(session.transaction(Arguments.Transaction.Type.WRITE));
            for (int i = 0; i < batches; i++) {
                CoreTransaction txn = transactions.get(i);
                txn.query().insert(TypeQL.parseQuery("insert " +
                        "$x isa person, has name 'Alice';"
                ));
                txn.commit();
            }

            databaseMgr.databases.get(database).statisticsCorrector().submitCorrection().get();

            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();
                assertEquals(batches, statistics.thingVertexCount(Label.of("person")));
                assertEquals(batches, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(1, statistics.thingVertexCount(Label.of("name")));
            }
        }
    }

    @Test
    public void concurrent_has_inserts_are_corrected() throws InterruptedException, ExecutionException {
        int batches = 10;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bill';").asInsert());
                txn.commit();
            }
            List<CoreTransaction> transactions = new ArrayList<>();
            for (int i = 0; i < batches; i++) transactions.add(session.transaction(Arguments.Transaction.Type.WRITE));
            for (int i = 0; i < batches; i++) {
                CoreTransaction txn = transactions.get(i);
                txn.query().insert(TypeQL.parseQuery("match " +
                        "$x isa person, has name 'Bill';" +
                        "insert" +
                        "$x has name 'Billy';"
                ));
                txn.commit();
            }

            databaseMgr.databases.get(database).statisticsCorrector().submitCorrection().get();

            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();
                assertEquals(1, statistics.thingVertexCount(Label.of("person")));
                assertEquals(2, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(2, statistics.thingVertexCount(Label.of("name")));
            }
        }
    }

    @Test
    public void concurrent_circular_ownerships_are_corrected() throws InterruptedException, ExecutionException {
        int batches = 10;
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $n 'Johnny' isa nickname, has nickname $n;").asInsert());
                txn.commit();
            }
            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();
                assertEquals(1, statistics.thingVertexCount(Label.of("nickname")));
                assertEquals(1, statistics.hasEdgeCount(Label.of("nickname"), Label.of("nickname")));
            }
            List<CoreTransaction> transactions = new ArrayList<>();
            for (int i = 0; i < batches; i++) transactions.add(session.transaction(Arguments.Transaction.Type.WRITE));
            for (int i = 0; i < batches; i++) {
                CoreTransaction txn = transactions.get(i);
                txn.query().insert(TypeQL.parseQuery("match " +
                        "$a isa nickname;" +
                        "insert" +
                        "$a has $a;"
                ));
                txn.commit();
            }

            databaseMgr.databases.get(database).statisticsCorrector().submitCorrection().get();

            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();
                assertEquals(1, statistics.thingVertexCount(Label.of("nickname")));
                assertEquals(1, statistics.hasEdgeCount(Label.of("nickname"), Label.of("nickname")));
            }
        }
    }

    @Test
    public void insert_and_delete_in_same_transaction() {
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            // starting with no data
            try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                ThingGraph.Statistics statistics = txn.graphMgr.data().stats();

                txn.query().insert(TypeQL.parseQuery("insert " +
                        "$x isa person, has name 'John';" +
                        "$clone isa person, has name 'John', has name 'Clone';" +
                        "$y isa person, has name 'Alice';" +
                        "(friend: $x, friend: $y) isa friendship;" +
                        "$c isa company, has name 'Vaticle', has address 'Margaret Street Nr. 1';" +
                        "(employee: $x, employer: $c) isa employment;"));

                assertEquals(3, statistics.thingVertexCount(Label.of("person")));
                assertEquals(4, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(4, statistics.thingVertexCount(Label.of("name")));
                assertEquals(1, statistics.thingVertexCount(Label.of("friendship")));
                assertEquals(2, statistics.thingVertexCount(Label.of("friend", "friendship")));
                assertEquals(1, statistics.thingVertexCount(Label.of("company")));
                assertEquals(1, statistics.hasEdgeCount(Label.of("company"), Label.of("name")));
                assertEquals(1, statistics.thingVertexCount(Label.of("address")));
                assertEquals(1, statistics.hasEdgeCount(Label.of("company"), Label.of("address")));
                assertEquals(1, statistics.thingVertexCount(Label.of("employment")));
                assertEquals(1, statistics.thingVertexCount(Label.of("employer", "employment")));
                assertEquals(1, statistics.thingVertexCount(Label.of("employee", "employment")));
                assertEquals(11, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("thing"))));
                assertEquals(4, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("role", "relation"))));

                txn.query().delete(TypeQL.parseQuery("match " +
                        "$x isa person, has name $n; $n 'John'; not { $x has name 'Clone';};" +
                        "$clone isa person, has name $n, has name $cn; $cn 'Clone';" +
                        "delete " +
                        "$x isa person; $n isa name; " +
                        "$clone has $cn;"));
                assertEquals(2, statistics.thingVertexCount(Label.of("person")));
                assertEquals(1, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(3, statistics.thingVertexCount(Label.of("name")));
                assertEquals(1, statistics.thingVertexCount(Label.of("friendship")));
                assertEquals(1, statistics.thingVertexCount(Label.of("friend", "friendship")));
                assertEquals(1, statistics.thingVertexCount(Label.of("employment")));
                assertEquals(1, statistics.thingVertexCount(Label.of("employer", "employment")));
                assertEquals(0, statistics.thingVertexCount(Label.of("employee", "employment")));
                assertEquals(9, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("thing"))));
                assertEquals(2, statistics.thingVertexTransitiveCount(txn.graphMgr.schema().getType(Label.of("role", "relation"))));

                txn.query().insert(TypeQL.parseQuery("match " +
                        "$clone isa person; not { $clone has name $n; };" +
                        "insert " +
                        "$clone has name 'Clone';"));
                assertEquals(2, statistics.hasEdgeCount(Label.of("person"), Label.of("name")));
                assertEquals(3, statistics.thingVertexCount(Label.of("name")));
            }
        }
    }

    @Test
    public void reboot_counts_correct() {

        // TODO

    }
}
