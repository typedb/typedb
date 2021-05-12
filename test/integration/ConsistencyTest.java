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


package com.vaticle.typedb.core.test.integration;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
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
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class ConsistencyTest {

    private static final String database = "isolation-test";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);

    private RocksTypeDB typeDB;

    @Before
    public void setup() throws IOException {
        Util.resetDirectory(dataDir);
        typeDB = RocksTypeDB.open(options);
        typeDB.databases().create(database);
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                txn.query().define(TypeQL.parseQuery("define " +
                                                            "person sub entity, owns name, plays friendship:friend;" +
                                                            "friendship sub relation, relates friend;" +
                                                            "name sub attribute, value string;" +
                                                            "company sub entity, owns address @key;" +
                                                            "address sub attribute, value string;").asDefine());
                txn.commit();
            }
        }
    }

    @After
    public void tearDown() {
        typeDB.close();
    }

    @Test
    public void concurrent_write_same_attribute_does_not_conflict() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("insert $x 'Alice' isa name;"));
            txn2.query().insert(TypeQL.parseQuery("insert $x 'Alice' isa name;"));
            txn1.commit();
            txn2.commit();
        }
    }

    @Test
    public void concurrent_write_same_ownership_does_not_conflict() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person; $a 'Alice' isa name;"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("match $x isa person; $a 'Alice' isa name; insert $x has $a;"));
            txn2.query().insert(TypeQL.parseQuery("match $x isa person; $a 'Alice' isa name; insert $x has $a;"));
            txn1.commit();
            txn2.commit();
        }
    }

    @Test
    public void concurrent_delete_same_concept_does_not_conflict() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person; $a 'Alice' isa name;"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $x isa person; $a 'Alice' isa name; delete $x isa person; $a isa name;"));
            txn2.query().delete(TypeQL.parseQuery("match $x isa person; $a 'Alice' isa name; delete $x isa person; $a isa name;"));
            txn1.commit();
            txn2.commit();
        }
    }

    @Test
    public void concurrent_delete_same_ownership_does_not_conflict() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Alice';"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $x isa person, has name $a; delete $x has $a;"));
            txn2.query().delete(TypeQL.parseQuery("match $x isa person, has name $a; delete $x has $a;"));
            txn1.commit();
            txn2.commit();
        }
    }

    @Test
    public void concurrent_delete_same_role_does_not_conflict() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery(
                    "insert $x isa person, has name 'Bob'; $y isa person, has name 'Alice'; (friend: $x, friend: $y) isa friendship;"
            ));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $f (friend: $x) isa friendship; $x isa person, has name 'Bob'; delete $f (friend: $x);"));
            txn2.query().delete(TypeQL.parseQuery("match $f (friend: $x) isa friendship; $x isa person, has name 'Bob'; delete $f (friend: $x);"));
            txn1.commit();
            txn2.commit();
        }
    }

    @Test
    public void concurrent_insert_delete_attribute_conflicts() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob';"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $a 'Bob' isa name; delete $a isa name;"));
            txn2.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();
        }
    }

    @Test
    public void sequential_insert_delete_attribute_does_not_conflict_in_any_order() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob';"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $a 'Bob' isa name; delete $a isa name;"));
            txn1.commit();
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn2.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn2.commit();
        }
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob';"));
            setupTxn.commit();
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn2.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn2.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $a 'Bob' isa name; delete $a isa name;"));
            txn1.commit();
        }
    }

    @Test
    public void insert_delete_concurrent_insert_attribute_conflicts() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn1.query().delete(TypeQL.parseQuery("match $a 'Bob' isa name; delete $a isa name;"));
            txn2.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();
        }
    }

    @Test
    public void delete_insert_concurrent_insert_attribute_does_not_conflict() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob';"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().delete(TypeQL.parseQuery("match $a 'Bob' isa name; delete $a isa name;"));
            txn1.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn2.query().insert(TypeQL.parseQuery("insert $a 'Bob' isa name;"));
            txn1.commit();
            txn2.commit();
        }
    }

    @Test
    public void concurrent_insert_delete_ownership_conflicts() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob';"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("match $x isa person; $a 'Bob' isa name; insert $x has $a;"));
            txn2.query().delete(TypeQL.parseQuery("match $x isa person, has $a; $a 'Bob'; delete $x has $a;"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();
        }
    }

    @Test
    public void concurrent_add_ownership_delete_attribute_conflicts() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person; $a 'Bob' isa name;"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("match $x isa person; $a 'Bob' isa name; insert $x has $a;"));
            txn2.query().delete(TypeQL.parseQuery("match $a 'Bob' isa name; delete $a isa name;"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();

        }
    }

    @Test
    public void concurrent_add_ownership_delete_owner_conflicts() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person; $a 'Bob' isa name;"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("match $x isa person; $a 'Bob' isa name; insert $x has $a;"));
            txn2.query().delete(TypeQL.parseQuery("match $x isa person; delete $x isa person;"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();
        }
    }

    @Test
    public void concurrent_add_relation_delete_player_conflicts() {
        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob'; $y isa person, has name 'Alice';"));
            setupTxn.commit();
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("match $x isa person, has name 'Bob'; $y isa person, has name 'Alice'; " +
                                                         "insert $f (friend: $x, friend: $y) isa friendship;"));
            txn2.query().delete(TypeQL.parseQuery("match $x isa person, has name 'Bob'; delete $x isa person;"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();
        }
    }

    @Test
    public void large_loads_end_with_zero_transaction_isolation_sets() throws ExecutionException, InterruptedException {
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {

            TypeDB.Transaction setupTxn = session.transaction(Arguments.Transaction.Type.WRITE);
            setupTxn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Bob'; $y isa person, has name 'Alice';"));
            setupTxn.commit();

            Random random = new Random(0);

            List<CompletableFuture<Void>> workers = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                workers.add(CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(random.nextInt(200));
                        try (TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                            for (int j = 0; j < 1; j++) {
                                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'alice-" + random.nextInt(100) + "';"));
                            }
                            txn.commit();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }));
            }

            CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).get();
            assertEquals(0, session.database().writesManager().recordedCommittedEvents());

            TypeDB.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE);
            txn.query().insert(TypeQL.parseQuery("match $x isa person, has name 'Bob'; $y isa person, has name 'Alice'; " +
                                                        "insert $f (friend: $x, friend: $y) isa friendship;"));
            txn.commit();
            assertEquals(0, session.database().writesManager().recordedCommittedEvents());
        }
    }

    @Test
    public void concurrent_key_insertion_conflicts() {

        try (TypeDB.Session session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            TypeDB.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
            TypeDB.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
            txn1.query().insert(TypeQL.parseQuery("insert $x isa company, has address 'abc-key-1';"));
            txn2.query().insert(TypeQL.parseQuery("insert $x isa company, has address 'abc-key-1';"));
            txn1.commit();
            try {
                txn2.commit();
            } catch (TypeDBException e) {
                // success
                return;
            } catch (Exception e) {
                fail("Wrong exception type: " + e);
            }
            fail();
        }
    }
}
