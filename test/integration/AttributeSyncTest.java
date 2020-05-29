/*
 * Copyright (C) 2020 Grakn Labs
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

package hypergraph.test.integration;

import hypergraph.Hypergraph;
import hypergraph.concept.thing.Attribute;
import hypergraph.concept.type.AttributeType;
import hypergraph.core.CoreHypergraph;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import static hypergraph.test.integration.Util.assertNotNulls;
import static hypergraph.test.integration.Util.resetDirectory;
import static org.junit.Assert.assertEquals;

public class AttributeSyncTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("attribute-sync-test");
    private static String keyspace = "attribute-sync-test";

    @Before
    public void reset_directory_and_create_attribute_types() throws IOException {
        resetDirectory(directory);

        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            graph.keyspaces().create(keyspace);
            try (Hypergraph.Session session = graph.session(keyspace)) {
                try (Hypergraph.Transaction txn = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    txn.concepts().putAttributeType("is-alive", Boolean.class);
                    txn.concepts().putAttributeType("age", Long.class);
                    txn.concepts().putAttributeType("score", Double.class);
                    txn.concepts().putAttributeType("name", String.class);
                    txn.concepts().putAttributeType("birth-date", LocalDateTime.class);

                    txn.commit();
                }
            }
        }
    }

    private AttributeType.Boolean isAlive(Hypergraph.Transaction txn) {
        return txn.concepts().getAttributeType("is-alive").asBoolean();
    }

    private AttributeType.Long age(Hypergraph.Transaction txn) {
        return txn.concepts().getAttributeType("age").asLong();
    }

    private AttributeType.Double score(Hypergraph.Transaction txn) {
        return txn.concepts().getAttributeType("score").asDouble();
    }

    private AttributeType.String name(Hypergraph.Transaction txn) {
        return txn.concepts().getAttributeType("name").asString();
    }

    private AttributeType.DateTime dob(Hypergraph.Transaction txn) {
        return txn.concepts().getAttributeType("birth-date").asDateTime();
    }

    @Test
    public void write_attributes_successfully_repeatedly() {
        for (int i = 0; i < 1000; i++) {
            write_attributes_successfully();
        }
    }

    @Test
    public void write_attributes_successfully() {
        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            try (Hypergraph.Session session = graph.session(keyspace)) {
                try (Hypergraph.Transaction txn = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    isAlive(txn).put(true);
                    age(txn).put(18);
                    score(txn).put(90.5);
                    name(txn).put("alice");
                    dob(txn).put(LocalDateTime.of(1991, 1, 1, 0, 0));

                    txn.commit();
                }

                try (Hypergraph.Transaction txn = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    LocalDateTime dateTime = LocalDateTime.of(1991, 1, 1, 0, 0);

                    Attribute.Boolean isAlive = isAlive(txn).get(true);
                    Attribute.Long age = age(txn).get(18);
                    Attribute.Double score = score(txn).get(90.5);
                    Attribute.String name = name(txn).get("alice");
                    Attribute.DateTime dob = dob(txn).get(dateTime);

                    assertNotNulls(isAlive, age, score, name, dob);
                    assertEquals(true, isAlive.value());
                    assertEquals(18, age.value().longValue());
                    assertEquals(90.5, score.value(), 0.001);
                    assertEquals("alice", name.value());
                    assertEquals(dateTime, dob.value());
                }
            }
        }
    }


    @Test
    public void write_different_attributes_in_parallel_successfully_repeatedly() {
        for (int i = 0; i < 1000; i++) {
            write_different_attributes_in_parallel_successfully();
        }
    }

    @Test
    public void write_different_attributes_in_parallel_successfully() {
        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            try (Hypergraph.Session session = graph.session(keyspace)) {
                Hypergraph.Transaction txn1 = session.transaction(Hypergraph.Transaction.Type.WRITE);
                Hypergraph.Transaction txn2 = session.transaction(Hypergraph.Transaction.Type.WRITE);
                Hypergraph.Transaction txn3 = session.transaction(Hypergraph.Transaction.Type.WRITE);

                isAlive(txn1).put(true);
                isAlive(txn2).put(false);
                age(txn1).put(17);
                age(txn2).put(18);
                age(txn3).put(19);
                score(txn1).put(70.5);
                score(txn2).put(80.6);
                score(txn3).put(90.7);
                name(txn1).put("alice");
                name(txn2).put("bob");
                name(txn3).put("charlie");
                dob(txn1).put(LocalDateTime.of(1991, 2, 3, 4, 5));
                dob(txn2).put(LocalDateTime.of(1992, 3, 4, 5, 6));
                dob(txn3).put(LocalDateTime.of(1993, 4, 5, 6, 7));

                txn1.commit();
                txn2.commit();
                txn3.commit();

                try (Hypergraph.Transaction txn = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    LocalDateTime d1 = LocalDateTime.of(1991, 2, 3, 4, 5);
                    LocalDateTime d2 = LocalDateTime.of(1992, 3, 4, 5, 6);
                    LocalDateTime d3 = LocalDateTime.of(1993, 4, 5, 6, 7);

                    assertEquals(true, isAlive(txn).get(true).value());
                    assertEquals(false, isAlive(txn).get(false).value());
                    assertEquals(17, age(txn).get(17).value().longValue());
                    assertEquals(18, age(txn).get(18).value().longValue());
                    assertEquals(19, age(txn).get(19).value().longValue());
                    assertEquals(70.5, score(txn).get(70.5).value(), 0.001);
                    assertEquals(80.6, score(txn).get(80.6).value(), 0.001);
                    assertEquals(90.7, score(txn).get(90.7).value(), 0.001);
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals("bob", name(txn).get("bob").value());
                    assertEquals("charlie", name(txn).get("charlie").value());
                    assertEquals(d1, dob(txn).get(d1).value());
                    assertEquals(d2, dob(txn).get(d2).value());
                    assertEquals(d3, dob(txn).get(d3).value());
                }
            }
        }
    }

    @Test
    public void write_identical_attributes_in_parallel_successfully_repeatedly() {
        for (int i = 0; i < 1000; i++) {
            System.out.println("Repetition: " + i);
            write_identical_attributes_in_parallel_successfully();
        }
    }

    @Test
    public void write_identical_attributes_in_parallel_successfully() {
        LocalDateTime date_1992_2_3_4_5 = LocalDateTime.of(1991, 2, 3, 4, 5);

        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            try (Hypergraph.Session session = graph.session(keyspace)) {
                Hypergraph.Transaction txn1 = session.transaction(Hypergraph.Transaction.Type.WRITE);
                Hypergraph.Transaction txn2 = session.transaction(Hypergraph.Transaction.Type.WRITE);
                Hypergraph.Transaction txn3 = session.transaction(Hypergraph.Transaction.Type.WRITE);

                isAlive(txn1).put(true);
                isAlive(txn2).put(true);
                isAlive(txn3).put(true);
                age(txn1).put(17);
                age(txn2).put(17);
                age(txn3).put(17);
                score(txn1).put(70.5);
                score(txn2).put(70.5);
                score(txn3).put(70.5);
                name(txn1).put("alice");
                name(txn2).put("alice");
                name(txn3).put("alice");
                dob(txn1).put(date_1992_2_3_4_5);
                dob(txn2).put(date_1992_2_3_4_5);
                dob(txn3).put(date_1992_2_3_4_5);

                txn1.commit();
                txn2.commit();
                txn3.commit();

                try (Hypergraph.Transaction txn = session.transaction(Hypergraph.Transaction.Type.READ)) {

                    assertEquals(true, isAlive(txn).get(true).value());
                    assertEquals(17, age(txn).get(17).value().longValue());
                    assertEquals(70.5, score(txn).get(70.5).value(), 0.001);
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals(date_1992_2_3_4_5, dob(txn).get(date_1992_2_3_4_5).value());
                }
            }
        }
    }
}
