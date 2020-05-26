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

public class AttributeSyncTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("attribute-sync-test");
    private static String keyspace = "attribute-sync-test";

    @Before
    public void reset_directory_and_create_attribute_types() throws IOException {
        resetDirectory(directory);

        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            graph.keyspaces().create(keyspace);
            try (Hypergraph.Session session = graph.session(keyspace)) {
                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    transaction.concepts().putAttributeType("is-alive", Boolean.class);
                    transaction.concepts().putAttributeType("age", Long.class);
                    transaction.concepts().putAttributeType("score", Double.class);
                    transaction.concepts().putAttributeType("name", String.class);
                    transaction.concepts().putAttributeType("birth-date", LocalDateTime.class);

                    transaction.commit();
                }
            }
        }
    }

    @Test
    public void write_attributes_successfully() {
        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            try (Hypergraph.Session session = graph.session(keyspace)) {
                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    AttributeType.Boolean isAlive = transaction.concepts().getAttributeType("is-alive").asBoolean();
                    AttributeType.Long age = transaction.concepts().getAttributeType("age").asLong();
                    AttributeType.Double score = transaction.concepts().getAttributeType("score").asDouble();
                    AttributeType.String name = transaction.concepts().getAttributeType("name").asString();
                    AttributeType.DateTime dob = transaction.concepts().getAttributeType("birth-date").asDateTime();

                    isAlive.put(true);
                    age.put(18);
                    score.put(90.5);
                    name.put("alice");
                    dob.put(LocalDateTime.of(1991, 1, 1, 0, 0));

                    transaction.commit();
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    Attribute.Boolean isAlive = transaction.concepts().getAttributeType("is-alive").asBoolean().get(true);
                    Attribute.Long age = transaction.concepts().getAttributeType("age").asLong().get(18);
                    Attribute.Double score = transaction.concepts().getAttributeType("score").asDouble().get(90.5);
                    Attribute.String name = transaction.concepts().getAttributeType("name").asString().get("alice");
                    Attribute.DateTime dob = transaction.concepts().getAttributeType("birth-date").asDateTime().get(LocalDateTime.of(1991, 1, 1, 0, 0));

                    assertNotNulls(isAlive, age, score, name, dob);
                }
            }
        }
    }

    @Test
    public void write_different_attributes_in_parallel_successfully() {

    }

    @Test
    public void write_identical_attributes_in_parallel_successfully() {

    }
}
