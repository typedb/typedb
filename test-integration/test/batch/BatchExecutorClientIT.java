/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.test.batch;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.batch.BatchExecutorClient;
import ai.grakn.batch.GraknClient;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Role;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.rule.GraknServer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Stream.generate;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

public class BatchExecutorClientIT {

    @ClassRule
    public static final GraknServer server = new GraknServer();

    public static final int MAX_DELAY = 100;
    private GraknSession session;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setupSession() {
        this.session = server.sessionWithNewKeyspace();
    }

    @Test
    public void whenSingleQueryLoaded_TaskCompletionExecutesExactlyOnce() {
        AtomicInteger numLoaded = new AtomicInteger(0);

        // Create a BatchExecutorClient with a callback that will fail
        try (BatchExecutorClient loader = loader(MAX_DELAY)) {
            loader.onNext(response -> numLoaded.incrementAndGet());

            // Load some queries
            generate(this::query).limit(1).forEach(q ->
                    loader.add(q, session.keyspace())
            );
        }

        // Verify that the logger received the failed log message
        assertEquals(1, numLoaded.get());
    }

    @Test
    public void whenSending100InsertQueries_100EntitiesAreLoadedIntoGraph() {
        int n = 100;

        try (BatchExecutorClient loader = loader(MAX_DELAY)) {
            generate(this::query).limit(100).forEach(q ->
                    loader.add(q, session.keyspace())
            );
        }
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            assertEquals(n, graph.getEntityType("name_tag").instances().count());
        }
    }

    private BatchExecutorClient loader(int maxDelay) {
        // load schema
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            Role role = tx.putRole("some-role");
            tx.putRelationshipType("some-relationship").relates(role);

            EntityType nameTag = tx.putEntityType("name_tag");
            AttributeType<String> nameTagString = tx
                    .putAttributeType("name_tag_string", AttributeType.DataType.STRING);
            AttributeType<String> nameTagId = tx
                    .putAttributeType("name_tag_id", AttributeType.DataType.STRING);

            nameTag.has(nameTagString);
            nameTag.has(nameTagId);
            tx.commit();

            GraknClient graknClient = GraknClient.of(server.uri());
            return spy(
                    BatchExecutorClient.newBuilder().taskClient(graknClient).maxDelay(maxDelay).requestLogEnabled(true)
                            .build());
        }
    }

    private InsertQuery query() {
        return Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
    }
}