/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.batch;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.batch.BatchExecutorClient;
import ai.grakn.batch.GraknClient;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Role;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.GraknTestUtil;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixRequestLog;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraknTestUtil.usingTinker;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;
import static java.util.stream.Stream.generate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.spy;

public class BatchExecutorClientIT {

    public static final int MAX_DELAY = 100;
    private GraknSession session;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.create();
    private Keyspace keyspace;

    @Before
    public void setupSession() {
        // Because we think tinkerpop is not thread-safe and batch-loading uses multiple threads
        assumeFalse(usingTinker());

        keyspace = randomKeyspace();
        this.session = Grakn.session(keyspace, engine.config());
    }

    @Ignore("This test stops and restart server - this is not supported yet by gRPC [https://github.com/grpc/grpc/issues/7031] - fix when gRPC 1.1 is released")
    @Test
    public void whenSingleQueryLoadedAndServerDown_RequestIsRetried() throws IOException, InterruptedException {
        AtomicInteger numLoaded = new AtomicInteger(0);
        // Create a BatchExecutorClient with a callback that will fail
        try (BatchExecutorClient loader = loader(MAX_DELAY)) {
            loader.onNext(response -> numLoaded.incrementAndGet());

            // Engine goes down
            engine.server().getHttpHandler().stopHTTP();
            // Most likely the first call doesn't find the server but it's retried
            generate(this::query).limit(1).forEach(q -> loader.add(q, keyspace));
            engine.server().getHttpHandler().startHTTP();
        }

        // Verify that the logger received the failed log message
        assertEquals(1, numLoaded.get());
    }

    @Test
    public void whenSingleQueryLoaded_TaskCompletionExecutesExactlyOnce() {
        AtomicInteger numLoaded = new AtomicInteger(0);

        // Create a BatchExecutorClient with a callback that will fail
        try (BatchExecutorClient loader = loader(MAX_DELAY)) {
            loader.onNext(response -> numLoaded.incrementAndGet());

            // Load some queries
            generate(this::query).limit(1).forEach(q ->
                    loader.add(q, keyspace)
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
                    loader.add(q, keyspace)
            );
        }
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            assertEquals(n, graph.getEntityType("name_tag").instances().count());
        }
    }

    @Ignore("This test interferes with other tests using the BEC (they probably use the same HystrixRequestLog)")
    @Test
    public void whenSending100Queries_TheyAreSentInBatch() throws InterruptedException {
        Condition everythingLoaded = new ReentrantLock().newCondition();

        // Increasing the max delay so eveyrthing goes in a single batch
        try (BatchExecutorClient loader = loader(MAX_DELAY * 100)) {
            loader.onNext(queryResponse -> everythingLoaded.signal());

            int n = 100;
            generate(this::query).limit(n).forEach(q ->
                    loader.add(q, keyspace)
            );

            everythingLoaded.await();

            assertEquals(1, HystrixRequestLog.getCurrentRequest().getAllExecutedCommands().size());
            HystrixCommand<?> command = HystrixRequestLog.getCurrentRequest()
                    .getAllExecutedCommands()
                    .toArray(new HystrixCommand<?>[1])[0];
            // assert the command is the one we're expecting
            assertEquals("CommandQueries", command.getCommandKey().name());
            // confirm that it was a COLLAPSED command execution
            assertTrue(command.getExecutionEvents().contains(HystrixEventType.COLLAPSED));
            // and that it was successful
            assertTrue(command.getExecutionEvents().contains(HystrixEventType.SUCCESS));
        }
    }


    @Ignore("Randomly failing test which is slowing down dev. This should be fixed")
    @Test
    public void whenEngineRESTFailsWhileLoadingWithRetryTrue_LoaderRetriesAndWaits()
            throws Exception {

        int n = 20;

        try (BatchExecutorClient loader = loader(MAX_DELAY)) {
            for (int i = 0; i < n; i++) {
                loader.add(query(), keyspace);

                if (i % 5 == 0) {
                    Thread.sleep(200);
                    System.out.println("Restarting engine");
                    engine.server().getHttpHandler().stopHTTP();
                    Thread.sleep(200);
                    engine.server().getHttpHandler().startHTTP();
                }
            }
        }

        if(GraknTestUtil.usingJanus()) {
            try (GraknTx graph = session.transaction(GraknTxType.READ)) {
                assertEquals(n, graph.getEntityType("name_tag").instances().count());
            }
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

            GraknClient graknClient = GraknClient.of(engine.uri());
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
