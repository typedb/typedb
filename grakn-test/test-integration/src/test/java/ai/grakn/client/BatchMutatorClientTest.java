/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.client;

import ai.grakn.GraknTx;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;
import static java.util.stream.Stream.generate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BatchMutatorClientTest {

    private GraknSession session;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setupSession(){
        this.session = engine.factoryWithNewKeyspace();
    }

    @Test
    public void whenSingleQueryLoaded_TaskCompletionExecutesExactlyOnce(){
        AtomicInteger tasksCompleted = new AtomicInteger(0);

        // Create a BatchMutatorClient with a callback that will fail
        BatchMutatorClient loader = loader();
        loader.setTaskCompletionConsumer((json) -> tasksCompleted.incrementAndGet());

        // Load some queries
        generate(this::query).limit(1).forEach(loader::add);

        // Wait for queries to finish
        loader.waitToFinish();

        // Verify that the logger received the failed log message
        assertEquals(1, tasksCompleted.get());
    }

    @Test
    public void whenSending50InsertQueries_50EntitiesAreLoadedIntoGraph() {
        BatchMutatorClient loader = loader();

        generate(this::query).limit(100).forEach(loader::add);
        loader.waitToFinish();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            assertEquals(100, graph.getEntityType("name_tag").instances().count());
        }
    }

    @Test
    public void whenSending100QueriesWithBatchSize20_EachBatchHas20Queries() {
        BatchMutatorClient loader = loader();

        loader.setBatchSize(20);
        generate(this::query).limit(100).forEach(loader::add);
        loader.waitToFinish();

        verify(loader, times(5)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 20));
    }

    @Test
    public void whenSending90QueriesWithBatchSize20_TheLastBatchHas10Queries(){
        BatchMutatorClient loader = loader();
        loader.setBatchSize(20);

        generate(this::query).limit(90).forEach(loader::add);

        loader.waitToFinish();

        verify(loader, times(4)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 20));
        verify(loader, times(1)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 10));
    }

    @Test
    public void whenSending20QueriesWith1ActiveTask_OnlyOneBatchIsActiveAtOnce() throws Exception {
        BatchMutatorClient loader = loader();
        loader.setNumberActiveTasks(1);
        loader.setBatchSize(5);

        generate(this::query).limit(20).forEach(loader::add);

        loader.waitToFinish();

        try (GraknTx graph = session.open(GraknTxType.READ)) {
            assertEquals(20, graph.getEntityType("name_tag").instances().count());
        }
    }

    @Test
    public void whenEngineRESTFailsWhileLoadingWithRetryTrue_LoaderRetriesAndWaits() throws Exception {
        AtomicInteger tasksCompletedWithoutError = new AtomicInteger(0);

        BatchMutatorClient loader = loader();
        loader.setRetryPolicy(true);
        loader.setBatchSize(5);
        loader.setTaskCompletionConsumer((json) -> {
            if(json != null){
                tasksCompletedWithoutError.incrementAndGet();
            }
        });

        for(int i = 0; i < 20; i++){
            loader.add(query());

            if(i%10 == 0) {
                engine.server().stopHTTP();
                engine.server().startHTTP();
            }
        }

        loader.waitToFinish();

        assertEquals(4, tasksCompletedWithoutError.get());
    }

    // TODO: Run this test in a more deterministic way (mocking endpoints?)
    @Test
    public void whenEngineRESTFailsWhileLoadingWithRetryFalse_LoaderDoesNotWait() throws Exception {
        AtomicInteger tasksCompletedWithoutError = new AtomicInteger(0);
        AtomicInteger tasksCompletedWithError = new AtomicInteger(0);

        BatchMutatorClient loader = loader();
        loader.setRetryPolicy(false);
        int batchSize = 5;
        loader.setBatchSize(batchSize);
        loader.setTaskCompletionConsumer((json) -> {
            if (json != null) {
                tasksCompletedWithoutError.incrementAndGet();
            } else {
                tasksCompletedWithError.incrementAndGet();
            }
        });

        int queries = 20;
        for(int i = 0; i < queries; i++){
            try {
                loader.add(query());
            } catch(RuntimeException e) {
                // Swallowing exceptions, there's a count of successful tasks anyway
            }

            if(i%10 == 0) {
                engine.server().stopHTTP();
                engine.server().startHTTP();
            }
        }

        loader.waitToFinish();

        int expectedTotal = queries/batchSize;
        assertThat(tasksCompletedWithoutError.get(), lessThanOrEqualTo(expectedTotal));
        assertThat(tasksCompletedWithoutError.get() + tasksCompletedWithError.get(), equalTo(expectedTotal));
    }

    @Test
    public void whenAddingReadOnlyQueriesThrowError() {
        BatchMutatorClient loader = loader();
        MatchQuery matchQuery = match(var("x").isa("y"));
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(READ_ONLY_QUERY.getMessage(matchQuery.toString()));
        loader.add(matchQuery);
    }

    @Test
    public void whenInsertingIdenticalQueriesMakeSureTheyAreAllSuccessful() {
        BatchMutatorClient mutatorClient = loader();
        InsertQuery insertQuery = insert(var("x").isa("person"));
        mutatorClient.add(insertQuery);
        mutatorClient.add(insertQuery);
        mutatorClient.waitToFinish();
        verify(mutatorClient, times(1)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 2));
    }

    private BatchMutatorClient loader(){
        // load ontology
        try(GraknTx graph = session.open(GraknTxType.WRITE)){
            EntityType nameTag = graph.putEntityType("name_tag");
            ResourceType<String> nameTagString = graph.putResourceType("name_tag_string", ResourceType.DataType.STRING);
            ResourceType<String> nameTagId = graph.putResourceType("name_tag_id", ResourceType.DataType.STRING);

            nameTag.resource(nameTagString);
            nameTag.resource(nameTagId);
            graph.admin().commitNoLogs();

            return spy(new BatchMutatorClient(graph.getKeyspace(), engine.uri()));
        }
    }

    private InsertQuery query(){
        return Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
    }
}
