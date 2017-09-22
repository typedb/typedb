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

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.EngineContext;
import static ai.grakn.util.ErrorMessage.READ_ONLY_QUERY;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.stream.Stream.generate;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BatchMutatorClientTest {

    private GraknSession session;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.inMemoryServer();

    @Before
    public void setupSession(){
        this.session = engine.sessionWithNewKeyspace();
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
        loader.close();

        // Verify that the logger received the failed log message
        assertEquals(1, tasksCompleted.get());
    }

    @Test
    public void whenSending50InsertQueries_50EntitiesAreLoadedIntoGraph() {
        BatchMutatorClient loader = loader();

        generate(this::query).limit(100).forEach(loader::add);
        loader.waitToFinish();
        loader.close();

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
        loader.close();

        verify(loader, times(5)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 20));
    }

    @Test
    public void whenSending90QueriesWithBatchSize20_TheLastBatchHas10Queries(){
        BatchMutatorClient loader = loader();
        loader.setBatchSize(20);

        generate(this::query).limit(90).forEach(loader::add);

        loader.waitToFinish();
        loader.close();

        verify(loader, times(4)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 20));
        verify(loader, times(1)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 10));
    }

    @Test
    public void whenEngineRESTFailsWhileLoadingWithRetryTrue_LoaderRetriesAndWaits() throws Exception {
        AtomicInteger tasksCompletedWithoutError = new AtomicInteger(0);

        BatchMutatorClient loader = loader();
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
        loader.close();
        assertEquals(4, tasksCompletedWithoutError.get());
    }

    @Test
    public void whenAddingReadOnlyQueriesThrowError() {
        BatchMutatorClient loader = loader();
        GetQuery getQuery = match(var("x").isa("y")).get();
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(READ_ONLY_QUERY.getMessage(getQuery.toString()));
        loader.add(getQuery);
    }

    @Test
    @Ignore("Fails because the graph is not initialised with person")
    public void whenInsertingIdenticalQueriesMakeSureTheyAreAllSuccessful() {
        BatchMutatorClient mutatorClient = loader();
        InsertQuery insertQuery = insert(var("x").isa("person"));
        mutatorClient.add(insertQuery);
        mutatorClient.add(insertQuery);
        mutatorClient.waitToFinish();
        mutatorClient.close();
        verify(mutatorClient, times(1)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 2));
    }

    private BatchMutatorClient loader(){
        // load schema
        try(GraknTx graph = session.open(GraknTxType.WRITE)){
            EntityType nameTag = graph.putEntityType("name_tag");
            AttributeType<String> nameTagString = graph.putAttributeType("name_tag_string", AttributeType.DataType.STRING);
            AttributeType<String> nameTagId = graph.putAttributeType("name_tag_id", AttributeType.DataType.STRING);

            nameTag.attribute(nameTagString);
            nameTag.attribute(nameTagId);
            graph.admin().commitNoLogs();

            return spy(new BatchMutatorClient(graph.getKeyspace(), engine.uri(), true, 5));
        }
    }

    private InsertQuery query(){
        return Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
    }
}
