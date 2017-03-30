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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.GraphContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import spark.Service;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static java.util.stream.Stream.generate;
import static org.mockito.Mockito.*;

public class LoaderClientTest {

    private static final int PORT = 4567;
    private static TaskManager manager = new StandaloneTaskManager(EngineID.me());
    private static Service spark;

    @Rule
    public final GraphContext graphContext = GraphContext.empty();

    @BeforeClass
    public static void setupSpark(){
        spark = Service.ignite();
        configureSpark(spark, PORT);

        new TasksController(spark, manager);

        spark.awaitInitialization();
    }

    //TODO put this method into a base class for all controller tests
    @AfterClass
    public static void closeSpark() throws Exception {
        stopSpark();
        manager.close();
    }

    private static void stopSpark(){
        spark.stop();

        // Block until server is truly stopped
        // This occurs when there is no longer a port assigned to the Spark server
        boolean running = true;
        while (running) {
            try {
                spark.port();
            } catch(IllegalStateException e){
                running = false;
            }
        }
    }

    @Test
    public void whenSending50InsertQueries_50EntitiesAreLoadedIntoGraph() {
        LoaderClient loader = loader();

        generate(this::query).limit(100).forEach(loader::add);
        loader.waitToFinish();

        Collection<Entity> nameTags = graphContext.graph().getEntityType("name_tag").instances();
        assertEquals(100, nameTags.size());
    }

    @Test
    public void whenSending100QueriesWithBatchSize20_EachBatchHas20Queries() {
        LoaderClient loader = loader();

        loader.setBatchSize(20);
        generate(this::query).limit(100).forEach(loader::add);
        loader.waitToFinish();

        verify(loader, times(5)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 20));
    }

    @Test
    public void whenSending90QueriesWithBatchSize20_TheLastBatchHas10Queries(){
        LoaderClient loader = loader();
        loader.setBatchSize(20);

        generate(this::query).limit(90).forEach(loader::add);

        loader.waitToFinish();

        verify(loader, times(4)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 20));
        verify(loader, times(1)).sendQueriesToLoader(argThat(insertQueries -> insertQueries.size() == 10));
    }

    @Test
    public void whenSending20QueriesWith1ActiveTask_OnlyOneBatchIsActiveAtOnce() throws Exception {
        LoaderClient loader = loader();
        loader.setNumberActiveTasks(1);
        loader.setBatchSize(5);

        generate(this::query).limit(20).forEach(loader::add);

        loader.waitToFinish();

        Collection<Entity> nameTags = graphContext.graph().getEntityType("name_tag").instances();
        assertEquals(20, nameTags.size());
    }

    @Test
    public void whenEngineRESTFailsWhileLoadingWithRetryTrue_LoaderRetriesAndWaits() throws Exception {
        AtomicInteger tasksCompletedWithoutError = new AtomicInteger(0);

        LoaderClient loader = loader();
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
                stopSpark();
                setupSpark();
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

        LoaderClient loader = loader();
        loader.setRetryPolicy(false);
        loader.setBatchSize(5);
        loader.setTaskCompletionConsumer((json) -> {
            if (json != null) {
                tasksCompletedWithoutError.incrementAndGet();
            } else {
                tasksCompletedWithError.incrementAndGet();
            }
        });


        for(int i = 0; i < 20; i++){
            loader.add(query());

            if(i%10 == 0) {
                stopSpark();
                setupSpark();
            }
        }

        loader.waitToFinish();

        assertThat(tasksCompletedWithoutError.get(), lessThanOrEqualTo(4));
        assertThat(tasksCompletedWithoutError.get() + tasksCompletedWithError.get(), equalTo(4));
    }

    private LoaderClient loader(){
        // load ontology
        try(GraknGraph graph = graphContext.graph()){
            EntityType nameTag = graph.putEntityType("name_tag");
            ResourceType<String> nameTagString = graph.putResourceType("name_tag_string", ResourceType.DataType.STRING);
            ResourceType<String> nameTagId = graph.putResourceType("name_tag_id", ResourceType.DataType.STRING);

            nameTag.resource(nameTagString);
            nameTag.resource(nameTagId);
            graph.commit();
        }

        return spy(new LoaderClient(graphContext.graph().getKeyspace(), Grakn.DEFAULT_URI));
    }

    private InsertQuery query(){
        return Graql.insert(
                var().isa("name_tag")
                        .has("name_tag_string", UUID.randomUUID().toString())
                        .has("name_tag_id", UUID.randomUUID().toString()));
    }
}
