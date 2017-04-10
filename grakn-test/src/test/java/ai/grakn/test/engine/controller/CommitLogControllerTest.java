/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.controller;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.cache.EngineCacheStandAlone;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import spark.Service;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.test.GraknTestEnv.ensureCassandraRunning;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.baseURI;
import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CommitLogControllerTest {
    private final ConceptCache cache = EngineCacheProvider.getCache();

    private static final String TEST_KEYSPACE = "test";
    private static final int PORT = 4567;

    private static Service spark;
    private static TaskManager manager;

    @BeforeClass
    public static void setupControllers() throws Exception {
        ensureCassandraRunning();

        EngineCacheProvider.init(EngineCacheStandAlone.getCache());

        baseURI = "http://localhost:" + PORT;
        spark = Service.ignite();
        configureSpark(spark, PORT);

        manager = mock(TaskManager.class);

        new CommitLogController(spark, manager);
        new SystemController(spark);

        spark.awaitInitialization();
    }

    @AfterClass
    public static void stopSpark() throws Exception {
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

        manager.close();
        EngineCacheProvider.clearCache();
    }

    @Before
    public void sendFakeCommitLog() throws Exception {
        String commitLog = "{\n" +
                "    \"" + REST.Request.COMMIT_LOG_FIXING + "\":[\n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"10\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"1\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"20\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"2\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"30\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"3\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"40\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"4\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"50\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"5\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"60\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"6\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.RESOURCE + "\"},\n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"70\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"7\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.RESOURCE + "\"},\n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"80\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"8\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"90\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"9\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"" + REST.Request.COMMIT_LOG_INDEX + "\":\"100\", \"" + REST.Request.COMMIT_LOG_ID + "\":\"10\", \"" + REST.Request.COMMIT_LOG_TYPE + "\":\"" + Schema.BaseType.RELATION + "\"}\n" +
                "    ],\n" +
                "    \"" + REST.Request.COMMIT_LOG_COUNTING + "\":[\n" +
                "        {\"" + REST.Request.COMMIT_LOG_TYPE_NAME + "\":\"Alpha\", \"" + REST.Request.COMMIT_LOG_INSTANCE_COUNT + "\":\"-3\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_TYPE_NAME + "\":\"Bravo\", \"" + REST.Request.COMMIT_LOG_INSTANCE_COUNT + "\":\"-2\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_TYPE_NAME + "\":\"Charlie\", \"" + REST.Request.COMMIT_LOG_INSTANCE_COUNT + "\":\"-1\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_TYPE_NAME + "\":\"Delta\", \"" + REST.Request.COMMIT_LOG_INSTANCE_COUNT + "\":\"1\"}, \n" +
                "        {\"" + REST.Request.COMMIT_LOG_TYPE_NAME + "\":\"Foxtrot\", \"" + REST.Request.COMMIT_LOG_INSTANCE_COUNT + "\":\"2\"} \n" +
                "    ]\n" +
                "}";

        given().contentType(ContentType.JSON).body(commitLog).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + TEST_KEYSPACE).
                then().statusCode(200).extract().response().andReturn();
    }

    @After
    public void clearCache() throws InterruptedException {
        cache.getCastingJobs(TEST_KEYSPACE).clear();
    }

    @Test
    public void whenClearingGraph_CommitLogClearsCache(){
        GraknGraph test = Grakn.session(Grakn.DEFAULT_URI, TEST_KEYSPACE).open(GraknTxType.WRITE);
        test.admin().clear(EngineCacheProvider.getCache());
        assertEquals(0, cache.getCastingJobs(TEST_KEYSPACE).size());
        assertEquals(0, cache.getResourceJobs(TEST_KEYSPACE).size());
    }

    @Test
    public void whenControllerReceivesLog_CacheIsUpdated() {
        assertEquals(4, cache.getCastingJobs(TEST_KEYSPACE).size());
        assertEquals(2, cache.getResourceJobs(TEST_KEYSPACE).size());
    }

    @Test
    public void whenCommittingGraph_CommitLogIsSent() throws GraknValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        GraknGraph bob = Grakn.session(Grakn.DEFAULT_URI, BOB).open(GraknTxType.WRITE);
        GraknGraph tim = Grakn.session(Grakn.DEFAULT_URI, TIM).open(GraknTxType.WRITE);

        addSomeData(bob);

        assertEquals(2, cache.getCastingJobs(BOB).size());
        assertEquals(1, cache.getResourceJobs(BOB).size());

        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs(TIM).size());
        assertEquals(1, cache.getResourceJobs(TIM).size());

        Grakn.session(Grakn.DEFAULT_URI, BOB).open(GraknTxType.WRITE).clear();
        Grakn.session(Grakn.DEFAULT_URI, TIM).open(GraknTxType.WRITE).clear();

        assertEquals(0, cache.getCastingJobs(BOB).size());
        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(BOB).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        bob.close();
        tim.close();
    }

    private void addSomeData(GraknGraph graph) throws GraknValidationException {
        RoleType role1 = graph.putRoleType("Role 1");
        RoleType role2 = graph.putRoleType("Role 2");
        RelationType relationType = graph.putRelationType("A Relation Type").relates(role1).relates(role2);
        EntityType type = graph.putEntityType("A Thing").plays(role1).plays(role2);
        ResourceType<String> resourceType = graph.putResourceType("A Resource Type Thing", ResourceType.DataType.STRING).plays(role1).plays(role2);
        Entity entity = type.addEntity();
        Resource resource = resourceType.putResource(UUID.randomUUID().toString());

        relationType.addRelation().addRolePlayer(role1, entity).addRolePlayer(role2, resource);

        graph.commit();
    }

    @Test
    public void whenDeletingViaController_CacheIsCleared() throws InterruptedException {
        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + TEST_KEYSPACE).
                then().statusCode(200).extract().response().andReturn();

        waitForCache(TEST_KEYSPACE, 0);

        assertEquals(0, cache.getCastingJobs(TEST_KEYSPACE).size());
        assertEquals(0, cache.getResourceJobs(TEST_KEYSPACE).size());
    }

    private void waitForCache(String keyspace, int value) throws InterruptedException {
        boolean flag = true;
        while(flag){
            if(cache.getCastingJobs(keyspace).size() != value){
                Thread.sleep(1000);
            } else{
                flag = false;
            }
        }
    }

    @Test
    public void whenSendingCommitLogs_TaskManagerReceivesCountTask(){
        verify(manager, atLeastOnce()).addHighPriorityTask(
                argThat(argument ->
                        argument.taskClass().equals(UpdatingInstanceCountTask.class)
                        && argument.configuration().at(COMMIT_LOG_COUNTING).asJsonList().size() == 5)
        );
    }

    @Test
    public void whenCommittingGraph_TaskManagerReceivesCountTask(){
        final String BOB = "bob";
        final String TIM = "tim";

        GraknGraph bob = Grakn.session(Grakn.DEFAULT_URI, BOB).open(GraknTxType.WRITE);
        GraknGraph tim = Grakn.session(Grakn.DEFAULT_URI, TIM).open(GraknTxType.WRITE);

        addSomeData(bob);
        addSomeData(tim);

        try {
            verify(manager, atLeastOnce()).addHighPriorityTask(argThat(argument ->
                    argument.configuration().at(KEYSPACE).asString().equals(BOB) &&
                            argument.configuration().at(COMMIT_LOG_COUNTING).asJsonList().size() == 3));

            verify(manager, atLeastOnce()).addHighPriorityTask(argThat(argument ->
                    argument.configuration().at(KEYSPACE).asString().equals(TIM) &&
                            argument.configuration().at(COMMIT_LOG_COUNTING).asJsonList().size() == 3));
        } finally {
            Grakn.session(Grakn.DEFAULT_URI, BOB).open(GraknTxType.WRITE).clear();
            Grakn.session(Grakn.DEFAULT_URI, TIM).open(GraknTxType.WRITE).clear();

            bob.close();
            tim.close();
        }
    }

    @Test
    public void whenCommittingSystemGraph_CommitLogsNotSent() throws GraknValidationException {
        GraknGraph graph1 = Grakn.session(Grakn.DEFAULT_URI, SystemKeyspace.SYSTEM_GRAPH_NAME).open(GraknTxType.WRITE);
        ResourceType<String> resourceType = graph1.putResourceType("New Resource Type", ResourceType.DataType.STRING);
        resourceType.putResource("a");
        resourceType.putResource("b");
        resourceType.putResource("c");
        graph1.commit();

        assertEquals(0, cache.getResourceJobs(SystemKeyspace.SYSTEM_GRAPH_NAME).size());
    }
}