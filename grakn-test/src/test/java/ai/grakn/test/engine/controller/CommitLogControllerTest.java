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
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.jayway.restassured.http.ContentType;
import mjson.Json;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import spark.Service;

import java.util.UUID;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.test.GraknTestEnv.ensureCassandraRunning;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_INSTANCE_COUNT;
import static ai.grakn.util.REST.Request.COMMIT_LOG_TYPE_NAME;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.baseURI;
import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static mjson.Json.array;
import static mjson.Json.object;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

//TODO Stopping commit log tasks when clearing graph
public class CommitLogControllerTest {

    private static final String TEST_KEYSPACE = "test";
    private static final int PORT = 4567;

    private static Service spark;
    private static TaskManager manager;
    private Json commitLog;

    @BeforeClass
    public static void setupControllers() throws Exception {
        ensureCassandraRunning();

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
    }

    @After
    public void resetMockitoMockCounts(){
        reset(manager);
    }

    @Test
    public void whenControllerReceivesLog_TaskManagerReceivesPPTask() {
        sendFakeCommitLog();

        verify(manager, times(1)).addLowPriorityTask(
                argThat(argument -> argument.taskClass().equals(PostProcessingTask.class)),
                argThat(argument -> argument.json().at(COMMIT_LOG_FIXING).equals(commitLog.at(COMMIT_LOG_FIXING)))
        );
    }

    @Test
    public void whenCommittingGraph_TaskManagerReceivesPPTask() throws GraknValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        GraknGraph bob = Grakn.session(Grakn.DEFAULT_URI, BOB).open(GraknTxType.WRITE);
        GraknGraph tim = Grakn.session(Grakn.DEFAULT_URI, TIM).open(GraknTxType.WRITE);

        addSomeData(bob);

        verify(manager, times(1)).addLowPriorityTask(
                argThat(argument ->
                        argument.taskClass().equals(PostProcessingTask.class)),
                argThat(argument ->
                        argument.json().at(KEYSPACE).asString().equals(BOB) &&
                        argument.json().at(COMMIT_LOG_FIXING).at(Schema.BaseType.CASTING.name()).asJsonMap().size() == 2 &&
                        argument.json().at(COMMIT_LOG_FIXING).at(Schema.BaseType.RESOURCE.name()).asJsonMap().size() == 1));

        verify(manager, never()).addLowPriorityTask(
                any(), argThat(arg -> arg.json().at(KEYSPACE).asString().equals(TIM)));

        addSomeData(tim);

        verify(manager, times(1)).addLowPriorityTask(
                argThat(argument ->
                        argument.taskClass().equals(PostProcessingTask.class)),
                argThat(argument ->
                        argument.json().at(KEYSPACE).asString().equals(TIM) &&
                        argument.json().at(COMMIT_LOG_FIXING).at(Schema.BaseType.CASTING.name()).asJsonMap().size() == 2 &&
                        argument.json().at(COMMIT_LOG_FIXING).at(Schema.BaseType.RESOURCE.name()).asJsonMap().size() == 1));

        bob.close();
        tim.close();
    }

    @Test
    @Ignore //TODO Add in stopping tasks
    public void whenDeletingViaController_CacheIsCleared() throws InterruptedException {
        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + TEST_KEYSPACE).
                then().statusCode(200).extract().response().andReturn();

        verify(manager, times(1)).stopTask(any());
    }

    @Test
    public void whenSendingCommitLogs_TaskManagerReceivesCountTask(){
        sendFakeCommitLog();

        verify(manager, atLeastOnce()).addHighPriorityTask(
                argThat(argument ->
                                argument.taskClass().equals(UpdatingInstanceCountTask.class)),
                argThat(argument ->
                                argument.json().at(COMMIT_LOG_COUNTING).asJsonList().size() == 5)
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
            verify(manager, atLeastOnce()).addHighPriorityTask(
                    argThat(argument ->
                            argument.taskClass().equals(UpdatingInstanceCountTask.class)),
                    argThat(argument ->
                            argument.json().at(KEYSPACE).asString().equals(BOB) &&
                            argument.json().at(COMMIT_LOG_COUNTING).asJsonList().size() == 3));

            verify(manager, atLeastOnce()).addHighPriorityTask(
                    argThat(argument ->
                            argument.taskClass().equals(UpdatingInstanceCountTask.class)),
                    argThat(argument ->
                            argument.json().at(KEYSPACE).asString().equals(TIM) &&
                            argument.json().at(COMMIT_LOG_COUNTING).asJsonList().size() == 3));
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

        verify(manager, never()).addLowPriorityTask(any(), any());
        verify(manager, never()).addHighPriorityTask(any(), any());
    }

    private void sendFakeCommitLog() {
        Json commitLogFixCasting = object();
        commitLogFixCasting.set("10", array(1));
        commitLogFixCasting.set("20", array(2));
        commitLogFixCasting.set("30", array(3));
        commitLogFixCasting.set("40", array(4));

        Json commitLogFixResource = object();
        commitLogFixResource.set("60", array(6));
        commitLogFixResource.set("70", array(7));

        Json commitLogFixing = object();
        commitLogFixing.set(Schema.BaseType.CASTING.name(), commitLogFixCasting);
        commitLogFixing.set(Schema.BaseType.RESOURCE.name(), commitLogFixResource);

        Json commitLogCounting = array();
        commitLogCounting.add(object(COMMIT_LOG_TYPE_NAME, "Alpha", COMMIT_LOG_INSTANCE_COUNT, -3));
        commitLogCounting.add(object(COMMIT_LOG_TYPE_NAME, "Bravo", COMMIT_LOG_INSTANCE_COUNT, -2));
        commitLogCounting.add(object(COMMIT_LOG_TYPE_NAME, "Delta", COMMIT_LOG_INSTANCE_COUNT, -1));
        commitLogCounting.add(object(COMMIT_LOG_TYPE_NAME, "Charlie", COMMIT_LOG_INSTANCE_COUNT,1));
        commitLogCounting.add(object(COMMIT_LOG_TYPE_NAME, "Foxtrot", COMMIT_LOG_INSTANCE_COUNT, 2));

        commitLog = object(
                COMMIT_LOG_FIXING, commitLogFixing,
                COMMIT_LOG_COUNTING, commitLogCounting
        );

        given().contentType(ContentType.JSON).body(commitLog.toString()).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + TEST_KEYSPACE).
                then().statusCode(200).extract().response().andReturn();
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
}