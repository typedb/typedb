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
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.test.SparkContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.jayway.restassured.http.ContentType;
import mjson.Json;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static ai.grakn.util.REST.Request.COMMIT_LOG_CONCEPT_ID;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_SHARDING_COUNT;
import static ai.grakn.util.REST.Request.KEYSPACE;
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

public class CommitLogControllerTest {

    private static final String TEST_KEYSPACE = "test";

    private static TaskManager manager = mock(TaskManager.class);

    @ClassRule
    public static SparkContext ctx = SparkContext.withControllers((spark, config) -> {
        EngineGraknGraphFactory factory = EngineGraknGraphFactory.create(config.getProperties());
        new CommitLogController(spark, config.getProperty(GraknEngineConfig.DEFAULT_KEYSPACE_PROPERTY), 100, manager);
        new SystemController(factory, new SystemKeyspace(factory), spark);
    });

    private Json commitLog;

    @BeforeClass
    public static void setUp() throws Exception {
        GraknTestSetup.startCassandraIfNeeded();
    }

    @After
    public void resetMockitoMockCounts(){
        reset(manager);
    }

    @Test
    @Ignore
    public void whenControllerReceivesLog_TaskManagerReceivesPPTask() {
        sendFakeCommitLog();

        verify(manager, times(1)).addTask(
                argThat(argument -> argument.taskClass().equals(PostProcessingTask.class)),
                argThat(argument -> argument.json().at(COMMIT_LOG_FIXING).equals(commitLog.at(COMMIT_LOG_FIXING)))
        );
    }

    @Test
    @Ignore
    public void whenCommittingGraph_TaskManagerReceivesPPTask() throws InterruptedException {

        final String BOB = "bob";
        final String TIM = "tim";

        GraknGraph bob = Grakn.session(ctx.uri(), BOB).open(GraknTxType.WRITE);
        GraknGraph tim = Grakn.session(ctx.uri(), TIM).open(GraknTxType.WRITE);

        addSomeData(bob);

        verify(manager, times(1)).addTask(
                argThat(argument ->
                        argument.taskClass().equals(PostProcessingTask.class)),
                argThat(argument ->
                        argument.json().at(KEYSPACE).asString().equals(BOB) &&
                        argument.json().at(COMMIT_LOG_FIXING).at(Schema.BaseType.RESOURCE.name()).asJsonMap().size() == 1));

        verify(manager, never()).addTask(
                any(), argThat(arg -> arg.json().at(KEYSPACE).asString().equals(TIM)));

        addSomeData(tim);

        verify(manager, times(1)).addTask(
                argThat(argument ->
                        argument.taskClass().equals(PostProcessingTask.class)),
                argThat(argument ->
                        argument.json().at(KEYSPACE).asString().equals(TIM) &&
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
    @Ignore
    public void whenSendingCommitLogs_TaskManagerReceivesCountTask(){
        sendFakeCommitLog();

        verify(manager, atLeastOnce()).addTask(
                argThat(argument ->
                                argument.taskClass().equals(UpdatingInstanceCountTask.class)),
                argThat(argument ->
                                argument.json().at(COMMIT_LOG_COUNTING).asJsonList().size() == 5)
        );
    }

    @Test
    @Ignore
    public void whenCommittingGraph_TaskManagerReceivesCountTask() throws InterruptedException {
        final String BOB = "bob";
        final String TIM = "tim";

        GraknGraph bob = Grakn.session(ctx.uri(), BOB).open(GraknTxType.WRITE);
        GraknGraph tim = Grakn.session(ctx.uri(), TIM).open(GraknTxType.WRITE);

        addSomeData(bob);
        addSomeData(tim);

        try {
            verify(manager, atLeastOnce()).addTask(
                    argThat(argument ->
                            argument.taskClass().equals(UpdatingInstanceCountTask.class)),
                    argThat(argument ->
                            argument.json().at(KEYSPACE).asString().equals(BOB) &&
                            argument.json().at(COMMIT_LOG_COUNTING).asJsonList().size() == 3));

            verify(manager, atLeastOnce()).addTask(
                    argThat(argument ->
                            argument.taskClass().equals(UpdatingInstanceCountTask.class)),
                    argThat(argument ->
                            argument.json().at(KEYSPACE).asString().equals(TIM) &&
                            argument.json().at(COMMIT_LOG_COUNTING).asJsonList().size() == 3));
        } finally {
            Grakn.session(ctx.uri(), BOB).open(GraknTxType.WRITE).admin().delete();
            Grakn.session(ctx.uri(), TIM).open(GraknTxType.WRITE).admin().delete();

            bob.close();
            tim.close();
        }
    }

    @Test
    public void whenCommittingSystemGraph_CommitLogsNotSent() throws InvalidGraphException {
        GraknGraph graph1 = Grakn.session(ctx.uri(), SystemKeyspace.SYSTEM_GRAPH_NAME).open(GraknTxType.WRITE);
        ResourceType<String> resourceType = graph1.putResourceType("New Resource Type", ResourceType.DataType.STRING);
        resourceType.putResource("a");
        resourceType.putResource("b");
        resourceType.putResource("c");
        graph1.commit();

        verify(manager, never()).addTask(any(), any());
        verify(manager, never()).addTask(any(), any());
    }

    private void sendFakeCommitLog() {
        Json commitLogFixResource = object();
        commitLogFixResource.set("60", array(6));
        commitLogFixResource.set("70", array(7));

        Json commitLogFixing = object();
        commitLogFixing.set(Schema.BaseType.RESOURCE.name(), commitLogFixResource);

        Json commitLogCounting = array();
        commitLogCounting.add(object(COMMIT_LOG_CONCEPT_ID, "Alpha", COMMIT_LOG_SHARDING_COUNT, -3));
        commitLogCounting.add(object(COMMIT_LOG_CONCEPT_ID, "Bravo", COMMIT_LOG_SHARDING_COUNT, -2));
        commitLogCounting.add(object(COMMIT_LOG_CONCEPT_ID, "Delta", COMMIT_LOG_SHARDING_COUNT, -1));
        commitLogCounting.add(object(COMMIT_LOG_CONCEPT_ID, "Charlie", COMMIT_LOG_SHARDING_COUNT,1));
        commitLogCounting.add(object(COMMIT_LOG_CONCEPT_ID, "Foxtrot", COMMIT_LOG_SHARDING_COUNT, 2));

        commitLog = object(
                COMMIT_LOG_FIXING, commitLogFixing,
                COMMIT_LOG_COUNTING, commitLogCounting
        );

        given().contentType(ContentType.JSON).body(commitLog.toString()).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + TEST_KEYSPACE).
                then().statusCode(200).extract().response().andReturn();
    }

    private void addSomeData(GraknGraph graph) throws InvalidGraphException, InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mockito.doAnswer((answer) -> {
            countDownLatch.countDown();
            return true;
        }).when(manager).addTask(any(), any());

        RoleType role1 = graph.putRoleType("Role 1");
        RoleType role2 = graph.putRoleType("Role 2");
        RelationType relationType = graph.putRelationType("A Relation Type").relates(role1).relates(role2);
        EntityType type = graph.putEntityType("A Thing").plays(role1).plays(role2);
        ResourceType<String> resourceType = graph.putResourceType("A Resource Type Thing", ResourceType.DataType.STRING).plays(role1).plays(role2);
        Entity entity = type.addEntity();
        Resource resource = resourceType.putResource(UUID.randomUUID().toString());
        relationType.addRelation().addRolePlayer(role1, entity).addRolePlayer(role2, resource);

        graph.commit();

        countDownLatch.await();
    }
}
