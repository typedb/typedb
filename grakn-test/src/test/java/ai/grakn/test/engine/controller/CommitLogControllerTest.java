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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.postprocessing.EngineCache;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.jayway.restassured.http.ContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;

public class CommitLogControllerTest {
    private final String KEYSPACE = "test";
    private final EngineCache cache = EngineCache.getInstance();

    @ClassRule
    public static final EngineContext engine = EngineContext.startMultiQueueServer();

    @Before
    public void setUp() throws Exception {
        String commitLog = "{\n" +
                "    \"concepts\":[\n" +
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
                "    ]\n" +
                "}";

        given().contentType(ContentType.JSON).body(commitLog).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + KEYSPACE).
                then().statusCode(200).extract().response().andReturn();
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs(KEYSPACE).clear();
    }

    @Test
    public void checkDirectClearWorks(){
        GraknGraph test = Grakn.factory(Grakn.DEFAULT_URI, KEYSPACE).getGraph();
        test.admin().clear(EngineCache.getInstance());
        assertEquals(0, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(0, cache.getResourceJobs(KEYSPACE).size());
    }

    @Test
    public void testControllerWorking() {
        assertEquals(4, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(2, cache.getResourceJobs(KEYSPACE).size());
    }

    @Test
    public void testCommitLogSubmission() throws GraknValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        GraknGraph bob = Grakn.factory(Grakn.DEFAULT_URI, BOB).getGraph();
        GraknGraph tim = Grakn.factory(Grakn.DEFAULT_URI, TIM).getGraph();

        addSomeData(bob);

        assertEquals(2, cache.getCastingJobs(BOB).size());
        assertEquals(1, cache.getResourceJobs(BOB).size());

        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs(TIM).size());
        assertEquals(1, cache.getResourceJobs(TIM).size());

        Grakn.factory(Grakn.DEFAULT_URI, BOB).getGraph().clear();
        Grakn.factory(Grakn.DEFAULT_URI, TIM).getGraph().clear();

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
        RelationType relationType = graph.putRelationType("A Relation Type").hasRole(role1).hasRole(role2);
        EntityType type = graph.putEntityType("A Thing").playsRole(role1).playsRole(role2);
        ResourceType<String> resourceType = graph.putResourceType("A Resource Type Thing", ResourceType.DataType.STRING).playsRole(role1).playsRole(role2);
        Entity entity = type.addEntity();
        Resource resource = resourceType.putResource(UUID.randomUUID().toString());

        relationType.addRelation().addRolePlayer(role1, entity).addRolePlayer(role2, resource);

        graph.commitOnClose();
        graph.close();
    }

    @Test
    public void testDeleteController() throws InterruptedException {
        assertEquals(4, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(2, cache.getResourceJobs(KEYSPACE).size());

        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + KEYSPACE).
                then().statusCode(200).extract().response().andReturn();

        waitForCache(KEYSPACE, 0);

        assertEquals(0, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(0, cache.getResourceJobs(KEYSPACE).size());
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
    public void testSystemKeyspaceNotSubmittingLogs() throws GraknValidationException {
        GraknGraph graph1 = Grakn.factory(Grakn.DEFAULT_URI, SystemKeyspace.SYSTEM_GRAPH_NAME).getGraph();
        ResourceType<String> resourceType = graph1.putResourceType("New Resource Type", ResourceType.DataType.STRING);
        resourceType.putResource("a");
        resourceType.putResource("b");
        resourceType.putResource("c");
        graph1.commitOnClose();
        graph1.close();

        assertEquals(0, cache.getResourceJobs(SystemKeyspace.SYSTEM_GRAPH_NAME).size());
    }
}