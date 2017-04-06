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
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graph.admin.ConceptCache;
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
    private final ConceptCache cache = EngineCacheProvider.getCache();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

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
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + KEYSPACE).
                then().statusCode(200).extract().response().andReturn();
    }

    @After
    public void clearCache() throws InterruptedException {
        cache.getCastingJobs(KEYSPACE).clear();
    }

    @Test
    public void whenClearingGraph_CommitLogClearsCache(){
        GraknGraph test = Grakn.session(Grakn.DEFAULT_URI, KEYSPACE).open(GraknTxType.WRITE);
        test.admin().clear(EngineCacheProvider.getCache());
        assertEquals(0, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(0, cache.getResourceJobs(KEYSPACE).size());
        assertEquals(0, cache.getInstanceCountJobs(KEYSPACE).size());
    }

    @Test
    public void whenControllerReceivesLog_CacheIsUpdated() {
        assertEquals(4, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(2, cache.getResourceJobs(KEYSPACE).size());
        assertEquals(5, cache.getInstanceCountJobs(KEYSPACE).size());
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
        assertEquals(3, cache.getInstanceCountJobs(BOB).size());

        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());
        assertEquals(0, cache.getInstanceCountJobs(TIM).size());

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs(TIM).size());
        assertEquals(1, cache.getResourceJobs(TIM).size());
        assertEquals(3, cache.getInstanceCountJobs(TIM).size());

        Grakn.session(Grakn.DEFAULT_URI, BOB).open(GraknTxType.WRITE).clear();
        Grakn.session(Grakn.DEFAULT_URI, TIM).open(GraknTxType.WRITE).clear();

        assertEquals(0, cache.getCastingJobs(BOB).size());
        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(BOB).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        assertEquals(0, cache.getInstanceCountJobs(BOB).size());
        assertEquals(0, cache.getInstanceCountJobs(BOB).size());

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
        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.KEYSPACE_PARAM + "=" + KEYSPACE).
                then().statusCode(200).extract().response().andReturn();

        waitForCache(KEYSPACE, 0);

        assertEquals(0, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(0, cache.getResourceJobs(KEYSPACE).size());
        assertEquals(0, cache.getInstanceCountJobs(KEYSPACE).size());
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