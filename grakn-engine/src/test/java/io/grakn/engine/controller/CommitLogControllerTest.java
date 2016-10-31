/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.engine.controller;

import com.jayway.restassured.http.ContentType;
import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.concept.Concept;
import io.grakn.concept.Entity;
import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.Resource;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.engine.MindmapsEngineTestBase;
import io.grakn.engine.postprocessing.Cache;
import io.grakn.exception.GraknValidationException;
import io.grakn.graph.internal.AbstractGraknGraph;
import io.grakn.util.REST;
import io.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommitLogControllerTest extends MindmapsEngineTestBase {
    public final String KEYSPACE = "test";
    private Cache cache;

    @Before
    public void setUp() throws Exception {
        cache = Cache.getInstance();

        String commitLog = "{\n" +
                "    \"concepts\":[\n" +
                "        {\"id\":\"1\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"2\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"3\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"4\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"5\", \"type\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"id\":\"6\", \"type\":\"" + Schema.BaseType.RESOURCE + "\"},\n" +
                "        {\"id\":\"7\", \"type\":\"" + Schema.BaseType.RESOURCE + "\"},\n" +
                "        {\"id\":\"8\", \"type\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"id\":\"9\", \"type\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"id\":\"10\", \"type\":\"" + Schema.BaseType.RELATION + "\"}\n" +
                "    ]\n" +
                "}";

        given().contentType(ContentType.JSON).body(commitLog).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + KEYSPACE).
                then().statusCode(200).extract().response().andReturn();
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs(KEYSPACE).clear();
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

        cache.getResourceJobs(BOB).forEach(resourceId -> {
            Concept concept = ((AbstractGraknGraph) bob).getConceptByBaseIdentifier(resourceId);
            assertTrue(concept.isResource());
        });

    }

    private void addSomeData(GraknGraph graph) throws GraknValidationException {
        RoleType role1 = graph.putRoleType("Role 1");
        RoleType role2 = graph.putRoleType("Role 2");
        RelationType relationType = graph.putRelationType("A Relation Type").hasRole(role1).hasRole(role2);
        EntityType type = graph.putEntityType("A Thing").playsRole(role1).playsRole(role2);
        ResourceType<String> resourceType = graph.putResourceType("A Resource Type Thing", ResourceType.DataType.STRING).playsRole(role1).playsRole(role2);
        Entity entity = graph.addEntity(type);
        Resource resource = graph.putResource(UUID.randomUUID().toString(), resourceType);

        graph.addRelation(relationType).putRolePlayer(role1, entity).putRolePlayer(role2, resource);

        graph.commit();
    }

    @Test
    public void testDeleteController() throws InterruptedException {
        assertEquals(4, cache.getCastingJobs(KEYSPACE).size());
        assertEquals(2, cache.getResourceJobs(KEYSPACE).size());

        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + KEYSPACE).
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
}