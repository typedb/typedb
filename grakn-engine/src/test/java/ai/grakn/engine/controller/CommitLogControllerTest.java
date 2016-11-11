/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

package ai.grakn.engine.controller;

import ai.grakn.Mindmaps;
import ai.grakn.concept.Entity;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.graph.internal.AbstractMindmapsGraph;
import com.jayway.restassured.http.ContentType;
import ai.grakn.Mindmaps;
import ai.grakn.MindmapsGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.MindmapsEngineTestBase;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.graph.internal.AbstractMindmapsGraph;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
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
    public void testCommitLogSubmission() throws MindmapsValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        MindmapsGraph bob = Mindmaps.factory(Mindmaps.DEFAULT_URI, BOB).getGraph();
        MindmapsGraph tim = Mindmaps.factory(Mindmaps.DEFAULT_URI, TIM).getGraph();

        addSomeData(bob);

        assertEquals(2, cache.getCastingJobs(BOB).size());
        assertEquals(1, cache.getResourceJobs(BOB).size());

        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs(TIM).size());
        assertEquals(1, cache.getResourceJobs(TIM).size());

        Mindmaps.factory(Mindmaps.DEFAULT_URI, BOB).getGraph().clear();
        Mindmaps.factory(Mindmaps.DEFAULT_URI, TIM).getGraph().clear();

        assertEquals(0, cache.getCastingJobs(BOB).size());
        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(BOB).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        cache.getResourceJobs(BOB).forEach(resourceId -> {
            Concept concept = ((AbstractMindmapsGraph) bob).getConceptByBaseIdentifier(resourceId);
            assertTrue(concept.isResource());
        });

    }

    private void addSomeData(MindmapsGraph graph) throws MindmapsValidationException {
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