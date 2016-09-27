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

package io.mindmaps.engine.controller;

import com.jayway.restassured.http.ContentType;
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.engine.Util;
import io.mindmaps.engine.postprocessing.Cache;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.util.REST;
import io.mindmaps.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import spark.Spark;

import java.util.UUID;

import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommitLogControllerTest {
    public final String KEYSPACE = "test";
    private Cache cache;

    @BeforeClass
    public static void setUpController() throws InterruptedException {
        Spark.stop();
        Thread.sleep(5000);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());
        new CommitLogController();
        new GraphFactoryController();
        Thread.sleep(5000);
    }

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

        MindmapsGraph bob = Mindmaps.factory().getGraph(BOB);
        MindmapsGraph tim = Mindmaps.factory().getGraph(TIM);

        addSomeData(bob);

        assertEquals(2, cache.getCastingJobs(BOB).size());
        assertEquals(1, cache.getResourceJobs(BOB).size());

        assertEquals(0, cache.getCastingJobs(TIM).size());
        assertEquals(0, cache.getResourceJobs(TIM).size());

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs(TIM).size());
        assertEquals(1, cache.getResourceJobs(TIM).size());

        Mindmaps.factory().getGraph(BOB).clear();
        Mindmaps.factory().getGraph(TIM).clear();

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

    @Ignore
    public void testDeleteController() throws InterruptedException {
        assertEquals(4, cache.getCastingJobs(KEYSPACE).size());

        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + KEYSPACE).
                then().statusCode(200).extract().response().andReturn();

        Thread.sleep(2000);

        assertEquals(0, cache.getCastingJobs(KEYSPACE).size());
    }
}