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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.http.ContentType;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.util.REST;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.engine.Util;
import io.mindmaps.engine.postprocessing.Cache;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.MindmapsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CommitLogControllerTest {
    private Cache cache;

    @BeforeClass
    public static void startController() {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
    }

    @Before
    public void setUp() throws Exception {
        new CommitLogController();
        new GraphFactoryController();
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());

        cache = Cache.getInstance();

        String commitLog = "{\n" +
                "    \"concepts\":[\n" +
                "        {\"id\":\"1\", \"type\":\"CASTING\"}, \n" +
                "        {\"id\":\"2\", \"type\":\"CASTING\"}, \n" +
                "        {\"id\":\"3\", \"type\":\"CASTING\"}, \n" +
                "        {\"id\":\"4\", \"type\":\"CASTING\"}, \n" +
                "        {\"id\":\"5\", \"type\":\"RELATION\"},\n" +
                "        {\"id\":\"6\", \"type\":\"RELATION\"}\n" +
                "    ]\n" +
                "}";

        given().contentType(ContentType.JSON).body(commitLog).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + "test").
                then().statusCode(200).extract().response().andReturn();
    }

    @After
    public void takeDown() {
        cache.getCastingJobs().clear();
    }

    @Test
    public void testControllerWorking() {
        assertEquals(4, cache.getCastingJobs().values().iterator().next().size());
    }

    @Test
    public void testCommitLogSubmission() throws MindmapsValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        MindmapsGraph bob = MindmapsClient.getGraph(BOB);
        MindmapsGraph tim = MindmapsClient.getGraph(TIM);

        addSomeData(bob);

        assertEquals(2, cache.getCastingJobs().get(BOB).size());

        assertNull(cache.getCastingJobs().get(TIM));

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs().get(TIM).size());

        MindmapsClient.getGraph(BOB).clear();
        MindmapsClient.getGraph(TIM).clear();

        assertEquals(0, cache.getCastingJobs().get(BOB).size());
        assertEquals(0, cache.getCastingJobs().get(TIM).size());
    }

    private void addSomeData(MindmapsGraph graph) throws MindmapsValidationException {
        RoleType role1 = graph.putRoleType("Role 1");
        RoleType role2 = graph.putRoleType("Role 2");
        RelationType relationType = graph.putRelationType("A Relation Type").hasRole(role1).hasRole(role2);
        EntityType type = graph.putEntityType("A Thing").playsRole(role1).playsRole(role2);
        Entity entity1 = graph.addEntity(type);
        Entity entity2 = graph.addEntity(type);

        graph.addRelation(relationType).putRolePlayer(role1, entity1).putRolePlayer(role2, entity2);

        graph.commit();
    }

    @Test
    public void testDeleteController() {
        assertEquals(4, cache.getCastingJobs().values().iterator().next().size());

        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + "test").
                then().statusCode(200).extract().response().andReturn();

        assertEquals(0, cache.getCastingJobs().values().iterator().next().size());
    }
}