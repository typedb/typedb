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

package io.mindmaps.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.postprocessing.Cache;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.RESTUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CommitLogControllerTest {
    private Properties prop = new Properties();
    private Cache cache;

    @Before
    public void setUp() throws Exception {
        new CommitLogController();
        new GraphFactoryController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        try {
            prop.load(ImportControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        RestAssured.baseURI = prop.getProperty("server.url");
        cache = Cache.getInstance();
    }

    @After
    public void takeDown(){
        cache.getRelationJobs().clear();
        cache.getCastingJobs().clear();
    }

    @Test
    public void testControllerWorking(){
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

        Response response = given().contentType(ContentType.JSON).body(commitLog).when().
                post(RESTUtil.WebPath.COMMIT_LOG_URI + "?" + RESTUtil.Request.GRAPH_NAME_PARAM + "=" + "bob").
                then().statusCode(200).extract().response().andReturn();

        String result = response.getBody().prettyPrint();
        assertEquals(4, cache.getCastingJobs().values().iterator().next().size());
        assertEquals(2, cache.getRelationJobs().values().iterator().next().size());
        assertTrue(result.contains("Graph [bob] now has [6] post processing jobs"));
    }

    @Test
    public void testCommitLogSubmission() throws MindmapsValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        MindmapsTransaction bob = MindmapsClient.getGraph(BOB).newTransaction();
        MindmapsTransaction tim = MindmapsClient.getGraph(TIM).newTransaction();

        addSomeData(bob);

        assertEquals(1, cache.getRelationJobs().get(BOB).size());
        assertEquals(2, cache.getCastingJobs().get(BOB).size());

        assertNull(cache.getRelationJobs().get(TIM));
        assertNull(cache.getCastingJobs().get(TIM));

        addSomeData(tim);

        assertEquals(1, cache.getRelationJobs().get(TIM).size());
        assertEquals(2, cache.getCastingJobs().get(TIM).size());
    }

    private void addSomeData(MindmapsTransaction transaction) throws MindmapsValidationException {
        RoleType role1 = transaction.putRoleType("Role 1");
        RoleType role2 = transaction.putRoleType("Role 2");
        RelationType relationType = transaction.putRelationType("A Relation Type").hasRole(role1).hasRole(role2);
        EntityType type = transaction.putEntityType("A Thing").playsRole(role1).playsRole(role2);
        Entity entity1 = transaction.addEntity(type);
        Entity entity2 = transaction.addEntity(type);

        transaction.addRelation(relationType).putRolePlayer(role1, entity1).putRolePlayer(role2, entity2);

        transaction.commit();
    }
}