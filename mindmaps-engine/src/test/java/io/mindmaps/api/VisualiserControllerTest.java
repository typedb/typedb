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
import com.jayway.restassured.response.Response;
import io.mindmaps.MindmapsTransaction;
import io.mindmaps.Util;
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.ConfigProperties;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VisualiserControllerTest {

    static String graphName;


    @Before
    public void setUp() throws Exception {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);

        new VisualiserController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        graphName="special-test-graph";
        MindmapsGraph graph = GraphFactory.getInstance().getGraph(graphName);
        MindmapsTransaction transaction = graph.getTransaction();
        EntityType man = transaction.putEntityType("Man");
        transaction.putEntity("actor-123", man);
        transaction.commit();
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());
    }

    @Test
     public void notExistingID() {
        Response response = get(RESTUtil.WebPath.CONCEPT_BY_ID_URI+"6573gehjio?graphName="+graphName).then().statusCode(404).extract().response().andReturn();
        String  message = response.getBody().asString();
        assertTrue(message.equals("ID [6573gehjio] not found in the graph."));
    }

    @Test
    public void getConceptByID() {
        Response response = get(RESTUtil.WebPath.CONCEPT_BY_ID_URI+"actor-123?graphName="+graphName).then().statusCode(200).extract().response().andReturn();
        JSONObject message = new JSONObject(response.getBody().asString());
        assertTrue(message.getString("_type").equals("Man"));
        assertFalse(message.has("_value"));
        assertTrue(message.getString("_id").equals("actor-123"));
    }

    @Test
    public void notExistingIDInDefaultGraph() {
        get("/graph/concept/actor-123").then().statusCode(404).extract().response().andReturn();
    }

    @After
    public void cleanGraph(){
        GraphFactory.getInstance().getGraph(graphName).clear();
    }
}
