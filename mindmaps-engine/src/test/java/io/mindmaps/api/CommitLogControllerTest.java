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
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.util.RESTUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

public class CommitLogControllerTest {
    private Properties prop = new Properties();

    @Before
    public void setUp() throws Exception {
        new CommitLogController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        try {
            prop.load(ImportControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        RestAssured.baseURI = prop.getProperty("server.url");
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
        assertTrue(result.contains("Graph [bob] now has [6] post processing jobs"));
    }
}