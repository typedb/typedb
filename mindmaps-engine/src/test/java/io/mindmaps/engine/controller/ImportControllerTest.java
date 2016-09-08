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
import com.jayway.restassured.response.Response;
import io.mindmaps.util.REST;
import io.mindmaps.engine.Util;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import mjson.Json;
import org.junit.*;

import static com.jayway.restassured.RestAssured.given;

public class ImportControllerTest {

    ImportController importer;
    String graphName;

    @BeforeClass
    public static void startController() {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
    }

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        importer = new ImportController();
        new CommitLogController();
        new GraphFactoryController();
        new ImportController();
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());

    }

    @Test
    public void testLoadOntologyAndData() {
        String ontologyPath = getClass().getClassLoader().getResource("dblp-ontology.gql").getPath();
        String dataPath = getClass().getClassLoader().getResource("small_nametags.gql").getPath();


        Response ontologyResponse = given().contentType("application/json").
                body(Json.object("path", ontologyPath).toString()).when().
                post(REST.WebPath.IMPORT_ONTOLOGY_URI);

        ontologyResponse.then().assertThat().statusCode(200);

        Response dataResponse = given().contentType("application/json").
                body(Json.object("path", dataPath).toString()).when().
                post(REST.WebPath.IMPORT_DATA_URI);

        dataResponse.then().assertThat().statusCode(200);

        //TODO: find a proper way to notify a client when loading is done. Something slightly more elegant than a thread sleep.
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

         Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getConcept("X506965727265204162656c").getId());
         GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }

    @Test
    public void testLoadOntologyAndDataOnCustomKeyspace() {
        String ontologyPath = getClass().getClassLoader().getResource("dblp-ontology.gql").getPath();
        String dataPath = getClass().getClassLoader().getResource("small_nametags.gql").getPath();
        String customGraph = "import-graph";


        Response ontologyResponse = given().contentType("application/json").
                body(Json.object("path", ontologyPath,"graphName",customGraph).toString()).when().
                post(REST.WebPath.IMPORT_ONTOLOGY_URI);

        ontologyResponse.then().assertThat().statusCode(200);

        Response dataResponse = given().contentType("application/json").
                body(Json.object("path", dataPath,"graphName",customGraph).toString()).when().
                post(REST.WebPath.IMPORT_DATA_URI);

        dataResponse.then().assertThat().statusCode(200);

        //TODO: find a proper way to notify a client when loading is done. Something slightly more elegant than a thread sleep.
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(customGraph).getConcept("X506965727265204162656c").getId());
        GraphFactory.getInstance().getGraphBatchLoading(customGraph).clear();
    }


}