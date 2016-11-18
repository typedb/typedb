/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

import ai.grakn.GraknGraph;
import ai.grakn.factory.GraphFactory;
import com.jayway.restassured.response.Response;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import ai.grakn.util.REST;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.jayway.restassured.RestAssured.given;

public class ImportControllerTest extends GraknEngineTestBase {

    private String graphName;

    @Before
    public void setUp() throws Exception {
        graphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    }

    @Test
    public void testLoadOntologyAndData() {
        String ontologyPath = getClass().getClassLoader().getResource("dblp-ontology.gql").getPath();
        String dataPath = getClass().getClassLoader().getResource("small_nametags.gql").getPath();

        importOntology(ontologyPath,graphName);

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

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getResourcesByValue("X506965727265204162656c").iterator().next().getId());
        GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }

    @Test
    public void testLoadOntologyAndDataDistributed() {
        String ontologyPath = getClass().getClassLoader().getResource("dblp-ontology.gql").getPath();
        String dataPath = getClass().getClassLoader().getResource("small_nametags.gql").getPath();


        importOntology(ontologyPath,graphName);

        Response dataResponse = given().contentType("application/json").
                body(Json.object("path", dataPath, "hosts", Json.array().add("127.0.0.1")).toString()).when().
                post(REST.WebPath.IMPORT_DISTRIBUTED_URI);

        dataResponse.then().assertThat().statusCode(200);

        //TODO: find a proper way to notify a client when loading is done. Something slightly more elegant than a thread sleep.
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(graphName).getResourcesByValue("X506965727265204162656c").iterator().next().getId());
        GraphFactory.getInstance().getGraphBatchLoading(graphName).clear();
    }

    @Test
    public void testLoadOntologyAndDataOnCustomKeyspace() {
        String ontologyPath = getClass().getClassLoader().getResource("dblp-ontology.gql").getPath();
        String dataPath = getClass().getClassLoader().getResource("small_nametags.gql").getPath();
        String customGraph = "importgraph";

        importOntology(ontologyPath,customGraph);

        Response dataResponse = given().contentType("application/json").
                body(Json.object("path", dataPath, "graphName", customGraph).toString()).when().
                post(REST.WebPath.IMPORT_DATA_URI);

        dataResponse.then().assertThat().statusCode(200);

        //TODO: find a proper way to notify a client when loading is done. Something slightly more elegant than a thread sleep.
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(GraphFactory.getInstance().getGraphBatchLoading(customGraph).getResourcesByValue("X506965727265204162656c").iterator().next().getId());
        GraphFactory.getInstance().getGraphBatchLoading(customGraph).clear();
    }

    private void importOntology(String ontologyFile, String graphNameParam){
        GraknGraph graph = GraphFactory.getInstance().getGraph(graphNameParam);

        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get(ontologyFile), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
        Graql.parse(query).withGraph(graph).execute();
        try {
            graph.commit();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }
    }


}