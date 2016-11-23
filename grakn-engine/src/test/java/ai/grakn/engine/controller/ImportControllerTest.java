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
import ai.grakn.util.REST;
import mjson.Json;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static ai.grakn.engine.util.ConfigProperties.DEFAULT_KEYSPACE_PROPERTY;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static org.junit.Assert.assertNotNull;

public class ImportControllerTest extends GraknEngineTestBase {

    private String keyspace = ConfigProperties.getInstance().getProperty(DEFAULT_KEYSPACE_PROPERTY);

    @Test
    public void testLoadOntologyAndData() {
        String dataPath = getPath("small_nametags.gql");
        Json body = Json.object("path", dataPath);
        runAndAssertCorrect(body, keyspace);
    }

    @Test
    public void testLoadOntologyAndDataDistributed() {
        String dataPath = getPath("small_nametags.gql");
        Json body = Json.object("path", dataPath, "hosts", Json.array().add("127.0.0.1"));
        runAndAssertCorrect(body, keyspace);
    }

    @Test
    public void testLoadOntologyAndDataOnCustomKeyspace(){
        String dataPath = getPath("small_nametags.gql");
        String customGraph = "importgraph";
        Json body = Json.object("path", dataPath);

        runAndAssertCorrect(body, customGraph);
    }

    private void runAndAssertCorrect(Json body, String keyspace){
        loadOntology("dblp-ontology.gql", keyspace);

        Response dataResponse = given().
                contentType("application/json").
                queryParam(KEYSPACE_PARAM, keyspace).
                body(body.toString()).when().
                post(REST.WebPath.IMPORT_DATA_URI);

        dataResponse.then().assertThat().statusCode(200);

        //TODO: find a proper way to notify a client when loading is done. Something slightly more elegant than a thread sleep.
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        GraknGraph graph = GraphFactory.getInstance().getGraph(keyspace);
        assertNotNull(graph.getResourcesByValue("X506965727265204162656c").iterator().next().getId());
        graph.clear();
        graph.close();
    }
}