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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.factory.GraphFactory;
import com.jayway.restassured.response.Response;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.util.REST;
import mjson.Json;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;

import static com.jayway.restassured.RestAssured.given;
import static ai.grakn.engine.util.ConfigProperties.DEFAULT_KEYSPACE_PROPERTY;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static com.jayway.restassured.RestAssured.post;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ImportControllerTest extends GraknEngineTestBase {

    private String keyspace = ConfigProperties.getInstance().getProperty(DEFAULT_KEYSPACE_PROPERTY);

    @Test
    public void testLoadOntologyAndData() {
        String dataPath = getPath("smaller_nametags.gql");
        Json body = Json.object("path", dataPath);
        runAndAssertCorrect(body, keyspace);
    }

    @Test
    public void testLoadOntologyAndDataDistributed() {
        String dataPath = getPath("smaller_nametags.gql");
        Json body = Json.object("path", dataPath, "hosts", Json.array().add(Grakn.DEFAULT_URI));
        runAndAssertCorrect(body, keyspace);
    }

    @Test
    public void testLoadOntologyAndDataOnCustomKeyspace(){
        String dataPath = getPath("smaller_nametags.gql");
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

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        waitToFinish();

        GraknGraph graph = GraphFactory.getInstance().getGraph(keyspace);

        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();
        assertEquals(nameTags.size(), 10);
        assertNotNull(graph.getResourcesByValue("X4a656e6e69666572204d656c6f6f6e").iterator().next().getId());
        graph.clear();
        graph.close();
    }

    private void waitToFinish() {
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 60000) {
            Response response = post(REST.WebPath.IMPORT_DATA_URI);
            if (response.statusCode() != 423)
                break;

            try {
                Thread.sleep(100);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}