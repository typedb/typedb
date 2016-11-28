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
import ai.grakn.concept.Entity;
import ai.grakn.engine.loader.LoaderTask;
import ai.grakn.factory.GraphFactory;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.PatternAdmin;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.graql.Graql.parse;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;

import static com.jayway.restassured.RestAssured.given;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TasksLoadingControllerTest extends GraknEngineTestBase {

    private static final String keyspace = "KEYSPACE";
    private static final int NUMBER_TO_TEST = 10;
    private GraknGraph graph;

    @Before
    public void setup() {
        graph = GraphFactory.getInstance().getGraph(keyspace);
    }

    @After
    public void clean(){
        graph.clear();
        graph.close();
    }

    @Test
    public void loaderTaskAPITest(){
        loadOntology("dblp-ontology.gql", keyspace);

        String nametags = readFileAsString("small_nametags.gql");

        String response = given()
                .queryParam(TASK_CLASS_NAME_PARAMETER, LoaderTask.class.getName())
                .queryParam(TASK_CREATOR_PARAMETER, TasksLoadingControllerTest.class.getName())
                .queryParam(TASK_RUN_AT_PARAMETER, new Date().getTime())
                .body(getConfiguration(nametags))
                .when().post(TASKS_SCHEDULE_URI).body().asString();

        String taskID = new JSONObject(response).getString("id");

        waitToFinish(taskID);

        graph = GraphFactory.getInstance().getGraph(keyspace);
        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(NUMBER_TO_TEST, nameTags.size());
        assertNotNull(graph.getResourcesByValue("X532e20492e204b68617368696e").iterator().next().getId());
    }

    private String getConfiguration(String queries){
        Collection<InsertQuery> inserts =
                ((InsertQuery) parse(queries)).admin().getVars().stream()
                .map(Pattern::admin)
                .map(PatternAdmin::getVars)
                .map(Graql::insert).limit(NUMBER_TO_TEST)
                        .collect(toSet());

        JSONObject json = new JSONObject();
        json.put(KEYSPACE_PARAM, keyspace);
        json.put(TASK_LOADER_INSERTS, inserts.stream().map(InsertQuery::toString).collect(toList()));
        return json.toString();
    }

    private void waitToFinish(String id) {
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 10000) {
            String response = given().get(TASKS_URI + "/" + id).then().extract().response().asString();
            String status = new JSONObject(response).getString(TASK_STATUS_PARAMETER);

            if (status.equals(COMPLETED.name()))
                break;

            try {
                Thread.sleep(100);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
