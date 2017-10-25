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
 *
 */

package ai.grakn.engine.controller;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineStatus;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.when;
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

/**
 * @author Felix Chapman
 */
public class SystemControllerTest {

    private static final Properties properties = GraknEngineConfig.create().getProperties();
    private static final GraknEngineStatus status = mock(GraknEngineStatus.class);

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        EngineGraknTxFactory factory = EngineGraknTxFactory.createAndLoadSystemSchema(properties);
        new SystemController(factory, spark, status, mock(MetricRegistry.class));
    });

    @Test
    public void whenCallingKBEndpoint_Return200() {
        when().get("/kb").then().statusCode(SC_OK);
    }

    @Test
    public void whenCallingKBEndpoint_ReturnJson() {
        when().get("/kb").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenInitiallyCallingKBEndpoint_GetEmptyList() {
        when().get("/kb").then().body("", empty());
    }

    @Test
    public void whenCallingPutKBEndpoint_Return204() {
        when().put("/kb/myks").then().statusCode(SC_NO_CONTENT).body(isEmptyString());
    }

    @Test
    public void whenCallingPutKBEndpointOnNonExistentKeyspace_KeyspaceAppearsInList() {
        RestAssured.put("/kb/myks");
        when().get("/kb").then().body("", hasItem("myks"));
    }

    @Test
    public void whenCallingPutKBEndpointTwice_NothingHappens() {
        RestAssured.put("/kb/myks");
        RestAssured.put("/kb/myks");

        when().get("/kb").then().body("", hasItem("myks"));
    }

    @Test
    public void whenCallingDeleteKBEndpointOnExistingKeyspace_Return204() {
        RestAssured.put("/kb/myks");

        when().delete("/kb/myks").then().statusCode(SC_NO_CONTENT).body(isEmptyString());
    }

    @Test
    public void whenCallingDeleteKBEndpointOnExistingKeyspace_KeyspaceDisappearsFromList() {
        RestAssured.put("/kb/myks");

        RestAssured.delete("/kb/myks");

        when().get("/kb").then().body("", not(hasItem("myks")));
    }

    @Test
    public void whenCallingConfigurationEndpoint_GetConfigurationWithoutJWTPassword() {
        Json expected = Json.make(properties);
        expected.delAt(GraknConfigKey.JWT_SECRET.name());

        when().get("/configuration").then().body(is(expected.toString()));
    }

    @Test
    public void whenCallingStatusEndpoint_AndStatusIsReady_ReturnReady() {
        Mockito.when(status.isReady()).thenReturn(true);
        when().get("/status").then().body(is("READY"));
    }

    @Test
    public void whenCallingStatusEndpoint_AndStatusIsNotReady_ReturnInitializing() {
        Mockito.when(status.isReady()).thenReturn(false);
        when().get("/status").then().body(is("INITIALIZING"));
    }
}