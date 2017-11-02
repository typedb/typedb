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

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineStatus;
import ai.grakn.engine.SystemKeyspaceFake;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
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
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final SystemKeyspaceFake systemKeyspace = SystemKeyspaceFake.of();

    @ClassRule
    public static final SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new SystemController(spark, properties, systemKeyspace, status, metricRegistry);
    });

    @Before
    public void setUp() {
        systemKeyspace.clear();
    }

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
    public void whenCallingGetKBEndpointOnNonExistentKeyspace_Return404() {
        when().get("/kb/myks").then().statusCode(SC_NOT_FOUND).body(isEmptyString());
    }

    @Test
    public void whenCallingGetKBEndpointOnExistingKeyspace_Return200() {
        RestAssured.put("/kb/myks");

        when().get("/kb/myks").then().statusCode(SC_OK).body(isEmptyString());
    }

    @Test
    public void whenCallingPutKBEndpoint_Return200_AndConfigInBody() {
        when().put("/kb/myks").then().statusCode(SC_OK).body(is(Json.make(properties).toString()));
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
    public void whenCallingStatusEndpoint_AndStatusIsReady_ReturnReady() {
        Mockito.when(status.isReady()).thenReturn(true);
        when().get("/status").then().body(is("READY"));
    }

    @Test
    public void whenCallingStatusEndpoint_AndStatusIsNotReady_ReturnInitializing() {
        Mockito.when(status.isReady()).thenReturn(false);
        when().get("/status").then().body(is("INITIALIZING"));
    }

    @Test
    public void whenCallingMetricsEndpoint_ReturnJson() {
        when().get("/metrics").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingMetricsEndpoint_ReturnOK() {
        when().get("/metrics").then().statusCode(SC_OK);
    }

    @Test
    public void whenCallingMetricsEndpointAndRequestingJson_ReturnJson() {
        given().param("format", "json").when().get("/metrics").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingMetricsEndpointAndRequestingText_ReturnText() {
        given().param("format", "prometheus").when().get("/metrics").then().contentType(ContentType.TEXT);
    }

    @Test
    public void whenCallingMetricsEndpointAndRequestingInvalidType_Return400() {
        given().param("format", "rainbows").when().get("/metrics").then().statusCode(SC_BAD_REQUEST);
    }
}

