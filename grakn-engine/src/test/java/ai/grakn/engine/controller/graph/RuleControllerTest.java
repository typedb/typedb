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

package ai.grakn.engine.controller.graph;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RuleControllerTest {
    private static GraknGraph mockGraph;
    private static EngineGraknGraphFactory mockFactory = mock(EngineGraknGraphFactory.class);

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        MetricRegistry metricRegistry = new MetricRegistry();

        new RuleTypeController(mockFactory, spark, metricRegistry);
    });

    @Before
    public void setupMock() {
        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");

        when(mockGraph.putRuleType(anyString())).thenAnswer(invocation ->
            graphContext.graph().putRuleType((String) invocation.getArgument(0)));
        when(mockGraph.getRuleType(anyString())).thenAnswer(invocation ->
            graphContext.graph().getRuleType(invocation.getArgument(0)));

        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.READ)).thenReturn(mockGraph);
        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockGraph);
    }

    @Test
    public void getRuleFromMovieGraphShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .get("/graph/ruleType/a-rule-type");

        Map<String, Object> responseBody = Json.read(response.body().asString()).asMap();

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.get("conceptId"), notNullValue());
        assertThat(responseBody.get("ruleTypeLabel"), equalTo("a-rule-type"));
    }

    @Test
    public void postRuleTypeShouldExecuteSuccessfully() {
        Json body = Json.object("ruleTypeLabel", "newRuleType");
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .body(body.toString())
            .post("/graph/ruleType");

        Map<String, Object> responseBody = Json.read(response.body().asString()).asMap();

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.get("conceptId"), notNullValue());
        assertThat(responseBody.get("ruleTypeLabel"), equalTo("newRuleType"));
    }
}
