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

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
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

public class RuleTypeControllerTest {
    private static GraknTx mockGraph;
    private static EngineGraknTxFactory mockFactory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext graphContext = SampleKBContext.preLoad(MovieKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        MetricRegistry metricRegistry = new MetricRegistry();

        new RuleTypeController(mockFactory, spark, metricRegistry);
    });

    @Before
    public void setupMock() {
        mockGraph = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");

        when(mockGraph.putRuleType(anyString())).thenAnswer(invocation ->
            graphContext.tx().putRuleType((String) invocation.getArgument(0)));
        when(mockGraph.getRuleType(anyString())).thenAnswer(invocation ->
            graphContext.tx().getRuleType(invocation.getArgument(0)));

        when(mockFactory.tx(mockGraph.getKeyspace(), GraknTxType.READ)).thenReturn(mockGraph);
        when(mockFactory.tx(mockGraph.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockGraph);
    }

    @Test
    public void getRuleTypeFromMovieGraphShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .get("/graph/ruleType/a-rule-type");

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.at("ruleType").at("conceptId").asString(), notNullValue());
        assertThat(responseBody.at("ruleType").at("label").asString(), equalTo("a-rule-type"));
    }

    @Test
    public void postRuleTypeShouldExecuteSuccessfully() {
        Json body = Json.object("ruleType", Json.object("label", "newRuleType"));
        Response response = with()
            .queryParam(KEYSPACE, mockGraph.getKeyspace())
            .body(body.toString())
            .post("/graph/ruleType");

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.at("ruleType").at("conceptId").asString(), notNullValue());
        assertThat(responseBody.at("ruleType").at("label").asString(), equalTo("newRuleType"));
    }
}
