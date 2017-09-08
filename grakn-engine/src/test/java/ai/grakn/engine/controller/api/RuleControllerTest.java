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

package ai.grakn.engine.controller.api;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static com.jayway.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RuleControllerTest {
    private static GraknTx mockTx;
    private static EngineGraknTxFactory factory = mock(EngineGraknTxFactory.class);

    @ClassRule
    public static SampleKBContext sampleKB = SampleKBContext.preLoad(MovieKB.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new RuleController(factory, spark);
    });

    @Before
    public void setupMock() {
        mockTx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.getKeyspace()).thenReturn("randomKeyspace");

        when(mockTx.graql()).thenAnswer(invocation -> sampleKB.tx().graql());

        when(mockTx.putRule(anyString(), any(), any())).thenAnswer(invocation ->
            sampleKB.tx().putRule(
                (String) invocation.getArgument(0),
                invocation.getArgument(1),
                invocation.getArgument(2)
            )
        );
        when(mockTx.getRule(anyString())).thenAnswer(invocation ->
            sampleKB.tx().getRule(invocation.getArgument(0)));

        when(factory.tx(mockTx.getKeyspace(), GraknTxType.READ)).thenReturn(mockTx);
        when(factory.tx(mockTx.getKeyspace(), GraknTxType.WRITE)).thenReturn(mockTx);
    }

    @Test
    public void getRuleFromMovieGraphShouldExecuteSuccessfully() {
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace())
            .get("/api/rule/expectation-rule");

        Json responseBody = Json.read(response.body().asString());
        
        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.at("rule").at("conceptId").asString(), notNullValue());
        assertThat(responseBody.at("rule").at("label").asString(), equalTo("expectation-rule"));
    }

    @Test
    public void postRuleShouldExecuteSuccessfully() {
        Json body = Json.object("rule", Json.object(
            "label", "newRule",
            "when", "$x has name \"newRule-when\"",
            "then", "$x has name \"newRule-then\""
        ));
        Response response = with()
            .queryParam(KEYSPACE, mockTx.getKeyspace())
            .body(body.toString())
            .post("/api/rule");

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(200));
        assertThat(responseBody.at("rule").at("conceptId").asString(), notNullValue());
        assertThat(responseBody.at("rule").at("label").asString(), equalTo("newRule"));
        assertThat(responseBody.at("rule").at("when").asString(), equalTo("$x has name \"newRule-when\""));
        assertThat(responseBody.at("rule").at("then").asString(), equalTo("$x has name \"newRule-then\""));
    }
}
