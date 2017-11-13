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
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.REST;
import ai.grakn.util.SampleKBLoader;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.LABEL_JSON_FIELD;
import static ai.grakn.util.REST.Request.RULE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.THEN_JSON_FIELD;
import static ai.grakn.util.REST.Request.WHEN_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.RULE;
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
    public static SampleKBContext sampleKB = MovieKB.context();

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new RuleController(factory, spark);
    });

    @Before
    public void setupMock() {
        mockTx = mock(GraknTx.class, RETURNS_DEEP_STUBS);

        when(mockTx.keyspace()).thenReturn(SampleKBLoader.randomKeyspace());

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

        when(factory.tx(mockTx.keyspace(), GraknTxType.READ)).thenReturn(mockTx);
        when(factory.tx(mockTx.keyspace(), GraknTxType.WRITE)).thenReturn(mockTx);
    }

    @Test
    public void getRuleFromMovieKbShouldExecuteSuccessfully() {
        String expectationRule = "expectation-rule";

        Response response = with()
            .get(REST.resolveTemplate(RULE + "/" + expectationRule, mockTx.keyspace().getValue()));

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at(RULE_OBJECT_JSON_FIELD).at(CONCEPT_ID_JSON_FIELD).asString(), notNullValue());
        assertThat(responseBody.at(RULE_OBJECT_JSON_FIELD).at(LABEL_JSON_FIELD).asString(), equalTo(expectationRule));
    }

    @Test
    public void postRuleShouldExecuteSuccessfully() {
        String rule = "newRule";
        String when = "$x has name \"newRule-when\"";
        String then = "$x has name \"newRule-then\"";
        Json body = Json.object(RULE_OBJECT_JSON_FIELD, Json.object(
            LABEL_JSON_FIELD, rule,
            WHEN_JSON_FIELD, when,
            THEN_JSON_FIELD, then
        ));
        Response response = with()
            .body(body.toString())
            .post(REST.resolveTemplate(RULE, mockTx.keyspace().getValue()));

        Json responseBody = Json.read(response.body().asString());

        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(responseBody.at(RULE_OBJECT_JSON_FIELD).at(CONCEPT_ID_JSON_FIELD).asString(), notNullValue());
        assertThat(responseBody.at(RULE_OBJECT_JSON_FIELD).at(LABEL_JSON_FIELD).asString(), equalTo(rule));
        assertThat(responseBody.at(RULE_OBJECT_JSON_FIELD).at(WHEN_JSON_FIELD).asString(), equalTo(when));
        assertThat(responseBody.at(RULE_OBJECT_JSON_FIELD).at(THEN_JSON_FIELD).asString(), equalTo(then));
    }
}
