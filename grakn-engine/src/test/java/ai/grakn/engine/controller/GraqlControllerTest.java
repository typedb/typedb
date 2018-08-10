/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine.controller;

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStoreFake;
import ai.grakn.engine.attribute.uniqueness.AttributeDeduplicator;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.engine.controller.util.JsonConceptBuilder;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.printer.JacksonPrinter;
import ai.grakn.graql.Query;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.REST.Request.Graql.ALLOW_MULTIPLE_QUERIES;
import static ai.grakn.util.REST.Request.Graql.DEFINE_ALL_VARS;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraqlControllerTest {
    private static final LockProvider mockLockProvider = mock(LockProvider.class);
    private static final Lock mockLock = mock(Lock.class);
    private static final JacksonPrinter printer = JacksonPrinter.create();

    private Response sendQuery(String query) {
        return sendQuery(query, APPLICATION_JSON);
    }

    private Response sendQuery(String query, String acceptType) {
        return sendQuery(query, acceptType, true, sampleKB.tx().keyspace().getValue(), false);
    }

    private Response sendQuery(String query,
                               String acceptType,
                               boolean reasoner,
                               String keyspace, boolean multi) {
        return RestAssured.with()
                .body(query)
                .queryParam(EXECUTE_WITH_INFERENCE, reasoner)
                .queryParam(ALLOW_MULTIPLE_QUERIES, multi)
                .queryParam(DEFINE_ALL_VARS, true)
                .accept(acceptType)
                .post(REST.resolveTemplate(REST.WebPath.KEYSPACE_GRAQL, keyspace));
    }

    private Response sendExplanationQuery(String query, String keyspace) {
        return RestAssured.with()
                .queryParam(QUERY, query)
                .accept(APPLICATION_JSON)
                .get(REST.resolveTemplate(REST.WebPath.KEYSPACE_EXPLAIN, keyspace));
    }


    public static SampleKBContext sampleKB = MovieKB.context();

    public static SampleKBContext genealogyKB = GenealogyKB.context();

    public static SparkContext sparkContext = SparkContext.withControllers((spark, config) -> {
        EngineGraknTxFactory factory = EngineGraknTxFactory
                .create(mockLockProvider, GraknConfig.create(), KeyspaceStoreFake.of());
        factory.keyspaceStore().loadSystemSchema();
        new GraqlController(factory, mock(AttributeDeduplicator.class), printer, new MetricRegistry()).start(spark);
    });

    @ClassRule
    public static final RuleChain chain = RuleChain.emptyRuleChain().around(sampleKB).around(genealogyKB).around(sparkContext);

    @Before
    public void setUp() {
        when(mockLockProvider.getLock(any())).thenReturn(mockLock);
    }

    @Test
    public void whenRunningGetQuery_EnsureAllInstancesAreReturned() {
        Set<Concept> expectedInstances = sampleKB.tx().getEntityType("movie").instances().
                map(ConceptBuilder::<Concept>build).collect(Collectors.toSet());

        List<Json> json = Json.read(sendQuery("match $x isa movie; get;", APPLICATION_JSON).body().asString()).
                asJsonList();

        Set<Concept> instances = json.stream().
                map(j -> j.at("x")).
                map(JsonConceptBuilder::<Concept>build).
                collect(Collectors.toSet());

        assertEquals(expectedInstances, instances);
    }

    @Test
    public void whenAcceptTypeIsText_EnsureEveryResultIsOnANewLine() {
        String response  = sendQuery("match $x isa movie; limit 7; get;", APPLICATION_TEXT).body().asString();
        assertEquals(6, StringUtils.countMatches(response, "\n"));
    }

    //TODO: This test should be improved
    @Test
    public void whenExecutingExplainQuery_responseIsValid() {
        String keyspace = genealogyKB.tx().keyspace().getValue();
        Response response = sendExplanationQuery("match ($x,$y) isa cousins; offset 0; limit 1; get;", keyspace);
        List<Json> json = Json.read(response.body().asString()).asJsonList();
        assertEquals(3, json.size());
    }

    @Test
    public void testInsertQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        try {
            resp.then().statusCode(200);
            Assert.assertFalse(resp.jsonPath().getList(".").isEmpty());
        } finally {
            ConceptId id = ConceptId.of(resp.jsonPath().getList("x.id").get(0).toString());
            sampleKB.rollback();
            sampleKB.tx().graql().match(var("x").id(id)).delete("x").execute();
        }
    }

    @Test
    public void whenRunningInsertQuery_JsonResponseCanBeMappedToJavaObject() {
        Concept expectedConcept = ConceptBuilder.build(sampleKB.tx().getEntityType("movie"));
        Json json = Json.read(sendQuery("insert $x label movie;", APPLICATION_JSON).body().asString()).
                asJsonList().iterator().next().at("x");
        Concept concept = JsonConceptBuilder.build(json);
        assertEquals(expectedConcept, concept);
    }

    @Test
    public void whenRunningMultiIdempotentInsertQuery_JsonResponseIsTheSameAsJavaGraql() {
        String single = "insert $x label movie;";
        String queryString = single + "\n" + single;
        Response resp = sendQuery(queryString, APPLICATION_JSON, true, sampleKB.tx().keyspace().getValue(), true);
        resp.then().statusCode(200);
        Stream<Query<?>> query = sampleKB.tx().graql().parser().parseList(queryString);
        String graqlResult = printer.toString(query.map(Query::execute).collect(
                Collectors.toList()));
        Json expected = Json.read(graqlResult);
        assertEquals(expected, Json.read(resp.body().asString()));

    }

    @Test
    public void testDeleteQuery() {
        Response resp = sendQuery("insert $x isa movie;");
        resp.then().statusCode(200);
        String id = resp.jsonPath().getList("x.id").get(0).toString();
        resp = sendQuery("match $x id \"" + id + "\"; delete $x; ");
        resp.then().statusCode(200);

        System.out.println(Json.array());
        System.out.println(resp.asString());
        assertEquals(Json.array(Json.array(id)), Json.read(resp.asString()));
    }

    @Test
    public void whenRunningAggregateQuery_JsonResponseIsTheSameAsJava() {
        assertResponseMatchesExpectedObject("match $x isa movie; aggregate count;");
    }

    @Test
    public void whenMatchingRules_ResponseStatusIs200() {
        String queryString = "match $x sub " + Schema.MetaSchema.RULE.getLabel().getValue() + "; get;";
        Response resp = sendQuery(queryString, APPLICATION_JSON);
        resp.then().statusCode(200);
    }


    @Test
    public void whenSendingGibberish_Ensure400IsReturned() {
        sendQuery(" gibberish ads a;49 agfdgdsf").then().statusCode(400);
    }

    private void assertResponseMatchesExpectedObject(String queryString) {
        Response resp = sendQuery(queryString, APPLICATION_JSON);
        assertResponseMatchesExpectedObject(resp, queryString);
    }

    private void assertResponseMatchesExpectedObject(Response resp, String queryString) {
        resp.then().statusCode(200);

        sampleKB.rollback();
        Query<?> query = sampleKB.tx().graql().parse(queryString);

        String expectedObject = printer.toString(query.execute());

        Object expected = Json.read(expectedObject);

        assertEquals(expected.toString(), resp.body().asString());
    }

}
