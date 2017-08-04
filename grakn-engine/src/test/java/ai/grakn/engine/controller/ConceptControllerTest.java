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
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.engine.GraknEngineStatus;
import static ai.grakn.engine.controller.Utilities.exception;
import static ai.grakn.engine.controller.Utilities.stringResponse;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALConceptData;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import ai.grakn.util.REST;
import static ai.grakn.util.REST.Request.Concept.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.Graql.IDENTIFIER;
import com.codahale.metrics.MetricRegistry;
import static com.jayway.restassured.RestAssured.with;
import com.jayway.restassured.response.Response;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConceptControllerTest {

    private static GraknGraph mockGraph;
    private static EngineGraknGraphFactory mockFactory = mock(EngineGraknGraphFactory.class);

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @ClassRule
    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        MetricRegistry metricRegistry = new MetricRegistry();
        new SystemController(mockFactory, spark, new GraknEngineStatus(), metricRegistry);
        new ConceptController(mockFactory, spark, metricRegistry);
    });

    @Before
    public void setupMock(){
        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("randomKeyspace");
        when(mockGraph.getConcept(any())).thenAnswer(invocation ->
                graphContext.graph().getConcept(invocation.getArgument(0)));

        when(mockFactory.getGraph(mockGraph.getKeyspace(), GraknTxType.READ)).thenReturn(mockGraph);
    }


    @Test
    public void gettingConceptById_ResponseStatusIs200(){
        Response response = sendRequest(graphContext.graph().getEntityType("movie"), 0);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void gettingConceptByIdWithNoAcceptType_ResponseContentTypeIsHAL() {
        Concept concept = graphContext.graph().getEntityType("movie");

        Response response = with()
                .queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(IDENTIFIER, concept.getId().getValue())
                .get(REST.WebPath.Concept.CONCEPT + concept.getId());

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void gettingConceptByIdWithHAlAcceptType_ResponseContentTypeIsHAL(){
        Concept concept = graphContext.graph().getEntityType("movie");

        Response response = with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .accept(APPLICATION_HAL)
                .get(REST.WebPath.Concept.CONCEPT + concept.getId());

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void gettingConceptByIdWithInvalidAcceptType_ResponseStatusIs406(){
        Concept concept = graphContext.graph().getEntityType("movie");

        Response response = with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(IDENTIFIER, concept.getId().getValue())
                .accept("invalid")
                .get(REST.WebPath.Concept.CONCEPT + concept.getId());

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(UNSUPPORTED_CONTENT_TYPE.getMessage("invalid")));
    }

    @Test
    @Ignore //TODO Figure out how to properly check the Json objects
    public void gettingInstanceElementById_ConceptIdIsReturnedWithCorrectHAL(){
        Concept concept = graphContext.graph().getEntityType("movie").instances().iterator().next();

        Response response = sendRequest(concept, 1);

        String expectedResponse = renderHALConceptData(concept, 1, "randomKeyspace", 0, 1);
        assertThat(stringResponse(response), equalTo(expectedResponse));
    }

    @Test
    @Ignore //TODO Figure out how to properly check the Json objects
    public void gettingOntologyElementById_ConceptIdIsReturnedWithCorrectHAL(){
        Concept concept = graphContext.graph().getEntityType("movie");

        Response response = sendRequest(concept, 1);

        String expectedResponse = renderHALConceptData(concept, 1, "randomKeyspace", 0, 1);
        assertThat(stringResponse(response), equalTo(expectedResponse));
    }

    @Test
    @Ignore //TODO Figure out how should work and how to test
    public void gettingConceptByIdWithNumberEmbedded2_Only2EmbeddedConceptsReturned(){
        fail();
    }

    @Test
    @Ignore //TODO Figure out how should work and how to test
    public void gettingConceptByIdWithOffset1_SecondConceptIsReturned(){
        fail();
    }

    @Test
    public void gettingNonExistingElementById_ResponseStatusIs404(){
        Response response = with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(IDENTIFIER, "invalid")
                .accept(APPLICATION_HAL)
                .get(REST.WebPath.Concept.CONCEPT + "blah");

        assertThat(response.statusCode(), equalTo(404));
    }

    private Response sendRequest(Concept concept, int numberEmbeddedComponents){
        return with().queryParam(KEYSPACE, mockGraph.getKeyspace())
                .queryParam(LIMIT_EMBEDDED, numberEmbeddedComponents)
                .accept(APPLICATION_HAL)
                .get(REST.WebPath.Concept.CONCEPT + concept.getId().getValue());
    }
}
