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

package ai.grakn.test.engine.controller;

import ai.grakn.GraknGraph;
import ai.grakn.engine.controller.VisualiserController;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.GraphContext;
import ai.grakn.test.engine.controller.TasksControllerTest.JsonMapper;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import spark.Service;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.renderHALArrayData;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_PARAMETERS;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.WebPath.Graph.GRAQL;
import static com.jayway.restassured.RestAssured.with;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VisualiserControllerTest {

    private static final String HOST = "localhost";
    private static final int PORT = 4567;
    private static Service spark;

    private static GraknGraph mockGraph;
    private static QueryBuilder mockQueryBuilder;
    private static EngineGraknGraphFactory mockFactory;

    private static final JsonMapper jsonMapper = new JsonMapper();

    @ClassRule
    public static GraphContext graphContext = GraphContext.preLoad(MovieGraph.get());

    @BeforeClass
    public static void setup(){
        spark = Service.ignite();
        configureSpark(spark, PORT);

        mockFactory = mock(EngineGraknGraphFactory.class);

        new VisualiserController(mockFactory, spark);

        spark.awaitInitialization();
    }

    @Before
    public void setupMock(){
        mockQueryBuilder = mock(QueryBuilder.class);

        when(mockQueryBuilder.materialise(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.infer(anyBoolean())).thenReturn(mockQueryBuilder);
        when(mockQueryBuilder.parse(any()))
                .thenAnswer(invocation -> graphContext.graph().graql().parse(invocation.getArgument(0)));

        mockGraph = mock(GraknGraph.class, RETURNS_DEEP_STUBS);

        when(mockGraph.getKeyspace()).thenReturn("keyspace");
        when(mockGraph.graql()).thenReturn(mockQueryBuilder);

        when(mockFactory.getGraph(mockGraph.getKeyspace())).thenReturn(mockGraph);
    }

    //TODO aggregate
    //TODO compute
    //TODO concept api
    //TODO ontology api
    //TODO json printing functionality
    //TODO string constants
    //TODO documentation

    @AfterClass
    public static void shutdown(){
        spark.stop();
    }

    @Test
    public void whenSendingGraqlMatch_QueryIsExecuted(){
        String query = "match $x isa person;";
        sendMatch(query, APPLICATION_TEXT);

        verify(mockGraph.graql().materialise(anyBoolean()).infer(anyBoolean()))
                .parse(argThat(argument -> argument.equals(query)));
    }

    @Test
    public void whenSendingGraqlMatch_ResponseStatusIs200(){
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void whenSendingGraqlMatchWithInvalidAcceptType_ResponseStatusIs406(){
        Response response = sendMatch("invalid");

        assertThat(response.statusCode(), equalTo(406));
        assertThat(exception(response), containsString(UNSUPPORTED_CONTENT_TYPE.getMessage("invalid")));
    }

    @Test
    public void whenSendingGraqlMatchWithNoKeyspace_ResponseStatusIs400(){
        Response response = with().get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage("keyspace")));
    }

    @Test
    public void whenSendingGraqlMatchWithNoQuery_ResponseStatusIs400(){
        Response response = with()
                .queryParam("keyspace", mockGraph.getKeyspace())
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage("query")));
    }

    @Test
    public void whenSendingGraqlMatchWithReasonerMissing_ResponseStatusIs400(){
        Response response = with().queryParam("keyspace", mockGraph.getKeyspace())
                .queryParam("query", "match $x isa person;")
                .queryParam("materialise", true)
                .accept(APPLICATION_TEXT)
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage("materialise")));
    }

    @Test
    public void whenSendingGraqlMatchWithReasonerTrue_ReasonerIsOnWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, true, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> arg));
    }

    @Test
    public void whenSendingGraqlMatchWithReasonerFalse_ReasonerIsOffWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).infer(booleanThat(arg -> !arg));
    }

    @Test
    public void whenSendingGraqlMatchWithMaterialiseMissing_ResponseStatusIs400(){
        Response response = with().queryParam("keyspace", mockGraph.getKeyspace())
                .queryParam("query", "match $x isa person;")
                .accept(APPLICATION_TEXT)
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));

        assertThat(response.statusCode(), equalTo(400));
        assertThat(exception(response), containsString(MISSING_MANDATORY_PARAMETERS.getMessage("infer")));
    }

    @Test
    public void whenSendingGraqlMatchWithMaterialiseFalse_MaterialiseIsOffWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, false, false, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> !arg));
    }

    @Test
    public void whenSendingGraqlMatchWithMaterialiseTrue_MaterialiseIsOnWhenExecuting(){
        sendMatch("match $x isa person;", APPLICATION_TEXT, false, true, 0);

        verify(mockQueryBuilder).materialise(booleanThat(arg -> arg));
    }

    @Test
    public void whenSendingGraqlMatchWithHALTypeAndNumberEmbedded1_ResponsesContainAtMost1Concept(){
        Response response =
                sendMatch("match $x isa person;", APPLICATION_HAL, false, true,1);

        jsonResponse(response).asJsonList().forEach(e -> {
             assertThat(e.asJsonMap().get("_embedded").asJsonMap().size(), lessThanOrEqualTo(1));
        });
    }

    @Test
    public void whenSendingGraqlMatchWithHALType_ResponseIsCorrectHal(){
        String queryString = "match $x isa movie;";
        Response response = sendMatch(queryString, APPLICATION_HAL);

        Json expectedResponse = renderHALArrayData(
                graphContext.graph().graql().parse(queryString), 0, -1);
        assertThat(jsonResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void whenSendingGraqlMatchWithHALType_ResponseContentTypeIsHal(){
        Response response = sendMatch(APPLICATION_HAL);

        assertThat(response.contentType(), equalTo(APPLICATION_HAL));
    }

    @Test
    public void whenSendingGraqlMatchWithHALTypeAndEmptyResponse_ResponseIsEmptyJsonArray(){
        Response response = sendMatch("match $x isa runtime;", APPLICATION_HAL);

        assertThat(jsonResponse(response), equalTo(Json.array()));
    }

    @Test
    public void whenSendingGraqlMatchWithHALType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_HAL);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void whenSendingGraqlMatchWithTextType_ResponseContentTypeIsGraql(){
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    @Test
    public void whenSendingGraqlMatchWithTextType_ResponseIsCorrectGraql(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_TEXT);

        String expectedResponse = graphContext.graph().graql().parse(query)
                .resultsString(Printers.graql())
                .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                .collect(joining("\n"));
        assertThat(stringResponse(response), equalTo(expectedResponse));
    }

    @Test
    public void whenSendingGraqlMatchWithTextType_ResponseContainsOriginalQuery(){
        String query = "match $x isa person;";
        Response response = sendMatch(APPLICATION_TEXT);

        assertThat(originalQuery(response), equalTo(query));
    }

    @Test
    public void whenSendingGraqlMatchWithTextTypeAndEmptyResponse_ResponseIsEmptyString(){
        Response response = sendMatch("match $x isa \"runtime\";", APPLICATION_TEXT);

        assertThat(stringResponse(response), isEmptyString());
    }

    @Test
    public void whenSendingMalformedGraqlMatch_ResponseStatusCodeIs400(){
        String query = "match $x isa ;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void whenSendingMalformedGraqlMatch_ResponseExceptionContainsSyntaxError(){
        String query = "match $x isa ;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("syntax error"));
    }

    // This is so that the word "Exception" does not appear on the dashboard when there is a syntax error
    @Test
    public void whenSendingMalformedGraqlMatch_ResponseExceptionDoesNotContainWordException(){
        String query = "match $x isa ;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(exception(response), not(containsString("Exception")));
    }

    @Test
    public void whenSendingGraqlInsert_ResponseExceptionContainsReadOnlyAllowedMessage(){
        String query = "insert $x isa person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(exception(response), containsString("read-only"));
    }

    @Test
    public void whenSendingGraqlInsert_ResponseStatusCodeIs405NotSupported(){
        String query = "insert $x isa person;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void whenSendingGraqlAggregateWithHalType_ResponseStatusCodeIs406(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_HAL);

        assertThat(response.statusCode(), equalTo(406));
    }

    @Test
    public void whenSendingGraqlAggregateWithTextType_ResponseStatusIs200(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void whenSendingGraqlAggregateWithTextType_ResponseIsCorrect(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        int numberPeople = graphContext.graph().getEntityType("person").instances().size();
        assertThat(stringResponse(response), equalTo(Integer.toString(numberPeople)));
    }

    @Test
    public void whenSendingGraqlAggregateWithTextType_ResponseTypeIsTextType(){
        String query = "match $x isa person; aggregate count;";
        Response response = sendMatch(query, APPLICATION_TEXT);

        assertThat(response.contentType(), equalTo(APPLICATION_TEXT));
    }

    private Response sendMatch(String acceptType){
        return sendMatch("match $x isa person;", acceptType, false, false, Integer.MAX_VALUE);
    }

    private Response sendMatch(String match, String acceptType){
        return sendMatch(match, acceptType, false, false, Integer.MAX_VALUE);
    }

    private Response sendMatch(String match, String acceptType, boolean reasonser,
                               boolean materialise, int numberEmbeddedComponents){
        return with().queryParam("keyspace", mockGraph.getKeyspace())
                .queryParam("query", match)
                .queryParam("infer", reasonser)
                .queryParam("materialise", materialise)
                .queryParam("numberEmbeddedComponents", numberEmbeddedComponents)
                .accept(acceptType)
                .get(String.format("http://%s:%s%s", HOST, PORT, GRAQL));
    }

    private static String exception(Response response){
        return response.getBody().as(Json.class, jsonMapper).at("exception").asString();
    }

    private static String stringResponse(Response response){
        return response.getBody().as(Json.class, jsonMapper).at("response").asString();
    }

    private static Json jsonResponse(Response response){
        return response.getBody().as(Json.class, jsonMapper).at("response");
    }

    private static String originalQuery(Response response){
        return response.getBody().as(Json.class, jsonMapper).at("originalQuery").asString();
    }
//
//    @Test
//    public void testOntologyRetrieval() {
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .get(REST.WebPath.GRAPH_ONTOLOGY_URI)
//                .then().statusCode(200).extract().response().andReturn();
//
//        Map<String, Json> resultArray = Json.read(response.getBody().asString()).asJsonMap();
//
//        assertEquals(4, resultArray.size());
//        assertEquals(9, resultArray.get("entities").asList().size());
//        assertEquals(true, resultArray.get("entities").asList().contains("christening"));
//        assertEquals(35, resultArray.get("roles").asList().size());
//        assertEquals(true, resultArray.get("roles").asList().contains("daughter-in-law"));
//        assertEquals(18, resultArray.get("resources").asList().size());
//        assertEquals(true, resultArray.get("resources").asList().contains("name"));
//        assertEquals(10, resultArray.get("relations").asList().size());
//        assertEquals(true, resultArray.get("relations").asList().contains("relatives"));
//    }
//
//    @Test
//    public void testPersonViaMatchAndId() {
//
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .queryParam(QUERY_FIELD, "match $x isa person;")
//                .accept(HAL_CONTENTTYPE)
//                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
//                .then().statusCode(200).extract().response().andReturn();
//
//        Json resultArray = Json.read(response.getBody().asString());
//        assertEquals(60, resultArray.asJsonList().size());
//
//        Json firstPerson = resultArray.at(0);
//        String firstPersonId = firstPerson.at("_id").asString();
//
//        checkHALStructureOfPerson(resultArray.at(0), firstPersonId);
//
//        Json samePerson = retrieveConceptById(firstPersonId);
//
//
//        assertEquals(firstPerson.at("_id"), samePerson.at("_id"));
//    }
//
//    //Test that we don't get an error 500 when asking for relationships without specifying their types
//    @Test
//    public void testGeneratedRelationshipsWithoutType() {
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .queryParam(QUERY_FIELD, "match (protagonist: $x, happening: $y); limit 10;")
//                .accept(HAL_CONTENTTYPE)
//                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
//                .then().statusCode(200).extract().response().andReturn();
//
//        Json resultArray = Json.read(response.getBody().asString());
//
//        //Asking for 10 relations that have 2 role-players each will give us an array of 20 nodes to show in the visualiser.
//        assertEquals(20, resultArray.asJsonList().size());
//
//        //Check we don't put an empty "isa" as a relationship type
//        resultArray.asJsonList().forEach(rolePlayer -> {
//            Map<String, Json> mappedEmbedded = rolePlayer.at("_embedded").asJsonMap();
//            //Loop through map containing Json arrays associated to embedded key
//            mappedEmbedded.keySet().forEach(key -> {
//                //Foreach element of the json array we check if it is a generated relation
//                mappedEmbedded.get(key).asJsonList().forEach(element -> {
//                    if (element.at("_baseType").asString().equals("generated-relation")) {
//                        assertFalse(element.at("_links").at("self").at("href").asString().contains("isa"));
//                    }
//                });
//
//            });
//        });
//    }
//
//    //Check that a generated relation _id contains role players' IDs.
//    @Test
//    public void checkGeneratedRelationId() {
//        Response firstResponse = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .queryParam(QUERY_FIELD, "match ($x, $y) isa event-protagonist; limit 1;")
//                .accept(HAL_CONTENTTYPE)
//                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
//                .then().statusCode(200).extract().response().andReturn();
//
//        Json resultArray = Json.read(firstResponse.getBody().asString());
//        Json firstRolePlayer = resultArray.at(0);
//        String firstRolePlayerId = firstRolePlayer.at("_id").asString();
//
//        Json secondRolePlayer = resultArray.at(1);
//        String secondRolePlayerId = secondRolePlayer.at("_id").asString();
//
//        if (firstRolePlayerId.compareTo(secondRolePlayerId) < 0) {
//            String tempString = secondRolePlayerId;
//            secondRolePlayerId = firstRolePlayerId;
//            firstRolePlayerId = tempString;
//        }
//
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .queryParam(QUERY_FIELD, "match $x id \"" + firstRolePlayerId + "\"; $y id \"" + secondRolePlayerId + "\"; ($x,$y); limit 1;")
//                .accept(HAL_CONTENTTYPE)
//                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
//                .then().statusCode(200).extract().response().andReturn();
//
//        Json secondResultArray = Json.read(response.getBody().asString());
//
//        //Asking for 1 relation that have 2 role-players each will give us an array of 2 nodes to show in the visualiser.
//        assertEquals(2, secondResultArray.asJsonList().size());
//
//        // Final vars for lambda
//        final String firstId=firstRolePlayerId;
//        final String secondId=secondRolePlayerId;
//
//        secondResultArray.asJsonList().forEach(rolePlayer -> {
//            Map<String, Json> mappedEmbedded = rolePlayer.at("_embedded").asJsonMap();
//            //Loop through map containing Json arrays associated to embedded key
//            mappedEmbedded.keySet().forEach(key -> {
//                //Foreach element of the json array we check if it is a generated relation
//                mappedEmbedded.get(key).asJsonList().forEach(element -> {
//                    if (element.at("_baseType").asString().equals("generated-relation")) {
//                        assertEquals(element.at("_id").getValue(), "temp-assertion-" + firstId + secondId);
//                    }
//                });
//
//            });
//        });
//    }
//
//
//    //Check that a generated relation _id contains role players' IDs.
//    @Test
//    public void testOntologyConceptHAL() {
//
//        Type protagonistType = graph.getType(TypeName.of("protagonist"));
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .get(REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI + protagonistType.getId().getValue())
//                .then().statusCode(200).extract().response().andReturn();
//        Json protagonist = Json.read(response.getBody().asString());
//
//        assertEquals("protagonist",protagonist.at("_name").getValue());
//        assertEquals(protagonistType.getId().getValue(),protagonist.at("_id").getValue());
//        assertEquals("event-protagonist",protagonist.at("_embedded").at("has-role").at(0).at("_name").getValue());
//    }
//
//    private void checkHALStructureOfPerson(Json person, String id) {
//        assertEquals(person.at("_type").asString(), "person");
//        assertEquals(person.at("_id").getValue(), id);
//        assertEquals(person.at("_baseType").asString(), Schema.BaseType.ENTITY.name());
//
//        //check we are always attaching the correct keyspace
//        String hrefLink = person.at("_links").at("self").at("href").asString();
//        Assert.assertEquals(true, hrefLink.contains(graph.getKeyspace()));
//
//        Json embeddedType = person
//                .at("_embedded")
//                .at("isa").at(0);
//        assertEquals(Schema.BaseType.ENTITY_TYPE.name(), embeddedType.at("_baseType").asString());
//    }
//
//    private void checkHALStructureOfPersonWithoutEmbedded(Json person, String id) {
//
//        assertEquals(person.at("_type").asString(), "person");
//        assertEquals(person.at("_id").getValue(), id);
//        assertEquals(person.at("_baseType").asString(), Schema.BaseType.ENTITY.name());
//
//        //check we are always attaching the correct keyspace
//        String hrefLink = person.at("_links").at("self").at("href").asString();
//        Assert.assertEquals(true, hrefLink.contains(graph.getKeyspace()));
//    }
//
//    private Json retrieveConceptById(String id) {
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .get(REST.WebPath.CONCEPT_BY_ID_URI + id)
//                .then().statusCode(200).extract().response().andReturn();
//
//        Json samePerson = Json.read(response.getBody().asString());
//        checkHALStructureOfPersonWithoutEmbedded(samePerson, id);
//
//        return samePerson;
//    }
//
//    @Test
//    public void retrieveConceptByIdWithOffsetAndLimit() {
//        Type personType = graph.getType(TypeName.of("person"));
//
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .queryParam("offset", "0")
//                .queryParam("limit", "3")
//                .get(REST.WebPath.CONCEPT_BY_ID_URI + personType.getId())
//                .then().statusCode(200).extract().response().andReturn();
//        Json person = Json.read(response.getBody().asString());
//
//        String hrefLink = person.at("_links").at("self").at("href").asString();
//        Assert.assertEquals(true, hrefLink.contains("offset=3&limit=3"));
//        Assert.assertEquals(3, person.at("_embedded").at("isa").asList().size());
//        Assert.assertEquals(true, person.at("_embedded").at("isa").asJsonList().get(0).at("_links").at("self").at("href").asString().contains("offset=0&limit=3"));
//
//    }
//
//
//    @Test
//    public void notExistingID() {
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .get(REST.WebPath.CONCEPT_BY_ID_URI + "6573gehjiok")
//                .then().statusCode(500).extract().response().andReturn();
//        String message = response.getBody().asString();
//        assertTrue(message.contains("No concept with ID [6573gehjiok] exists in keyspace"));
//    }
//
//    @Test
//    public void graqlContentTypeTest() {
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .queryParam(QUERY_FIELD, "match $x isa person;")
//                .accept(GRAQL_CONTENTTYPE)
//                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
//                .then().statusCode(200).extract().response().andReturn();
//        String graql = response.getBody().asString();
//        assertEquals(true, graql.contains("isa person"));
//    }
//
//    @Test
//    public void syntacticallyWrongMatchQuery() {
//        get(REST.WebPath.GRAPH_MATCH_QUERY_URI + "?keyspace=" + graph.getKeyspace() + "&query=match ersouiuiwne is ieeui;")
//                .then().statusCode(400).extract().response()
//                .then().assertThat().body(containsString("syntax error at line 1"));
//    }
//
//    @Test
//    public void whenSendingAnInvalidQuery_TheResponseShouldNotContainTheExceptionType() {
//        get(REST.WebPath.GRAPH_MATCH_QUERY_URI + "?keyspace=" + graph.getKeyspace() + "&query=match ersouiuiwne is ieeui;")
//                .then().statusCode(400).extract().response()
//                .then().assertThat().body(not(containsString("Exception")));
//    }
//
//    @Test
//    public void getTypeByID() {
//        Type personType = graph.getType(TypeName.of("person"));
//        Response response = with()
//                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
//                .get(REST.WebPath.CONCEPT_BY_ID_URI + personType.getId().getValue())
//                .then().statusCode(200).extract().response().andReturn();
//        Json message = Json.read(response.getBody().asString());
//
//        assertEquals(message.at("_id").asString(), personType.getId().getValue());
//        assertEquals(message.at("_name").asString(), "person");
//        assertEquals(Schema.BaseType.ENTITY_TYPE.name(), message.at("_baseType").asString());
//        assertEquals(message.at("_links").at("self").at("href").asString(), "/graph/concept/" + graph.getType(TypeName.of("person")).getId().getValue() + "?keyspace=" + graph.getKeyspace()+"&offset=0");
//        assertEquals(60, message.at("_embedded").at("isa").asJsonList().size());
//    }

}
