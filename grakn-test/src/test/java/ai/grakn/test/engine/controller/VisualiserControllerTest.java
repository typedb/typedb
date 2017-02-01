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
import ai.grakn.test.EngineContext;
import ai.grakn.concept.TypeName;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.graphs.TestGraph.loadFromFile;
import static ai.grakn.util.REST.Request.GRAQL_CONTENTTYPE;
import static ai.grakn.util.REST.Request.HAL_CONTENTTYPE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.QUERY_FIELD;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.with;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VisualiserControllerTest {
    private static GraknGraph graph;

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @BeforeClass
    public static void setUp() throws Exception{
        graph = engine.graphWithNewKeyspace();

        loadFromFile(graph, "genealogy/ontology.gql");
        loadFromFile(graph, "genealogy/data.gql");
    }

    @Test
    public void testOntologyRetrieval(){
        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .get(REST.WebPath.GRAPH_ONTOLOGY_URI)
                .then().statusCode(200).extract().response().andReturn();

        Map<String, Json> resultArray = Json.read(response.getBody().asString()).asJsonMap();

        assertEquals(4,resultArray.size());
        assertEquals(9,resultArray.get("entities").asList().size());
        assertEquals(true, resultArray.get("entities").asList().contains("christening"));
        assertEquals(35,resultArray.get("roles").asList().size());
        assertEquals(true, resultArray.get("roles").asList().contains("daughter-in-law"));
        assertEquals(18,resultArray.get("resources").asList().size());
        assertEquals(true, resultArray.get("resources").asList().contains("name"));
        assertEquals(10,resultArray.get("relations").asList().size());
        assertEquals(true, resultArray.get("relations").asList().contains("relatives"));
    }

    @Test
    public void testPersonViaMatchAndId(){

        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .queryParam(QUERY_FIELD, "match $x isa person;")
                .accept(HAL_CONTENTTYPE)
                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
                .then().statusCode(200).extract().response().andReturn();

        Json resultArray = Json.read(response.getBody().asString());
        assertEquals(60,resultArray.asJsonList().size());

        Json firstPerson = resultArray.at(0);
        String firstPersonId = firstPerson.at("_id").asString();

        checkHALStructureOfPerson(resultArray.at(0),firstPersonId);

        Json samePerson = retrieveConceptById(firstPersonId);

        assertEquals(firstPerson.at("_id"),samePerson.at("_id"));
    }

    private void checkHALStructureOfPerson(Json person, String id){
        assertEquals(person.at("_type").asString(), "person");
        assertEquals(person.at("_id").getValue(),id);
        assertEquals(person.at("_baseType").asString(), Schema.BaseType.ENTITY.name());

        //check we are always attaching the correct keyspace
        String hrefLink = person.at("_links").at("self").at("href").asString();
        Assert.assertEquals(true,hrefLink.substring(hrefLink.indexOf("keyspace")+9).equals(graph.getKeyspace()));

        Json embeddedType = person
                .at("_embedded")
                .at("isa").at(0);
        assertEquals(Schema.BaseType.ENTITY_TYPE.name(), embeddedType.at("_baseType").asString());
    }

    private void checkHALStructureOfPersonWithoutEmbedded(Json person, String id){

        assertEquals(person.at("_type").asString(), "person");
        assertEquals(person.at("_id").getValue(),id);
        assertEquals(person.at("_baseType").asString(), Schema.BaseType.ENTITY.name());

        //check we are always attaching the correct keyspace
        String hrefLink = person.at("_links").at("self").at("href").asString();
        Assert.assertEquals(true,hrefLink.substring(hrefLink.indexOf("keyspace")+9).equals(graph.getKeyspace()));
    }

    private Json retrieveConceptById(String id){
        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .get(REST.WebPath.CONCEPT_BY_ID_URI + id)
                .then().statusCode(200).extract().response().andReturn();

        Json samePerson =Json.read(response.getBody().asString());
        checkHALStructureOfPersonWithoutEmbedded(samePerson,id);

        return samePerson;
    }


    @Test
    public void notExistingID() {
        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .get(REST.WebPath.CONCEPT_BY_ID_URI + "6573gehjiok")
                .then().statusCode(500).extract().response().andReturn();
        String  message = response.getBody().asString();
        assertTrue(message.contains("No concept with ID [6573gehjiok] exists in keyspace"));
    }

    @Test
    public void graqlContentTypeTest(){
        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .queryParam(QUERY_FIELD, "match $x isa person;")
                .accept(GRAQL_CONTENTTYPE)
                .get(REST.WebPath.GRAPH_MATCH_QUERY_URI)
                .then().statusCode(200).extract().response().andReturn();
        String graql = response.getBody().asString();
        assertEquals(true,graql.contains("isa person"));
    }

    @Test
    public void syntacticallyWrongMatchQuery() {
        Response response = get(REST.WebPath.GRAPH_MATCH_QUERY_URI+"?keyspace="+graph.getKeyspace()+"&query=match ersouiuiwne is ieeui;").then().statusCode(500).extract().response().andReturn();
        assertEquals(true,response.getBody().asString().contains("syntax error at line 1"));
    }

    @Test
    public void getTypeByID() {
        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .get(REST.WebPath.CONCEPT_BY_ID_URI +graph.getType(TypeName.of("person")).getId().getValue())
                .then().statusCode(200).extract().response().andReturn();
        Json message = Json.read(response.getBody().asString());

        //TODO:maybe change person to proper id? and add  _nameType property
        assertEquals(message.at("_id").asString(),"person");
        assertEquals(Schema.BaseType.ENTITY_TYPE.name(), message.at("_baseType").asString());
        assertEquals(message.at("_links").at("self").at("href").asString(),"/graph/concept/"+graph.getType(TypeName.of("person")).getId().getValue()+"?keyspace="+graph.getKeyspace());
        assertEquals(60,message.at("_embedded").at("isa").asJsonList().size());
    }
    
}
