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

import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.test.AbstractGraphTest;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.io.Files;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import static ai.grakn.util.REST.Request.GRAQL_CONTENTTYPE;
import static ai.grakn.util.REST.Request.HAL_CONTENTTYPE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.QUERY_FIELD;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.with;
import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VisualiserControllerTest extends AbstractGraphTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setEngineUrl(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
    }

    @Before
    public void setUp() throws Exception{
        load(getFile("genealogy/ontology.gql"));
        load(getFile("genealogy/data.gql"));
    }


    protected static File getFile(String fileName){
        return new File(VisualiserControllerTest.class.getResource(fileName).getPath());
    }


    private void load(File file) {
        try {
            graph.graql()
                    .parse(Files.readLines(file, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException |GraknValidationException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testOntologyRetrieval(){
        Response response = with()
                .queryParam(KEYSPACE_PARAM, graph.getKeyspace())
                .get(REST.WebPath.GRAPH_ONTOLOGY_URI)
                .then().statusCode(200).extract().response().andReturn();

        //noinspection unchecked
        Map<String, ArrayList> resultArray = (Map<String, ArrayList>) Json.read(response.getBody().asString()).getValue();

        assertEquals(4,resultArray.size());
        assertEquals(9,resultArray.get("entities").size());
        assertEquals(35,resultArray.get("roles").size());
        assertEquals(19,resultArray.get("resources").size());
        assertEquals(10,resultArray.get("relations").size());
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
        assertEquals(2,resultArray.asJsonList().size());
        checkHALStructureOfPerson(resultArray.at(0));

        Json firstPerson = resultArray.at(0);
        Json samePerson = retrieveConceptById(firstPerson.at("_id").asString());

        assertEquals(firstPerson.at("_id"),samePerson.at("_id"));


    }

    private void checkHALStructureOfPerson(Json person){
        assertEquals(person.at("_type").asString(), "person");
        assertNotNull(person.at("_id"));
        assertEquals(person.at("_baseType").asString(), Schema.BaseType.ENTITY.name());

        //check we are always attaching the correct keyspace
        String hrefLink = person.at("_links").at("self").at("href").asString();
        Assert.assertEquals(true,hrefLink.substring(hrefLink.indexOf("keyspace")+9).equals(graph.getKeyspace()));

        Json embeddedType = person
                .at("_embedded")
                .at("isa").at(0);
        assertEquals(Schema.BaseType.ENTITY_TYPE.name(), embeddedType.at("_baseType").asString());
    }

    private void checkHALStructureOfPersonWithoutEmbedded(Json person){

        assertEquals(person.at("_type").asString(), "person");
        assertNotNull(person.at("_id"));
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
        checkHALStructureOfPersonWithoutEmbedded(samePerson);

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
                .get(REST.WebPath.CONCEPT_BY_ID_URI +graph.getType("person").getId())
                .then().statusCode(200).extract().response().andReturn();
        Json message = Json.read(response.getBody().asString());

        //TODO:maybe change person to proper id? and add  _nameType property
        assertEquals(message.at("_id").asString(),"person");
        assertEquals(Schema.BaseType.ENTITY_TYPE.name(), message.at("_baseType").asString());
        assertEquals(message.at("_links").at("self").at("href").asString(),"/graph/concept/"+graph.getType("person").getId()+"?keyspace="+graph.getKeyspace());
        assertEquals(2,message.at("_embedded").at("isa").asJsonList().size());
    }


}
