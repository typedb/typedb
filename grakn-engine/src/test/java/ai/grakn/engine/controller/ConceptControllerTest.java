/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

/*-
 * #%L
 * grakn-engine
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.GraknKeyspaceStoreFake;
import ai.grakn.engine.controller.response.Attribute;
import ai.grakn.engine.controller.response.AttributeType;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.engine.controller.response.EmbeddedAttribute;
import ai.grakn.engine.controller.response.Entity;
import ai.grakn.engine.controller.response.EntityType;
import ai.grakn.engine.controller.response.Link;
import ai.grakn.engine.controller.response.Relationship;
import ai.grakn.engine.controller.response.RelationshipType;
import ai.grakn.engine.controller.response.Role;
import ai.grakn.engine.controller.response.RolePlayer;
import ai.grakn.engine.controller.response.Rule;
import ai.grakn.engine.controller.util.JsonConceptBuilder;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.graql.Pattern;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.REST;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.Schema.MetaSchema;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConceptControllerTest {
    private static final Lock mockLock = mock(Lock.class);
    private static final LockProvider mockLockProvider = mock(LockProvider.class);
    private static final Keyspace keyspace = SampleKBLoader.randomKeyspace();

    private static EngineGraknTxFactory factory;
    private static Role roleWrapper1;
    private static Role roleWrapper2;
    private static RelationshipType relationshipTypeWrapper;
    private static Relationship relationshipWrapper;
    private static EntityType entityTypeWrapper;
    private static EntityType entityTypeSubWrapper;
    private static Entity entityWrapper;
    private static AttributeType attributeTypeWrapper;
    private static AttributeType attributeTypeKeyWrapper;
    private static Attribute attributeWrapper1;
    private static Attribute attributeWrapper2;
    private static Rule ruleWrapper;

    private static ai.grakn.concept.Entity entity;

    public static SessionContext sessionContext = SessionContext.create();

    public static SparkContext sparkContext = SparkContext.withControllers((spark, config) -> {
        factory = EngineGraknTxFactory.create(mockLockProvider, config, GraknKeyspaceStoreFake.of());
        factory.keyspaceStore().loadSystemSchema();
        new ConceptController(factory, new MetricRegistry()).start(spark);
    });
    @ClassRule
    public static final RuleChain chain = RuleChain.emptyRuleChain().around(sessionContext).around(sparkContext);

    @BeforeClass
    public static void setUp() {
        when(mockLockProvider.getLock(any())).thenReturn(mockLock);

        //Load Silly Sample Data
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.WRITE)){
            //Build The Sample KB
            ai.grakn.concept.Role role1 = tx.putRole("My Special Role 1");
            ai.grakn.concept.Role role2 = tx.putRole("My Special Role 2");

            ai.grakn.concept.AttributeType attributeType = tx.putAttributeType("My Attribute Type", ai.grakn.concept.AttributeType.DataType.STRING);
            ai.grakn.concept.Attribute attribute1 = attributeType.putAttribute("An attribute 1");
            ai.grakn.concept.Attribute attribute2 = attributeType.putAttribute("An attribute 2");

            ai.grakn.concept.AttributeType attributeTypeKey = tx.putAttributeType("My Key Attribute Type", ai.grakn.concept.AttributeType.DataType.STRING);
            ai.grakn.concept.Attribute key = attributeTypeKey.putAttribute("An attribute Key 1");

            ai.grakn.concept.EntityType entityType = tx.putEntityType("My Special Entity Type").plays(role1).plays(role2).attribute(attributeType).key(attributeTypeKey);
            ai.grakn.concept.EntityType entityTypeSub = tx.putEntityType("My Special Sub Entity Type").sup(entityType);
            entity = entityType.addEntity().attribute(attribute1).attribute(attribute2).attribute(key);

            ai.grakn.concept.RelationshipType relationshipType = tx.putRelationshipType("My Relationship Type").relates(role1).relates(role2);
            ai.grakn.concept.Relationship relationship = relationshipType.addRelationship().addRolePlayer(role1, entity).addRolePlayer(role2, entity);

            Pattern when = tx.graql().parser().parsePattern("$x isa \"My Relationship Type\"");
            Pattern then = tx.graql().parser().parsePattern("$x isa \"My Special Entity Type\"");
            ai.grakn.concept.Rule rule = tx.putRule("My Special Snowflake of a Rule", when, then);

            //Manually Serialise The Concepts
            roleWrapper1 = ConceptBuilder.build(role1);
            roleWrapper2 = ConceptBuilder.build(role2);

            relationshipTypeWrapper = ConceptBuilder.build(relationshipType);
            relationshipWrapper = ConceptBuilder.build(relationship);

            entityTypeWrapper = ConceptBuilder.build(entityType);
            entityTypeSubWrapper = ConceptBuilder.build(entityTypeSub);
            entityWrapper = ConceptBuilder.build(entity);

            attributeTypeWrapper = ConceptBuilder.build(attributeType);
            attributeTypeKeyWrapper = ConceptBuilder.build(attributeTypeKey);
            attributeWrapper1 = ConceptBuilder.build(attribute1);
            attributeWrapper2 = ConceptBuilder.build(attribute2);

            ruleWrapper = ConceptBuilder.build(rule);

            tx.commit();
        }
    }

    @Test
    public void whenGettingRelationships_EnsureRolePlayersAreReturned() throws IOException {
        //Get Expected Relationships
        Set<RolePlayer> relationshipsExpected = new HashSet<>();
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.READ)) {
            entity.plays().forEach(role -> {
                Link roleWrapper = Link.create(role);
                entity.relationships(role).forEach(relationship -> {
                    Link relationshipWrapper = Link.create(relationship);
                    relationshipsExpected.add(RolePlayer.create(roleWrapper, relationshipWrapper));
                });
            });
        }

        //Make the request
        String request = entityWrapper.relationships().id();
        Response response = RestAssured.when().get(request);
        assertEquals(SC_OK, response.getStatusCode());

        //Check relationships are embedded
        RolePlayer[] relationships = response.jsonPath().getObject("relationships", RolePlayer[].class);
        assertThat(relationships, arrayContainingInAnyOrder(relationshipsExpected.toArray()));
    }

    @Test
    public void whenGettingSubsOfSchemaConcept_EnsureSubsAreReturned() throws IOException {
        Response response = RestAssured.when().get(entityTypeWrapper.subs().id());
        assertEquals(SC_OK, response.getStatusCode());

        EntityType[] subs = response.jsonPath().getObject("subs", EntityType[].class);
        assertThat(subs, arrayContainingInAnyOrder(entityTypeWrapper, entityTypeSubWrapper));
    }

    @Test
    public void whenGettingPlaysOfType_EnsurePlaysAreReturned() throws IOException {
        Response response = RestAssured.when().get(entityTypeWrapper.plays().id());
        assertEquals(SC_OK, response.getStatusCode());

        List<Role> plays = Arrays.asList(response.jsonPath().getObject("plays", Role[].class));
        assertEquals(4, plays.size());
        assertTrue(plays.contains(roleWrapper1));
        assertTrue(plays.contains(roleWrapper2));
    }

    @Test
    public void whenGettingAttributesOfType_EnsureAttributesAreReturned() throws IOException {
        Response response = RestAssured.when().get(entityTypeWrapper.attributes().id());
        assertEquals(SC_OK, response.getStatusCode());

        AttributeType[] attributes = response.jsonPath().getObject("attributes", AttributeType[].class);
        assertThat(attributes, arrayContainingInAnyOrder(attributeTypeWrapper, attributeTypeKeyWrapper));
    }

    @Test
    public void whenGettingKeysOfType_EnsureAttributesAreReturned()throws IOException {
        Response response = RestAssured.when().get(entityTypeWrapper.keys().id());
        assertEquals(SC_OK, response.getStatusCode());

        AttributeType[] keys = response.jsonPath().getObject("keys", AttributeType[].class);
        assertThat(keys, arrayContainingInAnyOrder(attributeTypeKeyWrapper));
    }

    @Test
    public void whenGettingAttributesOfConcept_EnsureEmbeddedAttributesAreReturned() throws IOException {
        //Get the attributes we expect
        EmbeddedAttribute[] expectedAttributes;
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.READ)){
            expectedAttributes = entity.attributes().map(EmbeddedAttribute::create).toArray(EmbeddedAttribute[]::new);
        }

        //Ask the controller for the attributes
        String request = entityWrapper.attributes().id();
        Response response = RestAssured.when().get(request);
        assertEquals(SC_OK, response.getStatusCode());

        EmbeddedAttribute[] attributes = response.jsonPath().getObject("attributes", EmbeddedAttribute[].class);
        assertThat(attributes, arrayContainingInAnyOrder(expectedAttributes));
    }

    @Test
    public void whenGettingTypesInstances_EnsureInstancesAreReturned() throws JsonProcessingException {
        //TODO: Same as below get jackson to deserialise via a child constructor
        String request = entityTypeWrapper.instances().id();
        Response response = RestAssured.when().get(request);
        assertEquals(SC_OK, response.getStatusCode());

        //Hacky way to check if instance is embedded
        String instance = new ObjectMapper().writeValueAsString(entityWrapper);
        response.then().body(containsString(instance));
    }

    @Test
    public void whenGettingConceptsByLabel_EnsureConceptsAreReturned(){
        assertConceptsReturned(REST.WebPath.KEYSPACE_ROLE, Role[].class, "roles", roleWrapper1, roleWrapper2);
        assertConceptsReturned(REST.WebPath.KEYSPACE_RULE, Rule[].class, "rules", ruleWrapper);

        //Manual Check is necessary due to collection containing mixture of concepts
        String request = REST.resolveTemplate(REST.WebPath.KEYSPACE_TYPE, keyspace.getValue());
        List<Json> response = Json.read(RestAssured.when().get(request).body().asString()).at("types").asJsonList();
        Set<Concept> types = response.stream().map(JsonConceptBuilder::<Concept>build).collect(Collectors.toSet());

        assertTrue(String.format("Type {$s} missing from response", relationshipTypeWrapper), types.contains(relationshipTypeWrapper));
        assertTrue(String.format("Type {$s} missing from response", attributeTypeWrapper), types.contains(attributeTypeWrapper));
        assertTrue(String.format("Type {$s} missing from response", entityTypeWrapper), types.contains(entityTypeWrapper));
    }

    private static void assertConceptsReturned(
            String path, Class<? extends Concept[]> clazz, String key, Concept ... concepts
    ){
        String request = REST.resolveTemplate(path, keyspace.getValue());

        Response response = RestAssured.when().get(request);
        assertEquals(SC_OK, response.statusCode());

        Concept[] conceptsFound = response.jsonPath().getObject(key, clazz);
        assertThat(Arrays.asList(conceptsFound), hasItems(concepts));
    }

    @Test
    public void whenGettingConceptWhichExists_ConceptIsReturned() throws IOException {
        assertExists(roleWrapper1, Role.class);
        assertExists(roleWrapper2, Role.class);
        assertExists(relationshipTypeWrapper, RelationshipType.class);
        assertExists(relationshipWrapper, Relationship.class);
        assertExists(entityTypeWrapper, EntityType.class);
        assertExists(entityWrapper, Entity.class);
        assertExists(attributeTypeWrapper, AttributeType.class);
        assertExists(attributeWrapper1, Attribute.class);
        assertExists(attributeWrapper2, Attribute.class);
        assertExists(ruleWrapper, Rule.class);
    }

    @Test
    public void whenGettingConceptByIdAndConceptDoeNotExist_EmptyResponse(){
        String request = REST.resolveTemplate(REST.WebPath.CONCEPT_ID, keyspace.getValue(), "bob's smelly uncle");
        RestAssured.when().get(request).then().statusCode(SC_NOT_FOUND);
    }

    @Test
    public void whenCallingConceptEndpointAndRequestingJSON_ReturnJSON() {
        ConceptId id;
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.READ)){
            id = tx.admin().getMetaConcept().getId();
        }

        given().accept(ContentType.JSON).pathParam("keyspace", keyspace.getValue()).pathParam("id", id.getValue())
                .when().get("/kb/{keyspace}/concept/{id}")
                .then().statusCode(SC_OK).contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingTypeEndpointAndRequestingJSON_ReturnJSON() {
        Label label = MetaSchema.ENTITY.getLabel();

        given().accept(ContentType.JSON).pathParam("keyspace", keyspace.getValue()).pathParam("label", label.getValue())
                .when().get("/kb/{keyspace}/type/{label}")
                .then().statusCode(SC_OK).contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingRoleEndpointAndRequestingJSON_ReturnJSON() {
        Label label = MetaSchema.ROLE.getLabel();

        given().accept(ContentType.JSON).pathParam("keyspace", keyspace.getValue()).pathParam("label", label.getValue())
                .when().get("/kb/{keyspace}/role/{label}")
                .then().statusCode(SC_OK).contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingRuleEndpointAndRequestingJSON_ReturnJSON() {
        Label label = MetaSchema.RULE.getLabel();

        given().accept(ContentType.JSON).pathParam("keyspace", keyspace.getValue()).pathParam("label", label.getValue())
                .when().get("/kb/{keyspace}/rule/{label}")
                .then().statusCode(SC_OK).contentType(ContentType.JSON);
    }

    @Test
    public void whenCallingTypesEndpoint_ReturnIdLinkToSelf() {
        String typesLink = "/kb/" + keyspace.getValue() + "/type";
        RestAssured.when().get(typesLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(typesLink));
    }

    @Test
    public void whenCallingRolesEndpoint_ReturnIdLinkToSelf() {
        String rolesLink = "/kb/" + keyspace.getValue() + "/role";
        RestAssured.when().get(rolesLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(rolesLink));
    }

    @Test
    public void whenCallingRulesEndpoint_ReturnIdLinkToSelf() {
        String rulesLink = "/kb/" + keyspace.getValue() + "/rule";
        RestAssured.when().get(rulesLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(rulesLink));
    }

    @Test
    public void whenCallingRelationshipsEndpoint_ReturnIdLinkToSelf() {
        ConceptId id;
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.READ)){
            id = tx.admin().getMetaConcept().instances().findAny().get().getId();
        }

        String relationshipsLink = "/kb/" + keyspace.getValue() + "/concept/" + id + "/relationships";
        RestAssured.when().get(relationshipsLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(relationshipsLink));
    }

    @Test
    public void whenCallingAttributesEndpoint_ReturnIdLinkToSelf() {
        ConceptId id;
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.READ)){
            id = tx.admin().getMetaConcept().instances().findAny().get().getId();
        }

        String attributesLink = "/kb/" + keyspace.getValue() + "/concept/" + id + "/relationships";
        RestAssured.when().get(attributesLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(attributesLink));
    }

    @Test
    public void whenCallingKeysEndpoint_ReturnIdLinkToSelf() {
        ConceptId id;
        try(GraknTx tx = factory.tx(keyspace, GraknTxType.READ)){
            id = tx.admin().getMetaConcept().instances().findAny().get().getId();
        }

        String keysLink = "/kb/" + keyspace.getValue() + "/concept/" + id + "/relationships";
        RestAssured.when().get(keysLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(keysLink));
    }

    @Test
    public void whenCallingPlaysEndpoint_ReturnIdLinkToSelf() {
        String playsLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.ENTITY.getLabel().getValue() + "/plays";

        RestAssured.when().get(playsLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(playsLink));
    }

    @Test
    public void whenCallingSubsEndpoint_ReturnIdLinkToSelf() {
        String subsLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.ENTITY.getLabel().getValue() + "/subs";

        RestAssured.when().get(subsLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(subsLink));
    }

    @Test
    public void whenCallingAttributeTypesEndpoint_ReturnIdLinkToSelf() {
        String attributesLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.ENTITY.getLabel().getValue() + "/attributes";

        RestAssured.when().get(attributesLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(attributesLink));
    }

    @Test
    public void whenCallingKeyTypesEndpoint_ReturnIdLinkToSelf() {
        String keysLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.ENTITY.getLabel().getValue() + "/keys";

        RestAssured.when().get(keysLink)
                .then().statusCode(SC_OK).contentType(ContentType.JSON).body("@id", is(keysLink));
    }

    @Test
    public void whenCallingInstancesEndpoint_ReturnNextLink() {
        String instancesLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.THING.getLabel().getValue() + "/instances";

        String nextLink = RestAssured.given().param("limit", 2).get(instancesLink).jsonPath().getString("next");

        assertThat(nextLink, startsWith(instancesLink));
        assertThat(nextLink, anyOf(endsWith("?limit=2&offset=2"), endsWith("?offset=2&limit=2")));
    }

    @Test
    public void whenCallingInstancesEndpoint_ReturnPreviousLink() {
        String instancesLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.THING.getLabel().getValue() + "/instances";

        String prevLink = RestAssured.given().param("limit", 100).param("offset", 150)
                .get(instancesLink).jsonPath().getString("previous");

        assertThat(prevLink, startsWith(instancesLink));
        assertThat(prevLink, anyOf(endsWith("?limit=100&offset=50"), endsWith("?offset=50&limit=100")));
    }

    @Test
    public void whenCallingInstancesEndpointOnFirstPage_DontReturnPreviousLink() {
        String instancesLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.THING.getLabel().getValue() + "/instances";

        RestAssured.when().get(instancesLink).then().body("$", not(hasKey("previous")));
    }

    @Test
    public void whenCallingInstancesEndpointOnLastPage_DontReturnNextLink() {
        String instancesLink =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.THING.getLabel().getValue() + "/instances";

        RestAssured.when().get(instancesLink).then().body("$", not(hasKey("next")));
    }

    @Test
    public void whenCallingInstancesEndpoint_ReturnIdLinkToSelf() {
        String link =
                "/kb/" + keyspace.getValue() + "/type/" + MetaSchema.THING.getLabel().getValue() + "/instances";

        RestAssured
                .given().param("limit", 100).param("offset", 150).get(link)
                .then().body("@id", anyOf(is(link + "?limit=100&offset=150"), is(link + "?offset=150&limit=100")));
    }

    //We can't use the class of the wrapper because it will be an AutoValue class
    private static void assertExists(Concept wrapper, Class clazz) throws IOException {
        String request = wrapper.selfLink().id();
        Response response = RestAssured.when().get(request);
        assertEquals(SC_OK, response.statusCode());
        assertEquals(wrapper, response.as(clazz));
    }
}
