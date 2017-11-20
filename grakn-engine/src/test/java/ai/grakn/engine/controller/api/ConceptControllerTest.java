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
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.controller.response.Attribute;
import ai.grakn.engine.controller.response.AttributeType;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.engine.controller.response.Entity;
import ai.grakn.engine.controller.response.EntityType;
import ai.grakn.engine.controller.response.Relationship;
import ai.grakn.engine.controller.response.RelationshipType;
import ai.grakn.engine.controller.response.Role;
import ai.grakn.engine.controller.response.Rule;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.graql.Pattern;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.REST;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.Assert.assertEquals;
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
    private static Entity entityWrapper;
    private static AttributeType attributeTypeWrapper;
    private static Attribute attributeWrapper1;
    private static Attribute attributeWrapper2;
    private static Rule ruleWrapper;

    public static SessionContext sessionContext = SessionContext.create();

    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        factory = EngineGraknTxFactory.createAndLoadSystemSchema(mockLockProvider, GraknConfig.create());
        new ConceptController(factory, spark, new MetricRegistry());
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

            ai.grakn.concept.EntityType entityType = tx.putEntityType("My Special Entity Type").plays(role1).plays(role2).attribute(attributeType);
            ai.grakn.concept.Entity entity = entityType.addEntity().attribute(attribute1).attribute(attribute2);

            ai.grakn.concept.RelationshipType relationshipType = tx.putRelationshipType("My Relationship Type").relates(role1).relates(role2);
            ai.grakn.concept.Relationship relationship = relationshipType.addRelationship().addRolePlayer(role1, entity).addRolePlayer(role2, entity);

            Pattern when = tx.graql().parser().parsePattern("$x isa \"My Special Entity Type\"");
            Pattern then = tx.graql().parser().parsePattern("$x isa \"My Special Entity Type\"");
            ai.grakn.concept.Rule rule = tx.putRule("My Special Snowflake of a Rule", when, then);

            //Manually Serialise The Concepts
            roleWrapper1 = ConceptBuilder.build(role1);
            roleWrapper2 = ConceptBuilder.build(role2);

            relationshipTypeWrapper = ConceptBuilder.build(relationshipType);
            relationshipWrapper = ConceptBuilder.build(relationship);

            entityTypeWrapper = ConceptBuilder.build(entityType);
            entityWrapper = ConceptBuilder.build(entity);

            attributeTypeWrapper = ConceptBuilder.build(attributeType);
            attributeWrapper1 = ConceptBuilder.build(attribute1);
            attributeWrapper2 = ConceptBuilder.build(attribute2);

            ruleWrapper = ConceptBuilder.build(rule);

            tx.commit();
        }
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

    //We can't use the class of the wrapper because it will be an AutoValue class
    private static void assertExists(Concept wrapper, Class clazz) throws IOException {
        String request = wrapper.selfLink().id();
        Response response = RestAssured.when().get(request);
        assertEquals(SC_OK, response.statusCode());
        assertEquals(wrapper, response.as(clazz));
    }
}
