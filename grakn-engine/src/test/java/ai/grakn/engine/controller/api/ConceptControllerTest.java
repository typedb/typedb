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
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.controller.response.Attribute;
import ai.grakn.engine.controller.response.AttributeType;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.engine.controller.response.Entity;
import ai.grakn.engine.controller.response.EntityType;
import ai.grakn.engine.controller.response.Relationship;
import ai.grakn.engine.controller.response.RelationshipType;
import ai.grakn.engine.controller.response.Role;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConceptControllerTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Lock mockLock = mock(Lock.class);
    private static final LockProvider mockLockProvider = mock(LockProvider.class);
    private static final Keyspace keyspance = SampleKBLoader.randomKeyspace();

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

    public static SessionContext sessionContext = SessionContext.create();

    public static SparkContext sparkContext = SparkContext.withControllers(spark -> {
        factory = EngineGraknTxFactory.createAndLoadSystemSchema(mockLockProvider, GraknEngineConfig.create());
        new ConceptController(factory, spark, new MetricRegistry());
    });

    @ClassRule
    public static final RuleChain chain = RuleChain.emptyRuleChain().around(sessionContext).around(sparkContext);

    @BeforeClass
    public static void setUp() {
        when(mockLockProvider.getLock(any())).thenReturn(mockLock);

        //Load Silly Sample Data
        try(GraknTx tx = factory.tx(keyspance, GraknTxType.WRITE)){
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

            //Manually Serialise The Concepts
            roleWrapper1 = ConceptBuilder.<Role>build(role1).get();
            roleWrapper2 = ConceptBuilder.<Role>build(role2).get();

            relationshipTypeWrapper = ConceptBuilder.<RelationshipType>build(relationshipType).get();
            relationshipWrapper = ConceptBuilder.<Relationship>build(relationship).get();

            entityTypeWrapper = ConceptBuilder.<EntityType>build(entityType).get();
            entityWrapper = ConceptBuilder.<Entity>build(entity).get();

            attributeTypeWrapper = ConceptBuilder.<AttributeType>build(attributeType).get();
            attributeWrapper1 = ConceptBuilder.<Attribute>build(attribute1).get();
            attributeWrapper2 = ConceptBuilder.<Attribute>build(attribute2).get();

            tx.commit();
        }
    }

    @Test
    public void whenGettingConceptByIdAndConceptExists_ConceptIsReturned() throws IOException {
        String request = entityWrapper.selfLink().id();
        String content = RestAssured.when().get(request).thenReturn().body().asString();
        assertEquals(entityWrapper, objectMapper.readValue(content, ai.grakn.engine.controller.response.Entity.class));
    }

    @Test
    public void whenGettingConceptByIdAndConceptDoeNotExist_EmptyResponse(){

    }
}
