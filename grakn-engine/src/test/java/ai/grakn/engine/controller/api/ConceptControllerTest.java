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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.concurrent.locks.Lock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConceptControllerTest {
    private static final Lock mockLock = mock(Lock.class);
    private static final LockProvider mockLockProvider = mock(LockProvider.class);
    private static final Keyspace keyspance = SampleKBLoader.randomKeyspace();

    private static EngineGraknTxFactory factory;
    private static Role role1;
    private static Role role2;
    private static Rule rule;
    private static RelationshipType relationshipType;
    private static Relationship relationship;
    private static EntityType entityType;
    private static Entity entity;
    private static AttributeType<String> attributeType;
    private static Attribute<String> attribute;

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
            role1 = tx.putRole("My Special Role 1");
            role2 = tx.putRole("My Special Role 2");

            attributeType = tx.putAttributeType("My Attribute Type", AttributeType.DataType.STRING);
            attribute = attributeType.putAttribute("An attribute");

            entityType = tx.putEntityType("My Special Entity Type").plays(role1).plays(role2).attribute(attributeType);
            entity = entityType.addEntity().attribute(attribute);

            relationshipType = tx.putRelationshipType("My Relationship Type").relates(role1).relates(role2);
            relationship = relationshipType.addRelationship().addRolePlayer(role1, entity).addRolePlayer(role2, entity);

            tx.commit();
        }
    }


    @Test
    public void whenGettingConceptByIdAndConceptExists_ConceptIsReturned(){

    }

    @Test
    public void whenGettingConceptByIdAndConceptDoeNotExist_EmptyResponse(){

    }
}
