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

package ai.grakn.engine.controller.response;

import ai.grakn.GraknTx;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ConceptBuilderTest {
    public static GraknTx tx;

    @ClassRule
    public static SampleKBContext sampleKB = GenealogyKB.context();

    @BeforeClass
    public static void getTx(){
        tx = sampleKB.tx();
    }

    @AfterClass
    public static void closeTx(){
        tx.close();
    }

    @Test
    public void whenWrappingInvalidConcept_EmptyOptionalReturned(){
        assertFalse(ConceptBuilder.build(null).isPresent());
    }

    @Test
    public void whenWrappingEntity_EnsureEntityDetailsAreWrapped(){
        ai.grakn.concept.Entity entity = tx.getEntityType("person").instances().findAny().get();
        Optional<Entity> entityWrapper = ConceptBuilder.build(entity);

        assertTrue(entityWrapper.isPresent());

        //Check internal data
        assertEquals(entity.getId(), entityWrapper.get().conceptId());
        assertEquals(entity.keyspace(), entityWrapper.get().keyspace());
        assertEquals(Schema.BaseType.CONCEPT, entityWrapper.get().baseType());
        assertEquals(entity.getId().getValue(), entityWrapper.get().uniqueId());

        //Check Links to other concepts
        Set<String> attributeIds = entity.attributes().map(a -> a.getId().getValue()).collect(Collectors.toSet());
        Set<String> attributeWrapperIds = entityWrapper.get().attributesLinked().stream().map(a -> a.conceptId().getValue()).collect(Collectors.toSet());
        assertEquals(attributeIds, attributeWrapperIds);

        Set<String> keyIds = entity.keys().map(a -> a.getId().getValue()).collect(Collectors.toSet());
        Set<String> keyWrapperIds = entityWrapper.get().keysLinked().stream().map(a -> a.conceptId().getValue()).collect(Collectors.toSet());
        assertEquals(keyIds, keyWrapperIds);

        Set<String> relationshipIds = entity.relationships().map(a -> a.getId().getValue()).collect(Collectors.toSet());
        Set<String> relationshipWrapperIds = entityWrapper.get().relationshipsLinked().stream().map(a -> a.conceptId().getValue()).collect(Collectors.toSet());
        assertEquals(relationshipIds, relationshipWrapperIds);
    }

    @Test
    public void whenWrappingEntityType_EnsureEntityTypeDetailsAreWrapped(){
        ai.grakn.concept.EntityType entityType = tx.getEntityType("person");

        Optional<EntityType> entityTypeWrapper = ConceptBuilder.build(entityType);

        assertTrue(entityTypeWrapper.isPresent());

        //Check internal data
        assertEquals(entityType.getId(), entityTypeWrapper.get().conceptId());
        assertEquals(entityType.keyspace(), entityTypeWrapper.get().keyspace());
        assertEquals(Schema.BaseType.TYPE, entityTypeWrapper.get().baseType());
        assertEquals(entityType.getLabel().getValue(), entityTypeWrapper.get().uniqueId());
        assertEquals(entityType.isAbstract(), entityTypeWrapper.get().isAbstract());
        assertEquals(entityType.isImplicit(), entityTypeWrapper.get().implicit());
        assertEquals(entityType.getLabel(), entityTypeWrapper.get().label());
        assertEquals(entityType.sup().getId(), entityTypeWrapper.get().superConcept().conceptId());

        //Check Links to other concepts
        Set<String> supIds = entityType.subs().map(a -> a.getId().getValue()).collect(Collectors.toSet());
        Set<String> supWrapperIds = entityTypeWrapper.get().subConcepts().stream().map(a -> a.conceptId().getValue()).collect(Collectors.toSet());
        assertEquals(supIds, supWrapperIds);

        Set<String> playIds = entityType.plays().map(a -> a.getId().getValue()).collect(Collectors.toSet());
        Set<String> playWrapperIds = entityTypeWrapper.get().rolesPlayed().stream().map(a -> a.conceptId().getValue()).collect(Collectors.toSet());
        assertEquals(playIds, playWrapperIds);
    }
}

