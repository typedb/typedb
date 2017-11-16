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
        assertEquals(entity.getId(), entityWrapper.get().conceptId());
        assertEquals(entity.keyspace(), entityWrapper.get().keyspace());
        assertEquals(Schema.BaseType.ENTITY, entityWrapper.get().baseType());
        assertEquals(entity.getId().getValue(), entityWrapper.get().uniqueId());
        assertEquals(entity.attributes().collect(Collectors.toSet()), entityWrapper.get().attributesLinked());
        assertEquals(entity.keys().collect(Collectors.toSet()), entityWrapper.get().keysLinked());
        assertEquals(entity.relationships().collect(Collectors.toSet()), entityWrapper.get().relationshipsLinked());
    }

    @Test
    public void whenWrappingEntityType_EnsureEntityTypeDetailsAreWrapped(){

    }

    @Test
    public void whenWrappingRelationship_EnsureRelationshipDetailsAreWrapped(){

    }

    @Test
    public void whenWrappingRelationshipType_EnsureRelationshipTypeDetailsAreWrapped(){

    }

    @Test
    public void whenWrappingAttribute_EnsureAttributeDetailsAreWrapped(){

    }

    @Test
    public void whenWrappingAttributeType_EnsureAttributeTypeDetailsAreWrapped(){

    }

    @Test
    public void whenWrappingRole_EnsureRoleDetailsAreWrapped(){

    }

    @Test
    public void whenWrappingRuleType_EnsureRuleDetailsAreWrapped(){

    }
}

