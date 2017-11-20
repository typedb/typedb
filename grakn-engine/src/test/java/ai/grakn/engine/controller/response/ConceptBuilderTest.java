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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;

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
    public void whenWrappingEntity_EnsureEntityDetailsAreWrapped(){
        ai.grakn.concept.Entity entity = tx.getEntityType("person").instances().findAny().get();
        Entity entityWrapper = ConceptBuilder.build(entity);

        //Check internal data
        assertEquals(entity.getId(), entityWrapper.id());

        //Check Links to other concepts
        Set<Link> attributeIds = entity.attributes().map(Link::create).collect(Collectors.toSet());
        Set<Link> attributeWrapperIds = entityWrapper.attributes();
        assertEquals(attributeIds, attributeWrapperIds);

        Set<Link> keyIds = entity.keys().map(Link::create).collect(Collectors.toSet());
        Set<Link> keyWrapperIds = entityWrapper.keys();
        assertEquals(keyIds, keyWrapperIds);

        Set<RolePlayer> relationshipIds = new HashSet<>();
        entity.plays().forEach(role -> {
            Link roleWrapper = Link.create(role);
            entity.relationships(role).forEach(relationship -> {
                Link relationshipWrapper = Link.create(relationship);
                relationshipIds.add(RolePlayer.create(roleWrapper, relationshipWrapper));
            });
        });

        Set<RolePlayer> relationshipWrapperIds = entityWrapper.relationships();
        assertEquals(relationshipIds, relationshipWrapperIds);
    }

    @Test
    public void whenWrappingEntityType_EnsureEntityTypeDetailsAreWrapped(){
        ai.grakn.concept.EntityType entityType = tx.getEntityType("person");

        EntityType entityTypeWrapper = ConceptBuilder.build(entityType);

        //Check internal data
        assertEquals(entityType.getId(), entityTypeWrapper.id());
        assertEquals(entityType.getLabel(), entityTypeWrapper.label());
        assertEquals(entityType.isAbstract(), entityTypeWrapper.isAbstract());
        assertEquals(entityType.isImplicit(), entityTypeWrapper.implicit());

        //Check Links to other concepts
        Set<Link> supIds = entityType.subs().map(Link::create).collect(Collectors.toSet());
        Set<Link> supWrapperIds = entityTypeWrapper.subs();
        assertEquals(supIds, supWrapperIds);

        Set<Link> playIds = entityType.plays().map(Link::create).collect(Collectors.toSet());
        Set<Link> playWrapperIds = entityTypeWrapper.plays();
        assertEquals(playIds, playWrapperIds);
    }
}

